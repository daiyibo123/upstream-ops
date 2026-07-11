package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/bejix/upstream-ops/backend/config"
	"github.com/bejix/upstream-ops/backend/global"
	"github.com/gin-gonic/gin"
)

const (
	githubRepoURL              = "https://github.com/daiyibo123/upstream-ops"
	defaultGitHubLatestRelease = "https://api.github.com/repos/daiyibo123/upstream-ops/releases/latest"
	defaultGitHubTagsURL       = "https://api.github.com/repos/daiyibo123/upstream-ops/tags?per_page=1"
)

var (
	githubLatestReleaseURL = defaultGitHubLatestRelease
	githubTagsURL          = defaultGitHubTagsURL
	githubReleaseClient    = &http.Client{Timeout: 2 * time.Second}
)

type versionResponse struct {
	Name            string `json:"name"`
	Title           string `json:"title"`
	Version         string `json:"version"`
	LatestVersion   string `json:"latest_version"`
	UpdateAvailable bool   `json:"update_available"`
	RepoURL         string `json:"repo_url"`
	ReleaseURL      string `json:"release_url"`
	UpdateError     string `json:"update_error"`
}

type githubReleaseResponse struct {
	TagName string `json:"tag_name"`
	HTMLURL string `json:"html_url"`
}

type githubTagResponse struct {
	Name string `json:"name"`
}

func registerVersion(api *gin.RouterGroup, d *Deps) {
	api.GET("/version", func(c *gin.Context) {
		force := c.Query("force") == "1" || strings.EqualFold(c.Query("force"), "true")
		c.JSON(http.StatusOK, buildVersionResponse(c.Request.Context(), d, force))
	})

	// /api/system/* 系列：Docker 场景下的自更新只能"重启由 restart 策略拉起"这一条路
	// （容器内部没法优雅 docker pull 自身镜像并热替换）。因此这里只提供两个动作：
	//   - restart：进程主动 os.Exit(0)，靠 compose 里的 restart: unless-stopped 立即拉起
	//   - upgrade-command：把服务器上应执行的 docker 命令回给前端，用户复制到 SSH 执行
	// upgrade-command 是纯"信息型"接口，本身不做危险动作；restart 才是真操作。
	system := api.Group("/system")
	system.POST("/restart", func(c *gin.Context) { handleSystemRestart(c) })
	system.GET("/upgrade-command", func(c *gin.Context) { handleUpgradeCommand(c) })
}

// handleSystemRestart 让当前进程主动退出，交给外部 restart 策略拉起。
//
// 为什么不做 docker pull：容器内没有 docker CLI、也不该给它挂 docker.sock（这
// 等于给一个 Web 服务任意 host 权限）。sub2api 上游同样绕开了这个选项。
// 正确的流程是：用户在服务器 SSH 里跑 `docker compose pull && docker compose up -d`，
// 我们只帮他做"新版镜像已经 pull 好之后，让运行中的容器立刻用新版起来"这一步。
func handleSystemRestart(c *gin.Context) {
	// 先返回 202，让前端立刻能收到成功；真正的 exit 放到 goroutine 里延迟 500ms 执行，
	// 保证 HTTP 响应能完整写回客户端（否则连接会被中途切断）。
	c.JSON(http.StatusAccepted, gin.H{
		"status":  "restarting",
		"message": "服务正在重启，若容器 restart 策略正常，5 秒内应可自动恢复",
	})
	go func() {
		time.Sleep(500 * time.Millisecond)
		os.Exit(0)
	}()
}

// handleUpgradeCommand 返回"服务器上要跑什么命令来拉新版镜像"的说明。
// 前端会展示出来 + 提供一键复制。
func handleUpgradeCommand(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"command":       "docker compose pull && docker compose up -d",
		"auto_update":   "docker compose --profile autoupdate up -d",
		"rollback":      "IMAGE_TAG=<上一个可用版本> docker compose up -d app",
		"description":   "只更新应用镜像，不覆盖 ./data 里的 config.yaml 和数据库。执行完后可点『立即重启』让容器切到新版本。若想自动更新，改用 auto_update 命令带起 watchtower 侧车（旧镜像仍保留，可用 rollback 一行回退）。",
		"restart_after": true,
		"repo_url":      githubRepoURL,
	})
}

func buildVersionResponse(ctx context.Context, d *Deps, force bool) versionResponse {
	app := config.AppConfig{Title: "UpstreamOps"}
	proxyCfg := config.ProxyConfig{}
	if d != nil && d.Runtime != nil {
		if cfg, err := config.LoadFile(d.Runtime.ConfigPath()); err == nil {
			app = cfg.App
		}
		proxyCfg = d.Runtime.CurrentProxy()
	}

	resp := versionResponse{
		Name:    "upstream-ops",
		Title:   app.Title,
		Version: global.VERSION,
		RepoURL: githubRepoURL,
	}

	latest, releaseURL, err := fetchLatestGitHubRelease(ctx, versionCheckClient(proxyCfg, force))
	if err != nil {
		resp.UpdateError = err.Error()
		return resp
	}
	resp.LatestVersion = latest
	resp.ReleaseURL = releaseURL
	resp.UpdateAvailable = isVersionNewer(latest, global.VERSION)
	return resp
}

func versionCheckClient(proxyCfg config.ProxyConfig, force bool) *http.Client {
	if !proxyCfg.VersionCheckEnabled && !force {
		return githubReleaseClient
	}
	proxyURL, err := proxyCfg.ActiveURL()
	if err != nil || proxyURL == "" {
		return githubReleaseClient
	}
	u, err := url.Parse(proxyURL)
	if err != nil {
		return githubReleaseClient
	}
	return &http.Client{
		Timeout: githubReleaseClient.Timeout,
		Transport: &http.Transport{
			Proxy: http.ProxyURL(u),
		},
	}
}

func fetchLatestGitHubRelease(ctx context.Context, client *http.Client) (string, string, error) {
	if client == nil {
		client = githubReleaseClient
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, githubLatestReleaseURL, nil)
	if err != nil {
		return "", "", err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("User-Agent", "upstream-ops")

	resp, err := client.Do(req)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if resp.StatusCode == http.StatusNotFound {
			return fetchLatestGitHubTag(ctx, client)
		}
		return "", "", fmt.Errorf("github latest release status %d", resp.StatusCode)
	}

	var release githubReleaseResponse
	if err := json.NewDecoder(resp.Body).Decode(&release); err != nil {
		return "", "", err
	}
	if strings.TrimSpace(release.TagName) == "" {
		return "", "", errors.New("github latest release missing tag_name")
	}
	if strings.TrimSpace(release.HTMLURL) == "" {
		release.HTMLURL = githubRepoURL
	}
	return release.TagName, release.HTMLURL, nil
}

func fetchLatestGitHubTag(ctx context.Context, client *http.Client) (string, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, githubTagsURL, nil)
	if err != nil {
		return "", "", err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("User-Agent", "upstream-ops")

	resp, err := client.Do(req)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", "", fmt.Errorf("github tags status %d", resp.StatusCode)
	}
	var tags []githubTagResponse
	if err := json.NewDecoder(resp.Body).Decode(&tags); err != nil {
		return "", "", err
	}
	if len(tags) == 0 || strings.TrimSpace(tags[0].Name) == "" {
		return "", "", errors.New("github tags missing latest tag")
	}
	tag := strings.TrimSpace(tags[0].Name)
	return tag, githubRepoURL + "/releases/tag/" + tag, nil
}

func isVersionNewer(latest, current string) bool {
	lv, ok := parseVersion(latest)
	if !ok {
		return false
	}
	cv, ok := parseVersion(current)
	if !ok {
		return false
	}
	for i := range lv {
		if lv[i] > cv[i] {
			return true
		}
		if lv[i] < cv[i] {
			return false
		}
	}
	return false
}

func parseVersion(v string) ([3]int, bool) {
	var out [3]int
	v = strings.TrimSpace(strings.TrimPrefix(v, "v"))
	if i := strings.IndexAny(v, "-+"); i >= 0 {
		v = v[:i]
	}
	parts := strings.Split(v, ".")
	if len(parts) != 3 {
		return out, false
	}
	for i, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil || n < 0 {
			return out, false
		}
		out[i] = n
	}
	return out, true
}
