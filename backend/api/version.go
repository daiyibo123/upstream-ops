package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bejix/upstream-ops/backend/config"
	"github.com/bejix/upstream-ops/backend/global"
	"github.com/gin-gonic/gin"
)

const (
	githubRepoURL              = "https://github.com/daiyibo123/upstream-ops"
	defaultGitHubLatestRelease = "https://api.github.com/repos/daiyibo123/upstream-ops/releases/latest"
	defaultGitHubTagsURL       = "https://api.github.com/repos/daiyibo123/upstream-ops/tags?per_page=1"
	defaultVersionFallbackURL  = "https://cdn.jsdelivr.net/gh/daiyibo123/upstream-ops@main/backend/global/version.go"
)

var (
	githubLatestReleaseURL = defaultGitHubLatestRelease
	githubTagsURL          = defaultGitHubTagsURL
	versionFallbackURL     = defaultVersionFallbackURL
	githubReleaseClient    = &http.Client{Timeout: 2 * time.Second}
	systemUpdateState      = &updateState{Status: "idle"}
)

type updateState struct {
	mu         sync.RWMutex
	Status     string
	Message    string
	Source     string
	StartedAt  time.Time
	FinishedAt time.Time
}

type systemUpdateStatus struct {
	Status     string    `json:"status"`
	Message    string    `json:"message"`
	Source     string    `json:"source,omitempty"`
	StartedAt  time.Time `json:"started_at,omitempty"`
	FinishedAt time.Time `json:"finished_at,omitempty"`
}

func (s *updateState) snapshot() systemUpdateStatus {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return systemUpdateStatus{Status: s.Status, Message: s.Message, Source: s.Source, StartedAt: s.StartedAt, FinishedAt: s.FinishedAt}
}

func (s *updateState) start(source string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Status == "updating" {
		return false
	}
	s.Status = "updating"
	s.Message = "正在下载并验证新版本，完成前不会替换当前容器"
	s.Source = source
	s.StartedAt = time.Now()
	s.FinishedAt = time.Time{}
	return true
}

func (s *updateState) finish(status, message string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Status = status
	s.Message = message
	s.FinishedAt = time.Now()
}

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

	system := api.Group("/system")
	system.POST("/restart", func(c *gin.Context) { handleSystemRestart(c) })
	system.POST("/update", func(c *gin.Context) { handleSystemUpdate(c) })
	system.GET("/update/status", func(c *gin.Context) { c.JSON(http.StatusOK, systemUpdateState.snapshot()) })
	system.GET("/upgrade-command", func(c *gin.Context) { handleUpgradeCommand(c) })
}

// handleSystemRestart 让当前进程主动退出，交给外部 restart 策略拉起。
//
// 更新镜像由 /system/update 触发内网 updater 侧车处理；这里保留一个轻量的
// "仅重启"动作，用于配置变更或手动 SSH 更新后让容器快速拉起。
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
		"command":       "docker compose pull && docker compose up -d app",
		"auto_update":   "WATCHTOWER_HTTP_API_PERIODIC_POLLS=true docker compose up -d watchtower",
		"rollback":      "IMAGE_TAG=<上一个可用版本> docker compose up -d app",
		"description":   "只更新应用镜像并重建 app 容器，不覆盖 ./data 里的 config.yaml 和数据库。面板里的『立即更新并重启』会触发内网 watchtower 侧车执行同等更新。",
		"restart_after": true,
		"repo_url":      githubRepoURL,
	})
}

func handleSystemUpdate(c *gin.Context) {
	runner, err := resolveSystemUpdateRunner()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"status": "unsupported",
			// apiFetch renders `error` for failed HTTP requests. Returning it
			// keeps the setup problem actionable instead of showing only HTTP 400.
			"error":   err.Error(),
			"message": err.Error(),
			"command": "docker compose pull && docker compose up -d app",
		})
		return
	}

	if !systemUpdateState.start(runner.Source) {
		c.JSON(http.StatusConflict, gin.H{"status": "updating", "message": "已有更新任务正在执行，请等待完成"})
		return
	}
	c.JSON(http.StatusAccepted, gin.H{
		"status":  "updating",
		"message": "已开始拉取更新并重建应用容器，完成后服务会短暂重启。数据目录 ./data 不会被覆盖。",
		"source":  runner.Source,
	})
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
		defer cancel()
		if err := runSystemUpdate(ctx, runner); err != nil {
			systemUpdateState.finish("failed", "更新失败，当前版本未替换："+err.Error())
			_, _ = fmt.Fprintf(os.Stderr, "system update failed: %v\n", err)
			return
		}
		systemUpdateState.finish("restarting", "新版本已下载并交由更新器重建应用，服务将短暂重启")
	}()
}

type systemUpdateRunner struct {
	Source string
	Target string
}

func resolveSystemUpdateRunner() (systemUpdateRunner, error) {
	if cmd := strings.TrimSpace(os.Getenv("SYSTEM_UPDATE_COMMAND")); cmd != "" {
		return systemUpdateRunner{Source: "command", Target: cmd}, nil
	}
	endpoint := strings.TrimSpace(os.Getenv("SYSTEM_UPDATE_URL"))
	if endpoint == "" {
		return systemUpdateRunner{}, errors.New("当前环境未配置 SYSTEM_UPDATE_URL；请使用 docker-compose.yml 中的 watchtower 侧车，或设置 SYSTEM_UPDATE_COMMAND")
	}
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return systemUpdateRunner{}, fmt.Errorf("SYSTEM_UPDATE_URL 无效: %w", err)
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return systemUpdateRunner{}, errors.New("SYSTEM_UPDATE_URL 必须是完整的 http(s) 地址")
	}
	return systemUpdateRunner{Source: "watchtower", Target: endpoint}, nil
}

func runSystemUpdate(ctx context.Context, runner systemUpdateRunner) error {
	switch runner.Source {
	case "command":
		return runShellCommand(ctx, runner.Target)
	default:
		return triggerUpdateEndpoint(ctx, runner.Target, os.Getenv("SYSTEM_UPDATE_TOKEN"))
	}
}

func triggerUpdateEndpoint(ctx context.Context, endpoint, token string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, nil)
	if err != nil {
		return err
	}
	if token = strings.TrimSpace(token); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	return fmt.Errorf("updater status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
}

func runShellCommand(ctx context.Context, command string) error {
	cmd := exec.CommandContext(ctx, "sh", "-lc", command)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func buildVersionResponse(ctx context.Context, d *Deps, force bool) versionResponse {
	app := config.AppConfig{Title: "AI Gateway"}
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
		latest, releaseURL, err = fetchLatestVersionFallback(ctx, versionCheckClient(proxyCfg, force))
		if err != nil {
			resp.LatestVersion = global.VERSION
			resp.ReleaseURL = githubRepoURL
			resp.UpdateAvailable = false
			resp.UpdateError = "无法连接版本源，请检查服务器网络或版本检查代理"
			return resp
		}
	}
	resp.LatestVersion = latest
	resp.ReleaseURL = releaseURL
	resp.UpdateAvailable = isVersionNewer(latest, global.VERSION)
	return resp
}

func fetchLatestVersionFallback(ctx context.Context, client *http.Client) (string, string, error) {
	if client == nil {
		client = githubReleaseClient
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, versionFallbackURL, nil)
	if err != nil {
		return "", "", err
	}
	req.Header.Set("User-Agent", "upstream-ops")
	resp, err := client.Do(req)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", "", fmt.Errorf("version fallback status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if err != nil {
		return "", "", err
	}
	version := ""
	for _, line := range strings.Split(string(body), "\n") {
		if !strings.Contains(line, "VERSION") || !strings.Contains(line, "=") {
			continue
		}
		value := strings.TrimSpace(strings.SplitN(line, "=", 2)[1])
		value = strings.Trim(value, "\"' `\t\r")
		if _, ok := parseVersion(value); ok {
			version = value
			break
		}
	}
	if version == "" {
		return "", "", errors.New("version fallback missing VERSION")
	}
	tag := version
	if !strings.HasPrefix(strings.ToLower(tag), "v") {
		tag = "v" + tag
	}
	return tag, githubRepoURL + "/releases/tag/" + tag, nil
}

func versionCheckClient(proxyCfg config.ProxyConfig, force bool) *http.Client {
	if !proxyCfg.VersionCheckEnabled {
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
