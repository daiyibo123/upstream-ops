# UpstreamOps · 聚合调度网关

> 一个自用的 AI API 聚合调度网关：你创建自己的密钥（`sk-...`），请求打到本站，本站在多个上游中转站之间**按最便宜且存活的渠道自动调度转发**，一个渠道坏了自动切换，尽量降低丢包、提升稳定性。支持 OpenAI 与 Claude 两种请求格式，支持 Codex（Responses API）直连。

## 功能概览

- **聚合调度**：创建密钥请求本站 → 本站在活着的上游里挑最便宜的转发。公益/免费渠道优先，其次按倍率从低到高。
- **自动故障切换**：请求失败自动切换下一个候选；失败渠道冷却 5 分钟后自动恢复，也可手动解除。
- **健康检查（测活）**：并发测活、独立超时、极小 token 探针（发 `hi` + `max_tokens:1`）；OpenAI 与 Claude 渠道分别用各自格式探测，避免误判。活的标绿、死的标红。
- **格式兼容**：客户端可用 `/v1/responses`、`/v1/chat/completions`、`/v1/messages`；上游只支持 chat 时自动降级并把响应转回客户端期望的格式。Codex 直连（不经路由）也能用。
- **密钥管理**：`sk-` 前缀、命名、启用/停用、过期时间、每日/累计额度（按 M 计），可指定只走某些渠道。
- **公益 Key**：可在公开首页展示一个可复制的公益 Key，支持复制密码、提示词、到期时间。
- **使用记录**：记录每次请求的渠道、分组、模型、token 与时间。
- **手动添加渠道**：对无法登录的上游，可直接填分组名 + key 手动接入。
- **一键部署 + 自动更新**：Docker 一条命令拉起；watchtower 侧车可选自动更新，支持一键重启、回退。

## 快速部署（Docker）

服务器只需 `docker-compose.yml` + `.env` 两个文件，直接拉取打包好的镜像，无需在服务器上构建：

```bash
mkdir -p /root/upstream-ops && cd /root/upstream-ops
curl -fsSL -o docker-compose.yml https://raw.githubusercontent.com/daiyibo123/upstream-ops/main/docker-compose.yml

cat > .env <<EOF
APP_SECRET=$(openssl rand -hex 16)
AUTH_TOKEN_SECRET=$(openssl rand -hex 16)
ADMIN_USERNAME=admin
ADMIN_PASSWORD=改成你的密码
HTTP_PORT=127.0.0.1:8080
EOF

# 需要自动更新则加 --profile autoupdate
docker compose --profile autoupdate pull && docker compose --profile autoupdate up -d
```

- 账号密码在初始化时由 `.env` 设置，后续在 `.env` 里修改。
- 数据（渠道、密钥、数据库）持久化在 `./data` 目录，更新镜像不会丢数据。
- 建议前置 Caddy 反代 + 自动 HTTPS，应用只监听 `127.0.0.1`。

## 更新

```bash
cd /root/upstream-ops && docker compose --profile autoupdate pull && docker compose --profile autoupdate up -d
```

或在面板里点「检查更新 / 立即重启」。更新只替换镜像，`./data` 保持不变。

## 致谢

本项目是在他人开源成果基础上的二次开发，特此致谢：

- 直接二开自 **[bejix/upstream-ops](https://github.com/bejix/upstream-ops)** —— 感谢 [@bejix](https://github.com/bejix) 的开源工作。
- 其上游最初二开自 **[worryzyy/upstream-hub](https://github.com/worryzyy/upstream-hub)** —— 感谢 [@worryzyy](https://github.com/worryzyy) 的原始开源工作。

同时感谢 [sub2api](https://github.com/Wei-Shaw/sub2api)、[new-api](https://github.com/QuantumNous/new-api) 等项目在请求转发、密钥管理、并发控制等方面的实现思路提供的参考。

## 说明

本项目为自用性质，仅提供单账号鉴权。请自行确保上游账号与本站密钥的安全，遵守各上游服务条款。
