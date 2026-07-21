# OAuth 号池后端接口

所有 `/api/*` 管理接口继续使用项目原有管理员鉴权。外部 `/v1/*` 请求继续使用原有 API Key 鉴权、权限、额度、计费和日志流程。

## 固定对象

启动时幂等创建并修复：

- `chatgpt号池`：固定渠道类型 `chatgpt_pool`，固定分组引用 `pool-scope:gpt`，对外显示 `gpt号池`。
- `grok号池`：固定渠道类型 `grok_pool`，固定分组引用 `pool-scope:grok`，对外显示 `grok号池`。

固定渠道和固定分组不能通过普通渠道/分组接口创建仿冒、改绑或删除。稳定映射使用 `ChannelType + GroupRef`，不依赖显示名称。

## API Key 路由偏好

Gateway Key 创建、修改和详情增加：

```json
{ "route_preference": "ratio_first" }
```

可选值：

- `ratio_first`：默认，保持原倍率/公益优先顺序。
- `pool_first`：在相同公益、倍率、成本和模型能力层内优先固定号池。
- `upstream_first`：在相同公益、倍率、成本和模型能力层内优先普通上游。

三种模式都不会越过更低倍率候选；`route_preference` 只用于同一调度层的平局处理。

选项接口：`GET /api/gateway/route-preferences`。

## 账号管理

- `GET /api/oauth-pools`
- `GET /api/oauth-accounts/{chatgpt|grok}?page=1&page_size=10&status=alive&search=`
- `GET /api/oauth-accounts/{pool}/stats`
- `GET /api/oauth-accounts/{pool}/{id}`
- `POST /api/oauth-accounts/{pool}/import`：请求体为待识别 JSON，最大 10 MiB。
- `DELETE /api/oauth-accounts/{pool}/{id}`
- `POST /api/oauth-accounts/{pool}/batch-delete`：`{"ids":[1,2]}`
- `POST /api/oauth-accounts/{pool}/{id}/check`
- `POST /api/oauth-accounts/{pool}/{id}/quota`
- `POST /api/oauth-accounts/{pool}/inspect`
- `GET /api/oauth-accounts/{pool}/inspect`

分页大小只接受 `10/50/100/200`。列表和详情只返回脱敏标识，不返回 OAuth Token、Cookie 或密文。

导入返回部分成功结果，并为成功项启动受控测活；测活确认 `alive + in_rotation` 后账号才进入轮询。

## 调度和熔断

1. 先按 API Key 分组权限、公益/倍率、成本和模型规则选择渠道，再在同一调度层应用路由偏好。
2. 普通渠道直接请求上游；固定号池渠道再从对应 OAuth 池选择账号。
3. ChatGPT 与 Grok 使用独立快照和游标，所有 API Key 共享池内公平轮询。
4. 单个固定号池渠道每次最多快速尝试 3 个不同账号。
5. 临时失败连续 3 次进入冷却；401/凭据失效、429 可立即退出正常轮询。
6. 冷却结束后只允许一个 half-open 恢复探针；成功重置失败计数，失败重新冷却。
7. 流式响应只允许在首个有效输出写入客户端前切换账号或渠道。

渠道候选使用 30 秒兜底 TTL 的不可变内存快照，并在渠道/分组变更时主动失效；OAuth 账号快照 TTL 为 2 秒，导入、删除和健康状态变化会主动失效。

## 模型目录与严格白名单

- `GET /api/gateway/group-keys/{id}/models`：读取模型策略。
- `POST /api/gateway/group-keys/{id}/models/sync`：获取最新模型目录。
- `PUT /api/gateway/group-keys/{id}/models`：保存 `{"models":[...]}` 严格允许列表。

响应结构：

```json
{
  "available_models": ["gpt-5.6", "gpt-5.5"],
  "supported_models": ["gpt-5.6"],
  "restriction_enabled": true
}
```

第一次获取目录默认全选；后续获取保留仍存在的旧选择，新模型默认不自动启用。保存空选择表示该渠道拒绝全部模型。旧数据库中已有 `supported_models` 的记录升级后自动转为严格白名单；从未同步过模型的旧记录保持兼容的不限制状态，直到管理员首次获取或保存。

## 调度事件

- `GET /api/gateway/usage-logs?view=events`
- `GET /api/gateway/usage-logs/{id}`

详情包含请求时间、渠道、号池、脱敏账号、模型、尝试次数、状态、错误码、HTTP 状态和截断后的错误内容。历史记录没有 `error_detail` 时由服务端兼容生成。Authorization、Token、Cookie、密码和代理认证信息会统一脱敏。

## 代理配置

`proxy.selectedTargets` 使用稳定目标：

- `pool:chatgpt`
- `pool:grok`
- `fixed-channel:gpt`
- `fixed-channel:grok`
- `channel:{id}`

读取选项：`GET /api/settings/proxy/targets`。

- `enabled=false`：全部直连。
- `enabled=true` 且 `selectedTargets=[]`：全局代理。
- `enabled=true` 且有选择：只代理命中的普通渠道/固定渠道/号池。

配置热更新会关闭旧连接池。正常请求、流式请求、API Key 号池请求、测活、巡检和额度查询使用同一代理判定；代理配置或连接失败时不会静默回退直连。

## 数据库迁移

`AutoMigrate` 新增/扩展：

- `oauth_accounts`
- `gateway_keys.route_preference`
- `upstream_group_keys.available_models/model_restriction_enabled`
- `usage_logs.error_code/error_status/error_detail/oauth_pool/oauth_account/dispatch_attempt`

固定池数据由启动阶段的 `EnsureFixedOAuthPoolScopes` 幂等创建，不在通用 schema migration 中产生业务数据。
