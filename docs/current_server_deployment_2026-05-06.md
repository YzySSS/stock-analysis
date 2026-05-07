# 当前服务器部署记录（2026-05-06）

## 访问地址

- 公网首页：`http://www.yzysstock.cloud/`
- 健康检查：`http://www.yzysstock.cloud/api/health`

## 运行方式

FastAPI 后端由 systemd 托管：

- service：`stock-analysis-api.service`
- 工作目录：`/root/.openclaw/workspace/stock-analysis`
- Python 虚拟环境：`/root/.openclaw/workspace/stock-analysis/.venv`
- 入口：`app.api.main:app`
- 监听：`127.0.0.1:8000`

关键命令：

```bash
systemctl status stock-analysis-api.service
systemctl restart stock-analysis-api.service
journalctl -u stock-analysis-api.service -n 100 --no-pager
```

## 反向代理

Nginx 负责公网 HTTP 访问：

- 配置文件：`/etc/nginx/sites-available/stock-analysis`
- 启用链接：`/etc/nginx/sites-enabled/stock-analysis`
- 监听：`80`
- 域名：`www.yzysstock.cloud` / `yzysstock.cloud`
- upstream：`http://127.0.0.1:8000`

关键命令：

```bash
/usr/sbin/nginx -t
systemctl status nginx
systemctl reload nginx
```

## 已完成验证

2026-05-06 重装系统后重新部署并验证：

- `stock-analysis-api.service`：active + enabled
- `nginx`：active + enabled
- DNS：`www.yzysstock.cloud` / `yzysstock.cloud` 解析到当前服务器公网 IP `43.159.168.45`
- 本地 API：`http://127.0.0.1:8000/api/health` 返回 `{"status":"ok"}`
- 公网 API：`http://www.yzysstock.cloud/api/health` 返回 `200 OK`
- 公网页面：`http://www.yzysstock.cloud/` 返回股票分析控制台首页
- 关键前端接口已验证：
  - `/api/dashboard/summary?limit=5`
  - `/api/strategies`
  - `/api/strategies/detail`
  - `/api/system/status`
  - `/api/tracking/latest?limit=5&instrument_type=stock`
  - `/api/tracking/filters`
  - `/api/selection/results?strategy_id=lowvol_reversal`

## 注意事项

- 当前只恢复 HTTP，HTTPS 暂未配置。
- `nginx` 二进制在 `/usr/sbin/nginx`，当前 shell PATH 里可能没有 `/usr/sbin`。
- 本次仅做部署恢复，没有推送 GitHub；仓库仍需先清理 token/password 等敏感内容后再考虑 push。

## HTTPS 配置更新（2026-05-06 16:05）

已使用大X提供的证书完成 HTTPS 配置：

- 原始证书目录：`/root/.openclaw/workspace/yzysstock.cloud_nginx/`
- 证书：`yzysstock.cloud_bundle.pem` / `yzysstock.cloud_bundle.crt`
- 私钥：`yzysstock.cloud.key`
- Nginx 使用路径：
  - `/etc/nginx/ssl/yzysstock.cloud/fullchain.pem`
  - `/etc/nginx/ssl/yzysstock.cloud/privkey.key`
- 证书域名：`yzysstock.cloud`、`www.yzysstock.cloud`
- 有效期：`2026-05-06` 到 `2026-08-03`
- HTTP 80 已配置为自动 301 跳转到 HTTPS。

已验证：

- `https://www.yzysstock.cloud/api/health` 返回 `200 OK`，内容 `{"status":"ok"}`
- `https://www.yzysstock.cloud/` 返回首页 HTML
- `http://www.yzysstock.cloud/api/health` 返回 `301` 到 HTTPS
- TLS 证书链可读取，SAN 包含 `yzysstock.cloud` 和 `www.yzysstock.cloud`

注意：当前证书将在 `2026-08-03 23:59:59 GMT` 过期，后续需要提前续期并替换 nginx 证书文件。
