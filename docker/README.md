# CDC 同步栈 · Docker 部署与跨服务器运维手册

本栈部署在 **ClickHouse 所在服务器（B 服）**。MySQL 在 **A 服**，ClickHouse 是 B 服**宿主机原生**安装，二者均作为**外部服务**由 `.env` 配置。

```
A 服 (MySQL) ──binlog──▶ [B 服 docker]  kafka + schema-registry + connect(Debezium) + cdc-monitor
                                              │
                                              ▼ (宿主机 localhost:9094)
                                       B 服宿主机原生 ClickHouse (Kafka 引擎消费)
```

组件与端口（B 服宿主机）：

| 组件 | 端口 | 绑定 | 说明 |
|------|------|------|------|
| **Nginx（宿主机独立安装）** | `80` / `443` | **0.0.0.0** | **唯一对公网暴露的入口**，浏览器打开 `https://<B服IP>/` |
| cdc-monitor（监控面板） | `8080` | `127.0.0.1` | 仅供 Nginx 反代，外部不可直连 |
| Kafka（供宿主 CK） | `9094` | `127.0.0.1` | 原生 CK 用 `127.0.0.1:9094`，全链路 SASL |
| Schema Registry | `8081` | `127.0.0.1` | 仅供 CK / 容器内组件 |
| Kafka Connect REST | `8083` | `127.0.0.1` | **无鉴权**，绝不能暴露 |

> **只暴露一个页面**：公网只开放宿主机 Nginx 的 80/443，其余端口都只绑 `127.0.0.1`。
> 服务器如还有防火墙（ufw / 安全组），只放行 80、443 即可。

> **面板登录**：监控面板所有 `/api` 请求都要求先登录。用户名/密码读取 `config/settings.yaml`
> 的 `panel` 段（由 `.env` 的 `CDC_PANEL_USER`/`CDC_PANEL_PASSWORD` 播种），也可在面板「配置」页直接改。
> **密码必须配置**：留空时登录会直接返回 403（不再回退默认口令）。登录后会话（HttpOnly Cookie +
> `Secure`，只在 HTTPS 上发送）默认 12h 生效；改密码即全员会话失效。

> **Kafka SASL**：broker 使用 `SASL_PLAINTEXT + PLAIN`，凭据在 `.env`
> （`KAFKA_SASL_USER` / `KAFKA_SASL_PASSWORD`）。Connect worker、Debezium 连接器（含 schema 历史与
> 增量快照 signal topic）、ClickHouse Kafka 引擎表、监控面板四个客户端都会自动带上凭据。
> 切换认证后**必须重新生成并重发连接器 + 重建 CK Kafka 表**（见下方「SASL 变更」）。
>
> **默认单节点 Kafka**（约 3~4G 内存，一键即起）。需要高可用（3 节点 RF=3、容忍单节点故障）时改用
> `docker compose -f docker-compose.ha.yml up -d --build`，并按 `.env` 注释调整两个 broker 地址。
>
> **重要**：Kafka / Schema Registry / Kafka Connect 都是本栈**自己启动的容器**，不是外部服务。
> 监控面板也在这套 docker 里，因此配置里的 `kafka:9092`、`http://schema-registry:8081`、
> `http://connect:8083` 是 docker 内部名——部署后自动解析，之前在 docker 外测失败属正常。

---

## 一、A 服（MySQL）准备

1. 开启 binlog（`my.cnf`，见《01-设计方案.md》§4.1）：`binlog_format=ROW`、`binlog_row_image=FULL`、`gtid_mode=ON` 等，重启生效。
2. 建同步账号：
   ```sql
   CREATE USER 'debezium'@'%' IDENTIFIED BY 'StrongPass@123';
   GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium'@'%';
   FLUSH PRIVILEGES;
   ```
3. **放行网络**：允许 B 服 IP 访问 A 服 `3306`（安全组/防火墙/`bind-address`）。

## 二、B 服（ClickHouse + 本栈）准备

1. 确认原生 ClickHouse 可用，并**允许容器访问**：CK 监听 `0.0.0.0`（`listen_host`），HTTP `8123` 对本机开放。
2. 建目标库：`CREATE DATABASE IF NOT EXISTS yibuapi;`
3. 安装 Docker + Docker Compose 插件。
4. 安装 Nginx（宿主机独立安装，不在 compose 里）：`apt install -y nginx`；配置见
   **完整部署文档 [`部署文档.md`](../部署文档.md) 第五节**。

---

### TLS 证书（宿主机 Nginx 使用）

Nginx 需要两个文件，放在 **宿主机** 的 `/etc/nginx/certs/`：

```
/etc/nginx/certs/fullchain.pem      # 服务器证书（含中间证书链）
/etc/nginx/certs/privkey.pem        # 私钥
```

**自签（内网够用，浏览器会提示不受信任）**：

```bash
sudo mkdir -p /etc/nginx/certs && cd /etc/nginx/certs
sudo openssl req -x509 -nodes -days 3650 -newkey rsa:2048 \
  -keyout privkey.pem -out fullchain.pem \
  -subj "/CN=cdc.local" -addext "subjectAltName=IP:<B服IP>,DNS:cdc.local"
sudo chmod 600 privkey.pem
```

**正式证书**（有域名时）：把签发的 `fullchain.pem` / `privkey.pem` 拷进同目录即可，
文件变化后 `sudo systemctl reload nginx` 生效。

> 反代配置文件模板在 [`docker/nginx.conf`](nginx.conf)，拷到宿主机
> `/etc/nginx/conf.d/cdc-panel.conf`。完整步骤见 [`部署文档.md`](../部署文档.md)。
>
> **必须走 HTTPS**：面板经反代对外时，登录密码与会话 Cookie 都必须加密传输；
> 容器内已默认 `CDC_PANEL_COOKIE_SECURE=1`，即 Cookie 只在 HTTPS 上发送。

---

## 三、一键启动本栈（在 B 服执行）

```bash
cd docker
cp .env.example .env
vim .env                      # 必改：MYSQL_PASSWORD、CK_PASSWORD、KAFKA_SASL_*、CDC_PANEL_PASSWORD

# 若之前在 docker 外手工填过网页配置，删掉旧的 settings.yaml 让容器按 .env 重新播种一份正确的
rm -f ../config/settings.yaml
```

> 生成强口令：`openssl rand -base64 24`（`KAFKA_SASL_ADMIN_PASSWORD`、`KAFKA_SASL_PASSWORD`、
> `CDC_PANEL_PASSWORD` 各用一个，不要复用）。

**方式 A · 全自动一键（推荐）**：起栈 + 建 CK 表 + 发布连接器，一条命令搞定：

```bash
bash deploy.sh
```

**方式 B · 手动分步**：

```bash
docker compose up -d --build                                  # 起栈
docker compose ps                                             # 待 healthy
docker compose exec -T cdc-monitor python main.py bootstrap   # 建表+发布连接器
```

启动后浏览器打开 **`https://B服IP/`**（Nginx 走 443）。这次是**容器里的监控**，
之前失败的 Kafka / Schema Registry / Kafka Connect 三个「测试连接」现在都会变绿。

> 首次访问用自签证书会有浏览器告警，属预期（见「TLS 证书」一节）。
> 面板内部 8080 只绑 `127.0.0.1`，直接访问 `http://B服IP:8080` 是不通的（这是有意的）。

> `.env` 里请按实际环境填写：`MYSQL_HOST`（A 服务器 IP）、`CK_HOST`（默认 `host.docker.internal`，与本栈同机时有效）。
> 若 CK 测试不通，把 `.env` 的 `CK_HOST` 改成 B 服的实际 IP 再 `docker compose up -d`。
>
> `bootstrap` / `deploy.sh` 都是**幂等**的，可反复执行（建表用 IF NOT EXISTS，连接器用更新）。

## 四、接入表 → 建 CK 表 → 发布连接器

> 用了 `deploy.sh` / `bootstrap` 的话，这一步**已自动完成**，下面仅作手动/重跑参考。

表清单 `config/tables.yaml` 已由 `yibuapi.sql` 生成（35 表）。**直接在监控面板「配置」页**可查看/编辑，
或用「导入 SQL」重新上传 `.sql` 生成。

建 CK 三对象 + 发布连接器，**在监控面板点按钮即可**（配置页/操作区）：
- 「建 CK 表(三对象)」→ 在 ClickHouse 建好 35 张表的 Kafka 表/正式表/物化视图
- 「发布/更新连接器」→ 把 Debezium 连接器发到 Connect，开始抓 binlog

命令行等价（可选）：
```bash
docker compose exec cdc-monitor python main.py apply-ck
docker compose exec cdc-monitor python main.py deploy-connector
```

## 五、监控

浏览器打开 `https://B服IP/`：
- 组件健康卡：配置 / 连接器 / Kafka / ClickHouse
- 连接器任务状态、Kafka 各 topic 堆积(lag)、CK 消费状态、数据对账
- 操作按钮：发布/重启连接器、建 CK 表、运行对账
- **配置页**：所有连接配置可视化编辑 + 逐组件「测试连接」

命令行等价：
```bash
docker compose exec cdc-monitor python main.py status
docker compose exec cdc-monitor python main.py reconcile
```

---

## 六、跨服务器排障

| 现象 | 排查 |
|------|------|
| 三个测试仍 `getaddrinfo failed` | 监控没跑在 docker 里。必须用容器版监控（`https://B服IP/`），别在 docker 外单独跑 |
| 连接器 FAILED，连不上 MySQL | A 服 3306 是否放行 B 服 IP；账号权限；`.env` 的 `MYSQL_HOST` |
| CK 消费不到数据 | CK 的 Kafka 引擎表 broker 是否 `127.0.0.1:9094`（= `KAFKA_BROKER_FOR_CK`）；SASL 凭据是否与 broker 一致；`docker compose logs kafka` |
| 连接器报 `SaslAuthenticationException` | `.env` 的 `KAFKA_SASL_USER/PASSWORD` 与 broker 的 `user_xxx` 用户表不一致；改完需重建 Connect 容器并重发连接器 |
| CK 报 `Authentication failed` / 一直 `Broken pipe` | CK Kafka 表缺 SASL 设置：重跑 `generate` 后 `apply-ck`（旧表的 SETTINGS 改不了，需 DROP 重建） |
| 监控连不上 CK | `.env` 的 `CK_HOST`（默认 `host.docker.internal`，不通则改成 B 服实际 IP）；CK 是否监听 0.0.0.0 |
| Kafka lag 一直涨 | CK 消费是否异常（面板/`system.kafka_consumers`）；`docker compose logs connect` |
| 位点丢失需重做 | A 服 binlog 保留期 `expire_logs_days` 是否覆盖快照+追赶窗口 |
| 打不开面板 / 502 | `sudo nginx -t` 校验配置；`sudo journalctl -u nginx -n 50` 看反代日志；`docker compose ps` 看 cdc-monitor 是否 running；证书是否在 `/etc/nginx/certs/` |

## 七、常用运维

```bash
docker compose logs -f connect          # 连接器日志
docker compose logs -f kafka            # Kafka 日志
docker compose restart connect          # 重启 Connect
docker compose down                     # 停栈（保留数据卷）
docker compose down -v                  # 停栈并清 Kafka 数据（谨慎）

sudo nginx -t                           # 反代配置校验
sudo systemctl reload nginx             # 反代重载（改配置/换证书后）
sudo journalctl -u nginx -n 50          # 反代日志（502/证书问题看这里）
```

> 默认单节点 Kafka。需要高可用（3 节点 RF=3、容忍单 broker 故障）时：
> `docker compose -f docker-compose.ha.yml up -d --build`，并按 `.env` 注释把两个 broker 地址改成三节点形式。

---

## 八、纯内网离线部署

内网机器无法拉镜像 / pip 装依赖时，用 `offline.sh` 走「有网机器打包 → 内网导入」：

```bash
# 有网机器（架构需与内网一致，通常 linux/amd64）
cd docker
bash offline.sh save                 # 拉基础镜像 + 构建 cdc-connect/cdc-monitor + 导出 offline/cdc-images.tar.gz
# 跨架构（如在 M 系 Mac 上打包给 x86 服务器）：
#   PLATFORM=linux/amd64 bash offline.sh save

# 把整个 docker/ 目录（含 offline/、.env、certs/）拷进内网 B 服后
bash offline.sh load                 # docker load 导入
bash deploy.sh                       # 正常部署，不再联网
```

注意：
- `docker compose build connect cdc-monitor` 需要联网（`pip install` / `confluent-hub install`），所以构建必须发生在有网机器上。
- 后续更新代码：内网用 `bash update.sh`（非 git 仓库会自动跳过 `pull`）；若**新增了 Python 依赖**，
  需回到有网机器重新 `bash offline.sh save` 后再导入。
- 镜像包约 2~4G，已加入 `.gitignore`（`docker/offline/`），不要提交到仓库。

---

## 九、SASL 变更（已上线后要改 Kafka 认证）

Kafka 认证相关配置**不是热改的**：连接器和 CK 表里都固化了凭据，漏改任何一个都会出现
「连接器能连、CK 消费不了」的半吊子状态。改 `.env` 的 `KAFKA_SASL_*` 后按顺序执行：

```bash
# 1) 重建 Kafka + Connect（凭据注入 broker 用户表与 worker）
docker compose up -d --force-recreate kafka schema-registry connect

# 2) 让面板重新读取新凭据（settings.yaml 是首启播种的，需重新播种）
rm -f ../config/settings.yaml
docker compose up -d --force-recreate cdc-monitor

# 3) 重新生成连接器 JSON 与 CK SQL（凭据会写进产物里）
docker compose exec -T cdc-monitor python main.py generate

# 4) 重建 CK Kafka 表（SETTINGS 建表后不可改，必须 DROP 后重建）
#    正式表/MV 不用动；只重建 *_kafka 表即可：
#    DROP TABLE IF EXISTS <db>.<表>_kafka;  然后重跑 apply-ck
docker compose exec -T cdc-monitor python main.py apply-ck

# 5) 重发连接器（config 以 PUT 覆盖，凭据随之更新）
docker compose exec -T cdc-monitor python main.py deploy-connector
```

> **升级到 SCRAM**：PLAIN 的密码在 docker 网络与本机回环内传输（已不跨公网）。若要更强，
> 改用 `SCRAM-SHA-512` 时需在 **首次 `kafka-storage format`** 阶段用 `--add-scram` 预置凭据，
> 已有数据卷无法事后切换——须按 §reset 清空 Kafka 卷重新初始化，属于有停机窗口的操作。
> 若确实要跨机房传输 SASL，请用 `SASL_SSL`（同时配 `ssl.*` 证书），而不是单纯换 mechanism。
