# CDC同步MySQL至ClickHouse从库

MySQL → Debezium → Kafka → ClickHouse 实时 CDC 同步的**配置驱动自动化工具**。
把「每接一张表要手写 CK 三对象建表 SQL + Debezium 连接器 JSON」的重复劳动，收敛为
一份表清单 + 一条命令。

> 架构背景见 [`01-设计方案.md`](01-设计方案.md)、落地步骤见 [`02-执行方案.md`](02-执行方案.md)。

## 能力

| 命令 | 作用 |
|------|------|
| `import-sql` | 解析 mysqldump/Navicat 导出的 `.sql` → 生成 `config/tables.yaml`（离线，无需连库） |
| `introspect` | 连 MySQL 读 `information_schema`，内省表结构 → 缓存 `config/schema_cache.json` |
| `generate` | 生成 CK 三对象 SQL（`out/clickhouse/`）+ Debezium 连接器 JSON（`out/connector/`） |
| `apply-ck` | 对 ClickHouse 执行三对象 DDL（顺序：正式表 → Kafka 表 → 物化视图） |
| `deploy-connector` | 向 Kafka Connect REST 发布/更新连接器（幂等：无则 POST，有则 PUT） |
| `status` | 连接器状态 + CK `system.kafka_consumers` |
| `reconcile` | MySQL `COUNT(*)` vs CK `count() FINAL WHERE is_deleted=0` 行数对账 |
| `serve` | 启动监控 Web 面板（FastAPI，默认端口 8000），面板需登录（单用户） |

## Docker 部署与监控面板

生产部署（Kafka + Schema Registry + Connect/Debezium + 监控面板 + Nginx 反代）见 **[`docker/README.md`](docker/README.md)** —
一套 compose 跑在 ClickHouse 服务器上，MySQL/ClickHouse 作为外部服务由 `docker/.env` 配置，
浏览器打开 `https://<服务器IP>/` 实时监控并可一键发布连接器 / 建 CK 表 / 对账。

**安全默认**（纯内网 + 只暴露一个页面）：
- Kafka 全链路 `SASL_PLAINTEXT + PLAIN`：Connect worker、Debezium 连接器（含 schema 历史与
  增量快照 signal topic）、ClickHouse Kafka 引擎表、监控面板四个客户端都会自动带凭据。
- 只有 Nginx 的 80/443 对公网开放；Kafka(9094)、Schema Registry(8081)、Connect REST(8083)、
  面板(8080) 全部只绑 `127.0.0.1`。Connect REST 无鉴权，暴露即等于交出连接器控制权。
- 面板强制 HTTPS 会话 Cookie，密码必须显式配置，未配置时登录直接返回 403（不再回退默认口令）。

本地起监控面板：
```bash
python main.py serve --port 8000          # 打开 http://localhost:8000，登录后使用
```

面板登录（单用户 `root`）：
- 用户名/密码读取 `config/settings.yaml` 的 `panel` 段（`user` / `password`，支持 `${ENV_VAR}` 引用）。
- **密码为空时拒绝登录**（403）。仅本地调试确实要用默认口令时，显式设 `allow_default_password: true`。
- 改密码即全员会话失效，重启服务不失效。
- 容器部署见 [`docker/.env.example`](docker/.env.example) 的 `CDC_PANEL_USER` / `CDC_PANEL_PASSWORD`。
- 若前面没有反代、直接 http 访问，需设 `CDC_PANEL_COOKIE_SECURE=0`（默认 `1`，只在 HTTPS 上发 Cookie）。

## 快速开始

```bash
# 1. 安装依赖（首次）
.venv/Scripts/python.exe -m pip install -r requirements.txt

# 2. 准备配置
cp config/settings.example.yaml config/settings.yaml
#   填入 MySQL/CK/Kafka/Connect 地址；密码可用 ${ENV_VAR} 引用环境变量
#   在 config/tables.yaml 里登记要同步的表

# 3. 内省表结构（表在 tables.yaml 未内联 columns 时）
python main.py introspect

# 4. 生成产物并人工核对
python main.py generate
#   → out/clickhouse/*.sql  与  out/connector/*.json

# 5. 落地（先预览，再执行）
python main.py apply-ck --dry-run
python main.py apply-ck
python main.py deploy-connector

# 6. 验证
python main.py status
python main.py reconcile
```

## 配置说明

### `config/settings.yaml`（集群连接，git 忽略）
由 `settings.example.yaml` 复制而来。密码支持 `${MYSQL_PASSWORD}` 形式引用环境变量，避免明文入库。

### `config/tables.yaml`（接入表清单）
每张表：

```yaml
tables:
  - source_database: db_order     # MySQL 库
    source_table: t_order         # MySQL 表
    target_table: ods_order       # CK 正式表（Kafka 表=ods_order_kafka，MV=mv_ods_order）
    order_by: id                  # 可选：去重键；无主键表必须显式指定
    partition_by: create_time     # 可选：PARTITION BY toYYYYMM(该时间列)
    comment: 订单ODS实时同步表
    # columns 可选：内联列定义则无需连 MySQL（离线生成）
    columns:
      - { name: id, mysql_type: "bigint unsigned", nullable: false, is_pk: true }
      - ...
```

列结构来源优先级：**内联 `columns` → `schema_cache.json`（introspect 产出）→ 现场内省（`--introspect`）**。

## 设计要点

- **类型映射**（`cdc_sync/type_map.py`）：处理 unsigned / decimal(p,s) / datetime(n)→DateTime64 / Nullable；ORDER BY 列强制非空。
- **三对象**（对齐设计方案 §7.2）：Kafka 引擎表 + `ReplacingMergeTree(version)` 正式表 + 物化视图；`version=__source_ts_ms`，`is_deleted=if(__deleted,1,0)`。
- **连接器**（对齐设计方案 §5）：注入 `ExtractNewRecordState`（unwrap）SMT，展平 before/after 并产出 `__op/__deleted/__source_ts_ms`，与 CK Kafka 表列对齐。
- **无主键表**：`order_by` 显式指定唯一键；缺失则退化为全列去重并告警（设计方案 §7.6）。

## 目录结构

```
cdc_sync/            工具包
  cli.py             CLI 入口与子命令
  config.py          配置加载（settings/tables）
  type_map.py        MySQL→ClickHouse 类型映射
  mysql_introspect.py information_schema 内省 + 缓存
  ck_generator.py    三对象 SQL 生成
  connector_generator.py Debezium 连接器 JSON 生成
  ck_client.py       ClickHouse 执行/查询
  connect_client.py  Kafka Connect REST
  reconcile.py       行数对账
templates/           Jinja2 SQL 模板（三对象）
config/              settings.example.yaml / tables.yaml
out/                 生成产物（git 忽略）
main.py              入口（python main.py <command>）
```

## 注意

- `introspect / apply-ck / deploy-connector / status / reconcile` 需连真实 MySQL/CK/Connect；无法连接时给出清晰报错，不崩溃。
- `generate` 在 `tables.yaml` 内联 `columns` 时可**完全离线**运行，便于评审产物。
- 生成的 SQL 均为 `CREATE ... IF NOT EXISTS`，可重复执行；DDL 变更需按设计方案 §7.4 三对象一致调整（改 tables.yaml 后重跑）。
