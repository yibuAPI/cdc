"""连通性测试：对 MySQL / ClickHouse / Kafka / Schema Registry / Kafka Connect
逐一探测，返回 {ok, message, detail} 供配置页展示。

- 使用短超时，失败降级为 ok=False + 明确原因，不抛异常。
- MySQL 额外回报 binlog 就绪情况（对 CDC 至关重要）。
"""
from __future__ import annotations

import socket

import requests

from ..config import Settings

_HTTP_TIMEOUT = 6


def _r(ok: bool, message: str, **detail) -> dict:
    return {"ok": ok, "message": message, "detail": detail}


# ----------------------------- MySQL -----------------------------

def test_mysql(s: Settings) -> dict:
    try:
        import pymysql
    except ImportError:
        return _r(False, "缺少依赖 PyMySQL")
    conf = s.mysql
    try:
        conn = pymysql.connect(
            host=conf.host, port=conf.port, user=conf.user, password=conf.password,
            charset=conf.charset, connect_timeout=6, read_timeout=6,
            cursorclass=pymysql.cursors.DictCursor,
        )
    except Exception as e:  # noqa: BLE001
        return _r(False, f"连接失败 {conf.host}:{conf.port} — {e}")

    detail = {}
    warnings = []
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT VERSION() AS v")
            detail["version"] = cur.fetchone()["v"]
            # binlog 就绪检查（CDC 前置）
            for var, want in (("log_bin", "ON"), ("binlog_format", "ROW"), ("binlog_row_image", "FULL")):
                cur.execute("SHOW VARIABLES LIKE %s", (var,))
                row = cur.fetchone()
                val = row["Value"] if row else "?"
                detail[var] = val
                if val.upper() != want:
                    warnings.append(f"{var}={val}(应为{want})")
            # 权限自检
            try:
                cur.execute("SHOW GRANTS")
                grants = " ".join(r0 for row in cur.fetchall() for r0 in row.values())
                has_repl = "REPLICATION SLAVE" in grants.upper() and "REPLICATION CLIENT" in grants.upper()
                detail["replication_grant"] = "有" if has_repl else "缺"
                if not has_repl:
                    warnings.append("账号缺少 REPLICATION SLAVE/CLIENT 权限")
            except Exception:  # noqa: BLE001
                pass
    finally:
        conn.close()

    if warnings:
        return _r(True, "连接成功，但有告警：" + "；".join(warnings), **detail)
    return _r(True, f"连接成功，MySQL {detail.get('version','?')}，binlog 就绪", **detail)


# ----------------------------- ClickHouse -----------------------------

def test_clickhouse(s: Settings) -> dict:
    try:
        import clickhouse_connect
    except ImportError:
        return _r(False, "缺少依赖 clickhouse-connect")
    conf = s.clickhouse
    try:
        client = clickhouse_connect.get_client(
            host=conf.host, port=conf.port, username=conf.user, password=conf.password,
            database=conf.database, secure=conf.secure, connect_timeout=6, query_retries=1,
        )
    except Exception as e:  # noqa: BLE001
        return _r(False, f"连接失败 {conf.host}:{conf.port} — {e}")
    try:
        ver = client.query("SELECT version()").result_rows[0][0]
        # 校验目标库存在
        exists = client.query(
            "SELECT count() FROM system.databases WHERE name = {db:String}",
            parameters={"db": conf.database},
        ).result_rows[0][0]
        db_note = "存在" if exists else "不存在(需先建库)"
        return _r(True, f"连接成功，ClickHouse {ver}，库 {conf.database} {db_note}",
                  version=ver, database=conf.database, database_exists=bool(exists))
    except Exception as e:  # noqa: BLE001
        return _r(False, f"查询失败 — {e}")
    finally:
        try:
            client.close()
        except Exception:  # noqa: BLE001
            pass


# ----------------------------- Kafka -----------------------------

def test_kafka(s: Settings) -> dict:
    """测试容器内 broker（internal_broker_list）。逐 broker 探测 + 拉集群元数据。"""
    brokers = [b.strip() for b in (s.kafka_internal_broker_list or "").split(",") if b.strip()]
    if not brokers:
        return _r(False, "未配置 internal_broker_list")

    reach = []
    for b in brokers:
        host, _, port = b.rpartition(":")
        try:
            with socket.create_connection((host or "127.0.0.1", int(port or "9092")), timeout=4):
                reach.append((b, True))
        except Exception:  # noqa: BLE001
            reach.append((b, False))

    up = [b for b, ok in reach if ok]
    down = [b for b, ok in reach if not ok]
    if not up:
        return _r(False, f"全部 broker 不可达：{', '.join(brokers)}")

    # 拉元数据确认可用 broker / topic 数（Kafka 开 SASL 时握手也要带凭据）
    meta = {}
    try:
        from kafka import KafkaConsumer
        c = KafkaConsumer(
            bootstrap_servers=up, request_timeout_ms=6000, consumer_timeout_ms=6000,
            **s.kafka_sasl.python_kwargs(),
        )
        try:
            meta["brokers"] = len(list(c.cluster.brokers()))
            meta["topics"] = len(c.topics())
        finally:
            c.close(autocommit=False)
    except Exception as e:  # noqa: BLE001
        meta["meta_error"] = str(e)

    msg = f"可达 {len(up)}/{len(brokers)} broker"
    if "brokers" in meta:
        msg += f"，集群 {meta['brokers']} 节点 / {meta['topics']} topic"
    if down:
        msg += f"；不可达：{', '.join(down)}"
    ok = len(down) == 0 and "brokers" in meta
    return _r(ok, msg, reachable=up, unreachable=down, **meta)


# ----------------------------- Schema Registry -----------------------------

def test_schema_registry(s: Settings) -> dict:
    url = s.schema_registry_url.rstrip("/")
    try:
        r = requests.get(f"{url}/subjects", timeout=_HTTP_TIMEOUT)
    except requests.RequestException as e:
        return _r(False, f"连接失败 {url} — {e}")
    if r.status_code != 200:
        return _r(False, f"HTTP {r.status_code}: {r.text[:120]}")
    try:
        subjects = r.json()
        return _r(True, f"连接成功，已注册 {len(subjects)} 个 schema", subjects=len(subjects))
    except Exception:  # noqa: BLE001
        return _r(True, "连接成功", subjects=None)


# ----------------------------- Kafka Connect -----------------------------

def test_connect(s: Settings) -> dict:
    url = s.connect_url.rstrip("/")
    try:
        r = requests.get(f"{url}/connectors", timeout=_HTTP_TIMEOUT)
    except requests.RequestException as e:
        return _r(False, f"连接失败 {url} — {e}")
    if r.status_code != 200:
        return _r(False, f"HTTP {r.status_code}: {r.text[:120]}")
    try:
        connectors = r.json()
        # 顺带取版本
        ver = None
        try:
            root = requests.get(f"{url}/", timeout=_HTTP_TIMEOUT).json()
            ver = root.get("version")
        except Exception:  # noqa: BLE001
            pass
        return _r(True, f"连接成功，Connect {ver or ''}，现有 {len(connectors)} 个连接器",
                  connectors=len(connectors), version=ver)
    except Exception:  # noqa: BLE001
        return _r(True, "连接成功", connectors=None)


TESTS = {
    "mysql": test_mysql,
    "clickhouse": test_clickhouse,
    "kafka": test_kafka,
    "schema_registry": test_schema_registry,
    "connect": test_connect,
}


def run_test(component: str, s: Settings) -> dict:
    fn = TESTS.get(component)
    if not fn:
        return _r(False, f"未知组件：{component}")
    return fn(s)
