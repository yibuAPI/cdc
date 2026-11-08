"""同步管道的开启/停止/部署逻辑（CLI 与 Web 共用）。

控制策略（精简）：
- deploy: 建库+建表+建MV(保持ATTACHED) + 注册连接器 → 暂停 → 全程不执行
- start : 恢复连接器 + 触发增量快照（CK 消费常驻，数据来即消费）
- stop  : 暂停连接器（CK 消费不停，但 Kafka 不再有新数据进来）

去掉 DETACH/ATTACH MV 模式：CK Kafka 引擎消费者绑在 Kafka 表上，
DETACH MV 会导致消费链路断掉且 ATTACH 后不可靠恢复。改为连接器级控制。
"""
from __future__ import annotations

import logging
import time

from . import ck_client, ck_generator, connect_client, connector_generator, ui_state

log = logging.getLogger("cdc_sync.pipeline")


def _db(tables_cfg) -> str:
    return tables_cfg.tables[0].target_database if tables_cfg.tables else "default"


def _ensure_database(settings, db: str) -> None:
    """确保 CK 目标库存在（CK 26.x 连不存在的库直接抛 code 81）。"""
    try:
        client = ck_client._client(settings.clickhouse)
    except Exception:  # noqa: BLE001
        import copy
        conf = copy.deepcopy(settings.clickhouse)
        conf.database = "system"
        client = ck_client._client(conf)
    try:
        client.command(f"CREATE DATABASE IF NOT EXISTS {db}")
    finally:
        client.close()


def deploy_idle(settings, tables_cfg, *, ck_major=None) -> None:
    """完整部署（停止态）：建库 → 建表(含 MV，保持 ATTACHED) → 注册连接器 → 暂停。"""
    db = _db(tables_cfg)
    _ensure_database(settings, db)
    # 建 CK 三对象（Kafka 表 / ReplacingMergeTree 正式表 / MV，MV 创建后默认 ATTACHED）
    for t in tables_cfg.tables:
        tsql = ck_generator.build_table_sql(t, settings, ck_major=ck_major)
        ck_client.execute_statements(settings.clickhouse, tsql.ordered(), dry_run=False)
    # 注册连接器并立即暂停
    connector = connector_generator.build_connector(tables_cfg.tables, settings)
    connect_client.deploy(settings.connect_url, connector)
    time.sleep(2)
    try:
        connect_client.pause(settings.connect_url, settings.debezium.connector_name)
    except Exception as e:  # noqa: BLE001
        log.warning("暂停连接器失败（可忽略）：%s", e)
    ui_state.save_state({"pipeline": "stopped"})


def start(settings, tables_cfg, *, do_snapshot: bool = True) -> dict:
    """开启同步：恢复连接器 + 触发增量快照（CK 消费常驻，数据一来就消费）。"""
    name = settings.debezium.connector_name
    if not connect_client.connector_exists(settings.connect_url, name):
        connector = connector_generator.build_connector(tables_cfg.tables, settings)
        connect_client.deploy(settings.connect_url, connector)
    else:
        connect_client.resume(settings.connect_url, name)
    snap = None
    if do_snapshot:
        time.sleep(12)
        try:
            snap = send_snapshot(settings, tables_cfg)
        except Exception as e:  # noqa: BLE001
            log.warning("触发增量快照失败（可稍后手动 snapshot）：%s", e)
    ui_state.save_state({"pipeline": "running"})
    return {"connector": name, "snapshot_tables": snap}


def stop(settings, tables_cfg) -> dict:
    """停止同步：暂停连接器（CK 消费不停，但 Kafka 不再有新数据）。"""
    name = settings.debezium.connector_name
    paused = False
    try:
        if connect_client.connector_exists(settings.connect_url, name):
            connect_client.pause(settings.connect_url, name)
            paused = True
    except Exception as e:  # noqa: BLE001
        log.warning("暂停连接器失败：%s", e)
    ui_state.save_state({"pipeline": "stopped"})
    return {"connector_paused": paused}


def send_snapshot(settings, tables_cfg, only=None) -> int:
    """向 Kafka 信号 topic 发增量快照信号，回填历史（分块可续传）。返回表数。"""
    import json
    from kafka import KafkaProducer

    dcs = [f"{t.source_database}.{t.source_table}" for t in tables_cfg.tables]
    if only:
        want = set(only.split(","))
        dcs = [d for d in dcs if d in want or d.split(".", 1)[1] in want]
    if not dcs:
        raise ValueError("没有匹配的表")
    signal = {"type": "execute-snapshot", "data": {"type": "incremental", "data-collections": dcs}}
    brokers = [b.strip() for b in settings.kafka_internal_broker_list.split(",") if b.strip()]
    producer = KafkaProducer(
        bootstrap_servers=brokers, retries=3, request_timeout_ms=15000,
        **settings.kafka_sasl.python_kwargs(),
    )
    try:
        fut = producer.send("cdc-signals", key=b"mysql", value=json.dumps(signal).encode())
        fut.get(timeout=15)
        producer.flush()
    finally:
        producer.close()
    return len(dcs)
