"""生成 Debezium MySQL Source 连接器 JSON（Debezium 2.4 配置规范）。

- 注入 ExtractNewRecordState (unwrap) SMT，把 before/after 展平为扁平记录，
  并注入 __op / __source_ts_ms 元字段（软删由 __op='d' 判断，见 CK 物化视图）。
- delete.handling.mode=rewrite 保证 DELETE 事件不被丢弃、能传导到 CK。
- database.include.list / table.include.list 由 tables.yaml 汇总生成。
"""
from __future__ import annotations

import json
from pathlib import Path

from .config import Settings, TableDef


def build_connector(tables: list[TableDef], settings: Settings) -> dict:
    dbz = settings.debezium
    my = settings.mysql
    sasl = settings.kafka_sasl

    databases = sorted({t.source_database for t in tables})
    table_list = sorted({f"{t.source_database}.{t.source_table}" for t in tables})

    config = {
        "connector.class": "io.debezium.connector.mysql.MySqlConnector",
        # Debezium MySQL 连接器只支持单 task（tasks.max 必须为 1，否则报
        # "Only a single connector task may be started" 且 0 个 task 启动）
        "tasks.max": "1",
        "database.hostname": my.host,
        "database.port": str(my.port),
        "database.user": my.user,
        "database.password": my.password,
        "database.server.id": str(dbz.server_id),
        # Topic 前缀（Debezium 2.x 用 topic.prefix，取代旧 database.server.name）
        # 值为 mysql → topic 命名 mysql.<db>.<table>，与 CK Kafka 表对齐
        "topic.prefix": "mysql",
        # include.list 已限定同步范围，勿再设 exclude.list（二者互斥）
        "database.include.list": ",".join(databases),
        "table.include.list": ",".join(table_list),
        "snapshot.mode": dbz.snapshot_mode,
        "snapshot.locking.mode": "none",
        "include.schema.changes": "true",
        # ---- 增量快照（分块、可断点续传）----
        "signal.enabled.channels": "kafka",
        "signal.kafka.topic": "cdc-signals",
        "signal.kafka.bootstrap.servers": settings.kafka_internal_broker_list,
        "incremental.snapshot.chunk.size": "32768",
        # ---- 吞吐调优（提升快照/流式速度）----
        "snapshot.fetch.size": "50000",          # 快照每次从 MySQL 拉的行数
        "snapshot.max.threads": "4",             # 多表并行快照
        "max.batch.size": "16384",               # Debezium 单批事件数
        "max.queue.size": "65536",               # 内部队列
        "producer.override.batch.size": "524288",   # Kafka 生产者批大小 512KB
        "producer.override.linger.ms": "50",        # 攒批 50ms，提高吞吐
        "producer.override.acks": "1",              # 快照场景用 acks=1 提速（可重跑，容忍极端情况丢批）
        # ---- 大消息支持（logs/base64图片等大字段，单条易超默认 1MB）----
        # producer 请求上限 64MB + lz4 压缩，避免 "Unrecoverable exception from producer send callback"
        "producer.override.max.request.size": "67108864",
        "producer.override.compression.type": "lz4",
        "producer.override.buffer.memory": "134217728",
        # schema 历史 producer 同样放大
        "schema.history.internal.producer.max.request.size": "67108864",
        "schema.history.internal.producer.compression.type": "lz4",
        # ---- 错误容错：单条坏消息跳过并记日志，不再整个 task 崩掉 ----
        "errors.tolerance": "all",
        "errors.log.enable": "true",
        "errors.log.include.messages": "false",
        "errors.retry.timeout": "60000",
        "errors.retry.delay.max.ms": "10000",
        # ---- schema 历史（Debezium 2.x 键名 schema.history.internal.*）----
        "schema.history.internal.kafka.bootstrap.servers": settings.kafka_internal_broker_list,
        "schema.history.internal.kafka.topic": dbz.history_topic,
        # ---- Avro + Schema Registry ----
        "key.converter": "io.confluent.connect.avro.AvroConverter",
        "key.converter.schema.registry.url": settings.schema_registry_url,
        "value.converter": "io.confluent.connect.avro.AvroConverter",
        "value.converter.schema.registry.url": settings.schema_registry_url,
        # ---- unwrap SMT：展平 + 注入 __op/__source_ts_ms；rewrite 保留删除事件 ----
        "transforms": "unwrap",
        "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
        "transforms.unwrap.drop.tombstones": "false",
        "transforms.unwrap.delete.handling.mode": "rewrite",
        "transforms.unwrap.add.fields": "op,source.ts_ms",
        "transforms.unwrap.add.fields.prefix": "__",
    }

    if sasl.enabled:
        # ---- Kafka SASL：连接器自身产消息 + schema 历史 + 增量快照信号三处都要带凭据 ----
        for prefix in ("producer.override.", "schema.history.internal.producer.",
                       "schema.history.internal.consumer.", "signal.kafka."):
            for key, value in sasl.connector_settings().items():
                config[f"{prefix}{key}"] = value

    return {"name": dbz.connector_name, "config": config}


def write_connector_file(connector: dict, out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{connector['name']}.json"
    path.write_text(json.dumps(connector, ensure_ascii=False, indent=2), encoding="utf-8")
    return path
