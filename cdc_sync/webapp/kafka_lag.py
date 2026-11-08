"""Kafka 消费堆积（lag）计算：topic 末端 offset - CK 消费组已提交 offset。

用容器内部 broker 地址（settings.kafka_internal_broker_list）。
所有异常均降级为返回 error 字段，不向上抛，供监控页展示。
"""
from __future__ import annotations

import socket

from ..config import Settings, TableDef


def _first_broker(broker_list: str) -> tuple[str, int]:
    first = (broker_list or "").split(",")[0].strip()
    host, _, port = first.rpartition(":")
    return (host or "127.0.0.1"), int(port or "9092")


def kafka_reachable(broker_list: str, timeout: float = 3.0) -> tuple[bool, str | None]:
    """快速 TCP 探测 broker 端口是否可达（避免 kafka-python 长时间重试 bootstrap）。"""
    host, port = _first_broker(broker_list)
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True, None
    except Exception as e:  # noqa: BLE001
        return False, f"broker 不可达 {host}:{port} — {e}"


def _kafka_modules():
    from kafka import KafkaConsumer, KafkaAdminClient, TopicPartition  # 延迟导入
    return KafkaConsumer, KafkaAdminClient, TopicPartition


def topic_lag(settings: Settings, tables: list[TableDef]) -> list[dict]:
    """返回每张表对应 topic 的消费堆积。

    结构：{table, topic, group, partitions, end_offset, committed, lag, error?}
    """
    broker = settings.kafka_internal_broker_list

    # 先快速探测，broker 不可达则立即返回，避免 kafka-python 长时间阻塞
    ok, err = kafka_reachable(broker)
    if not ok:
        return [{"error": err}]

    try:
        KafkaConsumer, KafkaAdminClient, TopicPartition = _kafka_modules()
    except ImportError:
        return [{"error": "缺少依赖 kafka-python"}]

    results: list[dict] = []
    # Kafka 开启 SASL 后，Consumer/AdminClient 都必须带凭据，否则握手即失败
    sasl_kwargs = settings.kafka_sasl.python_kwargs()

    consumer = None
    admin = None
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=broker,
            enable_auto_commit=False,
            request_timeout_ms=8000,
            consumer_timeout_ms=8000,
            **sasl_kwargs,
        )
        admin = KafkaAdminClient(
            bootstrap_servers=broker,
            request_timeout_ms=8000,
            **sasl_kwargs,
        )
    except Exception as e:  # noqa: BLE001
        return [{"error": f"连接 Kafka 失败 {broker} — {e}"}]

    try:
        for t in tables:
            row = {"table": t.target_table, "topic": t.topic, "group": t.consumer_group}
            try:
                parts = consumer.partitions_for_topic(t.topic)
                if not parts:
                    row.update({"partitions": 0, "end_offset": 0, "committed": 0, "lag": 0,
                                "note": "topic 不存在或无分区"})
                    results.append(row)
                    continue
                tps = [TopicPartition(t.topic, p) for p in parts]
                end = consumer.end_offsets(tps)
                # kafka-python 3.x：list_group_offsets({group: None}) → {group: {tp: OffsetAndMetadata}}
                try:
                    res = admin.list_group_offsets({t.consumer_group: None})
                    committed = res.get(t.consumer_group, {}) or {}
                except Exception:  # noqa: BLE001
                    committed = {}

                end_sum = sum(end.get(tp, 0) for tp in tps)
                committed_sum = 0
                lag = 0
                for tp in tps:
                    e = end.get(tp, 0)
                    c = committed.get(tp)
                    c_off = c.offset if (c and c.offset is not None and c.offset >= 0) else 0
                    committed_sum += c_off
                    lag += max(0, e - c_off)
                row.update({
                    "partitions": len(tps),
                    "end_offset": end_sum,
                    "committed": committed_sum,
                    "lag": lag,
                })
            except Exception as e:  # noqa: BLE001
                row["error"] = str(e)
            results.append(row)
    finally:
        try:
            if consumer:
                consumer.close(autocommit=False)
        except Exception:  # noqa: BLE001
            pass
        try:
            if admin:
                admin.close()
        except Exception:  # noqa: BLE001
            pass

    return results
