"""生成 ClickHouse 三对象建表 SQL（Kafka 表 / 正式表 / 物化视图）。

对齐设计方案 §7.2。核心逻辑：
- 推断 ORDER BY（去重键）：优先 tables.yaml 的 order_by，其次主键；无则告警退化。
- 推断 PARTITION BY：优先 partition_by 时间列 → toYYYYMM(col)；无合适列则不分区。
- Kafka 表列不加 Nullable/COMMENT；正式表可空列包 Nullable，ORDER BY 列强制非空。
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, StrictUndefined

from .config import ROOT, Settings, TableDef
from . import type_map

log = logging.getLogger("cdc_sync.ck")

TEMPLATE_DIR = ROOT / "templates"

_env = Environment(
    loader=FileSystemLoader(str(TEMPLATE_DIR)),
    undefined=StrictUndefined,
    trim_blocks=True,
    lstrip_blocks=True,
    keep_trailing_newline=True,
)


@dataclass
class TableSQL:
    target_table: str
    kafka_sql: str
    target_sql: str
    mv_sql: str

    def ordered(self) -> list[tuple[str, str]]:
        """执行顺序：正式表 → Kafka 表 → MV（MV 最后，避免消费到未建的正式表）。"""
        return [
            (f"{self.target_table} (target)", self.target_sql),
            (f"{self.target_table}_kafka", self.kafka_sql),
            (f"mv_{self.target_table}", self.mv_sql),
        ]


def _resolve_order_by(table: TableDef) -> str:
    if table.order_by:
        return table.order_by
    pks = [c.name for c in table.columns if c.is_pk]
    if pks:
        # 反引号包裹，兼容 group/key/type 等保留字列名
        return ", ".join(f"`{name}`" for name in pks)
    log.warning(
        "表 %s.%s 无主键且未指定 order_by：退化为按全部列去重（append 语义可能重复），"
        "建议在 tables.yaml 显式指定唯一键。",
        table.source_database, table.source_table,
    )
    return ", ".join(f"`{c.name}`" for c in table.columns)


def _resolve_partition(table: TableDef) -> str | None:
    col = table.partition_by
    if not col:
        # 未指定：不自动挑列，保持稳定（避免误选）。返回 None → 不分区。
        return None
    match = next((c for c in table.columns if c.name == col), None)
    if match is None:
        log.warning("表 %s 指定的 partition_by=%s 不在列中，忽略分区。", table.target_table, col)
        return None
    if type_map.is_time_type(match.mysql_type):
        return f"toYYYYMM(`{col}`)"
    if type_map.is_int_type(match.mysql_type):
        # bigint 存 Unix 时间戳（本项目大量表如此，如 logs.created_at）。
        # 秒/毫秒无法从类型判断，用 partition_unit 声明；默认按秒。
        unit = (table.partition_unit or "s").lower()
        if unit in ("ms", "millis", "millisecond"):
            return f"toYYYYMM(toDateTime(intDiv(`{col}`, 1000)))"
        if unit in ("s", "sec", "second"):
            return f"toYYYYMM(toDateTime(`{col}`))"
        log.warning(
            "表 %s 的 partition_unit=%s 非法（应为 s 或 ms），忽略分区。",
            table.target_table, unit,
        )
        return None
    log.warning(
        "表 %s 的 partition_by=%s 类型 %s 不支持分区（需时间类型或整数时间戳），忽略分区。",
        table.target_table, col, match.mysql_type,
    )
    return None


def _storage_settings(table: TableDef) -> str | None:
    """正式表的存储位置 SETTINGS 片段；未配置返回 None。

    注意：必须建表时指定，建好后不可改（只能重建 + 迁数据）。
    仅正式表需要 —— Kafka 表数据在 Kafka、MV 只转发，都不落盘。
    """
    if table.disk:
        return f"disk = '{table.disk}'"
    if table.storage_policy:
        return f"storage_policy = '{table.storage_policy}'"
    return None


def _order_by_columns(order_by: str) -> set[str]:
    return {p.strip().strip("`") for p in order_by.split(",") if p.strip()}


def meta_column_names(table: TableDef) -> tuple[str, str]:
    """返回 (版本列名, 软删列名)。默认 version/is_deleted；
    若与业务列撞名则加前缀，避免 CREATE 时列名冲突（如 setups.version）。"""
    names = {c.name for c in table.columns}
    ver = "version" if "version" not in names else "_cdc_version"
    dele = "is_deleted" if "is_deleted" not in names else "_cdc_deleted"
    return ver, dele


def _kafka_settings_block(table: TableDef, settings: Settings, ck_major: int | None) -> str:
    """组装 Kafka 引擎 SETTINGS（Python 侧，便于按 CK 大版本适配）。

    注：kafka_broker_list/topic_list/group_name/format 等设置名在 CK 22.x~26.x 一致，
    此处集中管理，未来若某大版本设置名有差异，只需在这里按 ck_major 分支即可。
    """
    quoted = {
        "kafka_broker_list": settings.kafka_broker_list,
        "kafka_topic_list": table.topic,
        "kafka_group_name": table.consumer_group,   # 注意：是 group_name，不是 group_id
        # Debezium 用 Confluent Schema Registry 的 Avro 线格式 → CK 必须用 AvroConfluent（不是 Avro）
        "kafka_format": "AvroConfluent",
        # AvroConfluent 会按 schema id 去这个地址取 schema；CK 在宿主机，用宿主机可达地址
        "format_avro_schema_registry_url": settings.schema_registry_url_for_ck,
    }
    lines = [f"    {k} = '{v}'" for k, v in quoted.items()]
    # ---- Kafka SASL 认证（CK 22.x+ 均支持 kafka_sasl_* 设置）----
    sasl = settings.kafka_sasl
    if sasl.enabled:
        # CK 的 security_protocol 取值与 Java 客户端不同：SASL_SSL → sasl_ssl
        proto = "sasl_ssl" if sasl.security_protocol.upper() == "SASL_SSL" else "sasl_plaintext"
        lines.append(f"    kafka_security_protocol = '{proto}'")
        lines.append(f"    kafka_sasl_mechanism = '{sasl.mechanism}'")
        lines.append(f"    kafka_sasl_username = '{sasl.username}'")
        lines.append(f"    kafka_sasl_password = '{sasl.password}'")
    lines.append("    kafka_skip_broken_messages = 1")
    lines.append("    kafka_max_block_size = 65536")   # 大批量落盘，提升消费吞吐
    # 消费稳定性：单消费者(MV)就够(tasks.max=1→1分区)，poll batch 对齐 max_block_size
    lines.append("    kafka_poll_max_batch_size = 65536")
    return ",\n".join(lines)


def build_table_sql(table: TableDef, settings: Settings, ck_major: int | None = None) -> TableSQL:
    if not table.has_columns():
        raise ValueError(
            f"表 {table.source_database}.{table.source_table} 无列定义，"
            f"请先 introspect 或在 tables.yaml 内联 columns。"
        )

    db = table.target_database
    order_by = _resolve_order_by(table)
    partition_expr = _resolve_partition(table)
    ob_cols = _order_by_columns(order_by)

    # Kafka 表列：不加 Nullable（Kafka 引擎按扁平事件解析），保持源类型
    kafka_lines = [
        f"    `{c.name}` {type_map.map_type(c.mysql_type, nullable=False)},"
        for c in table.columns
    ]

    # 正式表列：可空包 Nullable，但 ORDER BY 列强制非空
    target_lines = []
    for c in table.columns:
        in_key = c.name in ob_cols
        ck_type = type_map.map_type(
            c.mysql_type,
            nullable=c.nullable,
            allow_nullable=not in_key,
        )
        target_lines.append(f"    `{c.name}` {ck_type},")

    # MV 映射列：与业务列同名直传
    mv_lines = [f"    `{c.name}`," for c in table.columns]

    ver_col, del_col = meta_column_names(table)

    common = {
        "database": db,
        "target_table": table.target_table,
        "kafka_table": table.kafka_table,
        "mv_name": table.mv_name,
    }

    kafka_sql = _env.get_template("ck_kafka.sql.j2").render(
        **common,
        columns_block="\n".join(kafka_lines),
        kafka_settings_block=_kafka_settings_block(table, settings, ck_major),
    )
    target_sql = _env.get_template("ck_target.sql.j2").render(
        **common,
        columns_block="\n".join(target_lines),
        order_by=order_by,
        partition_expr=partition_expr,
        storage_settings=_storage_settings(table),
        comment=table.comment,
        ver_col=ver_col,
        del_col=del_col,
    )
    mv_sql = _env.get_template("ck_mv.sql.j2").render(
        **common,
        columns_block="\n".join(mv_lines),
        ver_col=ver_col,
        del_col=del_col,
    )

    return TableSQL(
        target_table=table.target_table,
        kafka_sql=kafka_sql,
        target_sql=target_sql,
        mv_sql=mv_sql,
    )


def write_sql_files(table_sql: TableSQL, out_dir: Path) -> list[Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    combined = "\n".join(
        [table_sql.target_sql, table_sql.kafka_sql, table_sql.mv_sql]
    )
    path = out_dir / f"{table_sql.target_table}.sql"
    path.write_text(combined, encoding="utf-8")
    written.append(path)
    return written
