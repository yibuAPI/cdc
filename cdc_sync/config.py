"""配置加载：settings.yaml（集群连接）+ tables.yaml（接入表清单）。

- 支持 ${ENV_VAR} 环境变量插值，避免明文密码入库。
- 解析为 dataclass，供各模块类型安全地取用。
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

_ENV_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*(?::-[^}]*)?)\}")

# 项目根目录（本文件位于 <root>/cdc_sync/config.py）
ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = ROOT / "config"
DEFAULT_SETTINGS = CONFIG_DIR / "settings.yaml"
DEFAULT_TABLES = CONFIG_DIR / "tables.yaml"


def _interpolate_env(value: Any) -> Any:
    """递归地把字符串里的 ${VAR} / ${VAR:-default} 替换为环境变量值。"""
    if isinstance(value, str):
        def repl(m: re.Match) -> str:
            expr = m.group(1)
            var, sep, default = expr.partition(":-")
            env = os.environ.get(var)
            if env is None:
                if sep:  # 提供了 :-默认（允许默认值为空字符串）
                    return default
                raise ConfigError(f"环境变量未设置: {var}（在配置中被 ${{{var}}} 引用）")
            return env
        return _ENV_PATTERN.sub(repl, value)
    if isinstance(value, dict):
        return {k: _interpolate_env(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_interpolate_env(v) for v in value]
    return value


class ConfigError(Exception):
    """配置缺失/非法时抛出，CLI 捕获后给出友好报错。"""


def _load_yaml(path: Path) -> dict:
    if not path.exists():
        raise ConfigError(f"配置文件不存在: {path}")
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ConfigError(f"配置文件格式非法（应为映射）: {path}")
    return data


# ----------------------------- 集群设置 -----------------------------

@dataclass
class MySQLConf:
    host: str = "127.0.0.1"
    port: int = 3306
    user: str = "debezium"
    password: str = ""
    charset: str = "utf8mb4"


@dataclass
class ClickHouseConf:
    host: str = "127.0.0.1"
    port: int = 8123
    user: str = "default"
    password: str = ""
    database: str = "default"
    secure: bool = False


@dataclass
class DebeziumConf:
    connector_name: str = "mysql-business-connector"
    server_name: str = "mysql-business"
    server_id: int = 1001
    tasks_max: int = 4
    snapshot_mode: str = "initial"
    history_topic: str = "schema-changes.mysql-business"


@dataclass
class PanelConf:
    """监控面板登录（单用户）。

    allow_default_password=False（默认）时，settings 里密码为空将拒绝启动/拒绝登录，
    不再回退内置默认口令 —— 面板可能暴露到公网，默认口令等于没有鉴权。
    """
    user: str = "root"
    password: str = ""
    allow_default_password: bool = False


@dataclass
class KafkaSaslConf:
    """Kafka 客户端 SASL 认证配置。

    开启后必须同时提供 username/password；四个客户端（Connect worker、Debezium
    连接器、ClickHouse Kafka 引擎、监控面板）共用同一份凭据，避免各写一套。

    默认 PLAIN：KRaft 模式下 SCRAM 凭据必须在 kafka-storage format 时预置，
    否则客户端无法登录（详见 docker/README.md「SASL 凭据」一节）。
    """
    enabled: bool = False
    security_protocol: str = "SASL_PLAINTEXT"   # SASL_PLAINTEXT | SASL_SSL
    mechanism: str = "PLAIN"                    # PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512

    username: str = ""
    password: str = ""

    def jaas_config(self) -> str:
        """供 Java 客户端（Connect / Debezium）使用的 JAAS 串。"""
        module = "org.apache.kafka.common.security.scram.ScramLoginModule" \
            if self.mechanism.upper().startswith("SCRAM") \
            else "org.apache.kafka.common.security.plain.PlainLoginModule"
        # 密码中的引号需转义，避免拼出非法 JAAS 串
        user = self.username.replace("\\", "\\\\").replace('"', '\\"')
        pwd = self.password.replace("\\", "\\\\").replace('"', '\\"')
        return f'{module} required username="{user}" password="{pwd}";'

    def connector_settings(self) -> dict:
        """Debezium / Connect 连接器里的 SASL 属性（无认证时返回空 dict）。"""
        if not self.enabled:
            return {}
        return {
            "security.protocol": self.security_protocol,
            "sasl.mechanism": self.mechanism,
            "sasl.jaas.config": self.jaas_config(),
        }

    def python_kwargs(self) -> dict:
        """kafka-python（Consumer/AdminClient/Producer）的关键字参数。"""
        if not self.enabled:
            return {}
        return {
            "security_protocol": self.security_protocol,
            "sasl_mechanism": self.mechanism,
            "sasl_plain_username": self.username,
            "sasl_plain_password": self.password,
        }


@dataclass
class Settings:
    mysql: MySQLConf = field(default_factory=MySQLConf)
    clickhouse: ClickHouseConf = field(default_factory=ClickHouseConf)
    kafka_broker_list: str = "kafka:9092"
    kafka_internal_broker_list: str = "kafka:9092"
    kafka_sasl: KafkaSaslConf = field(default_factory=KafkaSaslConf)
    schema_registry_url: str = "http://schema-registry:8081"
    schema_registry_url_for_ck: str = "http://localhost:8081"
    connect_url: str = "http://connect:8083"
    # Connect REST 未做鉴权时，公网必须靠反代挡；此处仅供 CLI/面板内网直连
    debezium: DebeziumConf = field(default_factory=DebeziumConf)
    panel: PanelConf = field(default_factory=PanelConf)


def _as_int(v, default: int) -> int:
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


def _as_bool(v, default: bool = False) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.strip().lower() in ("1", "true", "yes", "on")
    return default


def load_settings(path: Path | str | None = None) -> Settings:
    path = Path(path) if path else DEFAULT_SETTINGS
    raw = _interpolate_env(_load_yaml(path))
    return build_settings(raw)


def build_settings(raw: dict) -> Settings:
    """从已解析的 dict 构造 Settings（不读文件、不做 env 插值）。

    供 config_store / 连通性测试用「未保存的表单值」直接构造配置。
    """
    kafka = raw.get("kafka") or {}
    broker_list = kafka.get("broker_list", "kafka:9092")

    my = raw.get("mysql") or {}
    ck = raw.get("clickhouse") or {}
    dbz = raw.get("debezium") or {}

    mysql = MySQLConf(
        host=my.get("host", "127.0.0.1"),
        port=_as_int(my.get("port"), 3306),
        user=my.get("user", "debezium"),
        password=my.get("password", ""),
        charset=my.get("charset", "utf8mb4"),
    )
    clickhouse = ClickHouseConf(
        host=ck.get("host", "127.0.0.1"),
        port=_as_int(ck.get("port"), 8123),
        user=ck.get("user", "default"),
        password=ck.get("password", ""),
        database=ck.get("database", "default"),
        secure=_as_bool(ck.get("secure"), False),
    )
    debezium = DebeziumConf(
        connector_name=dbz.get("connector_name", "mysql-business-connector"),
        server_name=dbz.get("server_name", "mysql-business"),
        server_id=_as_int(dbz.get("server_id"), 1001),
        tasks_max=_as_int(dbz.get("tasks_max"), 4),
        snapshot_mode=dbz.get("snapshot_mode", "initial"),
        history_topic=dbz.get("history_topic", "schema-changes.mysql-business"),
    )
    sr = raw.get("schema_registry") or {}
    panel = raw.get("panel") or {}
    sasl = kafka.get("sasl") or {}
    return Settings(
        mysql=mysql,
        clickhouse=clickhouse,
        kafka_broker_list=broker_list,
        # 容器内组件（Connect history / 监控）用的内部地址；缺省回退宿主地址
        kafka_internal_broker_list=kafka.get("internal_broker_list", broker_list),
        kafka_sasl=KafkaSaslConf(
            enabled=_as_bool(sasl.get("enabled"), False),
            security_protocol=(sasl.get("security_protocol") or "SASL_PLAINTEXT").strip(),
            mechanism=(sasl.get("mechanism") or "SCRAM-SHA-512").strip(),
            username=sasl.get("username") or "",
            password=sasl.get("password") or "",
        ),
        schema_registry_url=sr.get("url", "http://schema-registry:8081"),
        # CK(宿主机原生) 用的 Schema Registry 地址；缺省宿主机映射端口
        schema_registry_url_for_ck=sr.get("url_for_ck", "http://localhost:8081"),
        connect_url=(raw.get("connect") or {}).get("url", "http://connect:8083"),
        debezium=debezium,
        panel=PanelConf(
            user=panel.get("user", "root"),
            password=panel.get("password", ""),
            allow_default_password=_as_bool(panel.get("allow_default_password"), False),
        ),
    )


# ----------------------------- 表清单 -----------------------------

@dataclass
class ColumnDef:
    """一列的源端定义。mysql_type 为 information_schema 的 COLUMN_TYPE，如 'bigint unsigned'。"""
    name: str
    mysql_type: str
    nullable: bool = True
    is_pk: bool = False


@dataclass
class TableDef:
    source_database: str
    source_table: str
    target_table: str
    target_database: str = "default"
    order_by: str | None = None       # 未指定则用主键推断
    partition_by: str | None = None   # 未指定则尝试推断时间列
    # partition_by 为整数时间戳列时，声明单位：s(秒，默认) / ms(毫秒)
    partition_unit: str | None = None
    # 存储位置（仅正式表用；Kafka 表/MV 不落盘）。二者互斥，disk 优先。
    # 必须在建表时指定 —— 建好后改不了（只能重建 + 迁数据）。
    disk: str | None = None            # 单盘，如 ext_disk（需在 CK storage.xml 定义）
    storage_policy: str | None = None  # 存储策略，如 hot_cold（多盘/分层时用）
    comment: str = ""
    columns: list[ColumnDef] = field(default_factory=list)  # 空则需 introspect

    @property
    def topic(self) -> str:
        """Debezium 默认 Topic 命名：mysql.<db>.<table> —— 对齐设计方案 §5.3。"""
        return f"mysql.{self.source_database}.{self.source_table}"

    @property
    def kafka_table(self) -> str:
        return f"{self.target_table}_kafka"

    @property
    def mv_name(self) -> str:
        return f"mv_{self.target_table}"

    @property
    def consumer_group(self) -> str:
        return f"ck-{self.target_table}-consumer-group"

    def has_columns(self) -> bool:
        return bool(self.columns)


@dataclass
class TablesConfig:
    target_database: str
    tables: list[TableDef]


def load_tables(path: Path | str | None = None) -> TablesConfig:
    path = Path(path) if path else DEFAULT_TABLES
    raw = _load_yaml(path)
    default_db = raw.get("target_database", "default")
    tables: list[TableDef] = []
    for i, t in enumerate(raw.get("tables") or []):
        if not isinstance(t, dict):
            raise ConfigError(f"tables[{i}] 格式非法")
        for key in ("source_database", "source_table", "target_table"):
            if not t.get(key):
                raise ConfigError(f"tables[{i}] 缺少必填字段: {key}")
        cols = [
            ColumnDef(
                name=c["name"],
                mysql_type=c["mysql_type"],
                nullable=bool(c.get("nullable", True)),
                is_pk=bool(c.get("is_pk", False)),
            )
            for c in (t.get("columns") or [])
        ]
        if t.get("disk") and t.get("storage_policy"):
            raise ConfigError(
                f"tables[{i}] ({t['source_table']}) 的 disk 与 storage_policy 互斥，只能配其一"
            )
        tables.append(
            TableDef(
                source_database=t["source_database"],
                source_table=t["source_table"],
                target_table=t["target_table"],
                target_database=t.get("target_database", default_db),
                order_by=t.get("order_by"),
                partition_by=t.get("partition_by"),
                partition_unit=t.get("partition_unit"),
                disk=t.get("disk"),
                storage_policy=t.get("storage_policy"),
                comment=t.get("comment", ""),
                columns=cols,
            )
        )
    if not tables:
        raise ConfigError("tables.yaml 未定义任何表")
    return TablesConfig(target_database=default_db, tables=tables)
