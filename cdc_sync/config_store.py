"""配置读写：供 Web 面板 / CLI 增删改 settings 与 tables，含校验与原子写。

- settings：结构化 dict ⇄ config/settings.yaml（literal 值，可被网页保存）
- tables：原始 YAML 文本 ⇄ config/tables.yaml
- seed：从 ${ENV} 模板解析出 literal 值播种 settings.yaml（容器首启用）

所有写操作先写临时文件并用 config.py 的加载器校验，通过后原子替换，失败不破坏原文件。
"""
from __future__ import annotations

import os
from pathlib import Path

import yaml

from . import config
from .config import ConfigError


def active_settings_path() -> Path:
    return Path(os.environ.get("CDC_SETTINGS") or config.DEFAULT_SETTINGS)


def active_tables_path() -> Path:
    return Path(os.environ.get("CDC_TABLES") or config.DEFAULT_TABLES)


# ----------------------------- settings -----------------------------

_EMPTY_SETTINGS = {
    "mysql": {"host": "", "port": 3306, "user": "debezium", "password": "", "charset": "utf8mb4"},
    "clickhouse": {"host": "", "port": 8123, "user": "default", "password": "", "database": "default", "secure": False},
    "kafka": {
        "broker_list": "localhost:9094",
        "internal_broker_list": "kafka:9092",
        "sasl": {
            "enabled": False,
            "security_protocol": "SASL_PLAINTEXT",
            "mechanism": "SCRAM-SHA-512",
            "username": "",
            "password": "",
        },
    },
    "schema_registry": {"url": "http://schema-registry:8081", "url_for_ck": "http://localhost:8081"},
    "connect": {"url": "http://connect:8083"},
    "debezium": {"connector_name": "cdc-connector", "server_name": "cdc", "server_id": 1001,
                 "tasks_max": 4, "snapshot_mode": "initial", "history_topic": "schema-changes.cdc"},
    "panel": {"user": "root", "password": "", "allow_default_password": False},
}


def read_settings_dict() -> dict:
    """读当前 settings 文件为结构化 dict（literal）。文件缺失/含未解析 ${} 时回退空模板。"""
    path = active_settings_path()
    if not path.exists():
        return _EMPTY_SETTINGS
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError:
        return _EMPTY_SETTINGS
    if not isinstance(raw, dict):
        return _EMPTY_SETTINGS
    # 与空模板做浅合并，保证前端拿到所有分组键（兼容非法值如 null/数组/字符串）
    merged = {}
    for k, v in _EMPTY_SETTINGS.items():
        if isinstance(v, dict):
            src = raw.get(k)
            merged[k] = {**v, **(src if isinstance(src, dict) else {})}
        else:
            merged[k] = raw.get(k, v)
    # 保留模板之外的自定义段（如 ai）
    for k, v in raw.items():
        if k not in merged:
            merged[k] = v
    return merged


def save_settings_dict(data: dict) -> None:
    """校验并原子写入 settings 文件。校验失败抛 ConfigError，原文件不变。"""
    if not isinstance(data, dict):
        raise ConfigError("配置格式非法（应为对象）")

    # 数值字段显式校验（load_settings 的 _as_int 会静默回退，故这里先卡一道）
    numeric = [("mysql", "port"), ("clickhouse", "port"),
               ("debezium", "server_id"), ("debezium", "tasks_max")]
    for grp, key in numeric:
        v = (data.get(grp) or {}).get(key)
        if v is None or v == "":
            continue
        try:
            int(v)
        except (TypeError, ValueError):
            raise ConfigError(f"{grp}.{key} 必须为整数，收到：{v!r}")

    path = active_settings_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(yaml.safe_dump(data, allow_unicode=True, sort_keys=False), encoding="utf-8")
    try:
        config.load_settings(tmp)  # 用正式加载器校验（会做类型/env 检查）
    except Exception as e:  # noqa: BLE001
        tmp.unlink(missing_ok=True)
        raise ConfigError(f"配置校验失败：{e}") from e
    tmp.replace(path)


def seed_settings_from(env_template: Path | str, *, force: bool = False) -> tuple[bool, str]:
    """settings.yaml 不存在（或 force）时，从 ${ENV} 模板解析 literal 值写入。

    返回 (是否写入, 说明)。
    """
    dest = active_settings_path()
    if dest.exists() and not force:
        return False, f"{dest} 已存在，跳过播种"
    env_template = Path(env_template)
    if not env_template.exists():
        return False, f"模板不存在：{env_template}"
    # load_settings 会做 ${ENV} 插值，得到 literal 值
    s = config.load_settings(env_template)
    data = {
        "mysql": {"host": s.mysql.host, "port": s.mysql.port, "user": s.mysql.user,
                  "password": s.mysql.password, "charset": s.mysql.charset},
        "clickhouse": {"host": s.clickhouse.host, "port": s.clickhouse.port, "user": s.clickhouse.user,
                       "password": s.clickhouse.password, "database": s.clickhouse.database, "secure": s.clickhouse.secure},
        "kafka": {
            "broker_list": s.kafka_broker_list,
            "internal_broker_list": s.kafka_internal_broker_list,
            "sasl": {
                "enabled": s.kafka_sasl.enabled,
                "security_protocol": s.kafka_sasl.security_protocol,
                "mechanism": s.kafka_sasl.mechanism,
                "username": s.kafka_sasl.username,
                "password": s.kafka_sasl.password,
            },
        },
        "schema_registry": {"url": s.schema_registry_url, "url_for_ck": s.schema_registry_url_for_ck},
        "connect": {"url": s.connect_url},
        "debezium": {"connector_name": s.debezium.connector_name, "server_name": s.debezium.server_name,
                     "server_id": s.debezium.server_id, "tasks_max": s.debezium.tasks_max,
                     "snapshot_mode": s.debezium.snapshot_mode, "history_topic": s.debezium.history_topic},
        "panel": {"user": s.panel.user, "password": s.panel.password,
                  "allow_default_password": s.panel.allow_default_password},
    }
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(yaml.safe_dump(data, allow_unicode=True, sort_keys=False), encoding="utf-8")
    return True, f"已播种 {dest}"


# ----------------------------- tables -----------------------------

def read_tables_text() -> tuple[str, int]:
    """返回 (tables.yaml 原始文本, 表数量)。缺失返回空模板。"""
    path = active_tables_path()
    if not path.exists():
        return "target_database: default\ntables: []\n", 0
    text = path.read_text(encoding="utf-8")
    try:
        cfg = config.load_tables(path)
        count = len(cfg.tables)
    except Exception:  # noqa: BLE001
        count = -1  # 存在但当前不合法
    return text, count


def save_tables_text(text: str) -> int:
    """校验并原子写入 tables.yaml。返回表数量。校验失败抛 ConfigError。"""
    # 先 YAML 语法校验
    try:
        yaml.safe_load(text)
    except yaml.YAMLError as e:
        raise ConfigError(f"YAML 语法错误：{e}") from e

    path = active_tables_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    try:
        cfg = config.load_tables(tmp)  # 结构校验
    except Exception as e:  # noqa: BLE001
        tmp.unlink(missing_ok=True)
        raise ConfigError(f"表清单校验失败：{e}") from e
    tmp.replace(path)
    return len(cfg.tables)


def write_tables_from_parsed(doc: dict) -> int:
    """把 import-sql 产出的 dict 写入 tables.yaml（复用 save 的校验）。"""
    text = yaml.safe_dump(doc, allow_unicode=True, sort_keys=False, default_flow_style=False, width=200)
    return save_tables_text(text)
