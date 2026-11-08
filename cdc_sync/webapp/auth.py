"""监控面板登录鉴权。

单用户方案(无用户表)：
  - 用户名/密码从 config/settings.yaml 的 `panel` 段读取(user / password)。
    走 webapp 服务的 `--settings`(或环境变量 CDC_SETTINGS)指定的文件，与页面「配置」页一致；
    网页改完保存后立即生效(每次登录请求都重读，而非 import 时缓存)。
  - `panel` 段缺失或 password 为空时，回退内置默认值 root / cdc@123(仅用于首启/本地)。
  - 登录成功签发一个 HMAC 签名的 token，存为 HttpOnly Cookie。
    token 无服务端状态，重启不失效；改密码(改签名密钥)才使全员会话失效。
  - 暴力破解检测：同一 IP 1 分钟内密码错误 ≥10 次触发告警
    （logging.warning + 内存缓存 + 持久化到 config/alerts.yaml）。
"""
from __future__ import annotations

import base64
import collections
import hashlib
import hmac
import json
import logging
import os
import secrets
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml

from .. import config, config_store

log = logging.getLogger("cdc_sync.auth")

# ---- 会话参数(可用环境变量覆盖) ---------------------------------------------
# 单次登录默认有效 12 小时；可用 CDC_PANEL_SESSION_SECONDS(秒)覆盖
SESSION_SECONDS = int(os.environ.get("CDC_PANEL_SESSION_SECONDS", str(12 * 3600)))
COOKIE_NAME = "cdc_panel_session"
# 面板可能经反代暴露到公网，默认按 HTTPS 处理（Cookie 只在 TLS 连接上发送）。
# 纯内网 http 直连调试时可显式置 CDC_PANEL_COOKIE_SECURE=0。
COOKIE_SECURE = os.environ.get("CDC_PANEL_COOKIE_SECURE", "1") == "1"

# 回退内置凭据仅用于本地开发；可通过 allow_default_password 显式开启。
# 生产（尤其公网暴露）必须配置真实密码，否则拒绝登录。
DEFAULT_USER = "root"
DEFAULT_PASSWORD = "cdc@123"

# 不需要会话即可访问的 API 路径（精确匹配，防前缀绕过）
PUBLIC_API_PATHS = frozenset({"/api/login", "/api/logout", "/api/health", "/api/me"})

# ---- 暴力破解检测 ------------------------------------------------------------
# 滑动窗口：同一 IP 在 BRUTE_WINDOW_SECONDS 内失败次数 >= BRUTE_THRESHOLD 触发告警
BRUTE_WINDOW_SECONDS: int = 60
BRUTE_THRESHOLD: int = 10
# 告警时间统一按东八区(+8)显示，不随部署机器时区变化
_TZ_CST = timezone(timedelta(hours=8))
# 内存 + 文件各最多保留 200 条
_ALERT_MAX_KEEP: int = 200

# 告警持久化文件（gitignored，重启后不丢失）
_ALERTS_FILE: Path = (
    Path(os.environ.get("CDC_SETTINGS") or config_store.active_settings_path())
    .parent / "alerts.yaml"
)

_lock = threading.Lock()
# {ip: deque of fail timestamps}
_fail_ts: dict[str, collections.deque] = collections.defaultdict(
    lambda: collections.deque()
)
# 内存告警列表（启动时从文件加载）
_alerts: collections.deque = collections.deque(maxlen=_ALERT_MAX_KEEP)


def _load_alerts_from_file() -> None:
    """进程启动时从 alerts.yaml 加载历史告警到内存 deque。"""
    try:
        if not _ALERTS_FILE.exists():
            return
        raw = yaml.safe_load(_ALERTS_FILE.read_text(encoding="utf-8")) or []
        if isinstance(raw, list):
            for entry in raw[-_ALERT_MAX_KEEP:]:
                if isinstance(entry, dict):
                    _alerts.append(entry)
    except Exception:  # noqa: BLE001
        pass  # 文件损坏不影响服务启动


def _save_alerts_to_file() -> None:
    """把当前内存告警列表写入 alerts.yaml（在 _lock 内调用）。"""
    try:
        _ALERTS_FILE.parent.mkdir(parents=True, exist_ok=True)
        _ALERTS_FILE.write_text(
            yaml.safe_dump(list(_alerts), allow_unicode=True, sort_keys=False),
            encoding="utf-8",
        )
    except Exception:  # noqa: BLE001
        pass  # 写文件失败不应阻断主流程


def record_fail(ip: str) -> None:
    """记录一次密码错误；窗口内累计达阈值时写告警日志并追加到告警列表与文件。"""
    now = time.time()
    cutoff = now - BRUTE_WINDOW_SECONDS
    with _lock:
        dq = _fail_ts[ip]
        dq.append(now)
        # 清理窗口外的旧记录
        while dq and dq[0] < cutoff:
            dq.popleft()
        count = len(dq)
        # 恰好越过阈值时触发一次告警（避免每次失败都重复告警）
        if count == BRUTE_THRESHOLD:
            entry = {
                "time": datetime.fromtimestamp(now, _TZ_CST).strftime("%Y-%m-%d %H:%M:%S"),
                "type": "brute_force",
                "action": "login_fail",
                "ip": ip,
                "count": count,
                "window_seconds": BRUTE_WINDOW_SECONDS,
                "detail": f"{count} 次密码错误（{BRUTE_WINDOW_SECONDS}s 内）",
            }
            _alerts.append(entry)
            _save_alerts_to_file()
            log.warning(
                "⚠️  登录暴力破解告警：IP %s 在 %d 秒内密码错误 %d 次",
                ip, BRUTE_WINDOW_SECONDS, count,
            )


def get_alerts() -> list[dict]:
    """返回全部告警记录（最新在前）。"""
    with _lock:
        return list(reversed(_alerts))


def record_alert(entry: dict) -> None:
    """记录任意告警条目（登录以外的风险操作告警）。"""
    with _lock:
        _alerts.append(entry)
        _save_alerts_to_file()


# 模块加载时从文件恢复历史告警
_load_alerts_from_file()




def _panel_conf() -> dict:
    """当前生效的 panel 段（每次重读 settings.yaml，网页改动即时生效）。"""
    try:
        return (config_store.read_settings_dict().get("panel") or {})
    except Exception:  # noqa: BLE001
        return {}


def _allow_default_password(panel: dict | None = None) -> bool:
    panel = panel if panel is not None else _panel_conf()
    return config._as_bool(panel.get("allow_default_password"), False)


def password_configured() -> bool:
    """是否已配置真实面板密码（或显式允许默认口令）。"""
    panel = _panel_conf()
    pw = (panel.get("password") or "").strip()
    # 未解析的 ${ENV} 占位符不算已配置，避免把占位串当密码用
    if pw and not (pw.startswith("${") and pw.endswith("}")):
        return True
    return _allow_default_password(panel)


def _credentials() -> tuple[str, str] | None:
    """当前生效的 (username, password)；未配置密码且未允许默认口令时返回 None。

    面板可能直接暴露到公网，内置默认口令等于没有鉴权，因此默认拒绝：
    只有 settings 里配了非空密码，或显式设置 allow_default_password: true 才放行。
    """
    panel = _panel_conf()
    user = (panel.get("user") or "").strip() or DEFAULT_USER
    password = (panel.get("password") or "").strip()
    if password:
        return user, password
    if _allow_default_password(panel):
        return user, DEFAULT_PASSWORD
    return None


def verify_password(username: str, password: str) -> bool:
    """常量时间比对，防时序侧信道。未配置密码时一律拒绝。"""
    creds = _credentials()
    if creds is None:
        return False
    user, pw = creds
    user_ok = hmac.compare_digest(username.encode("utf-8"), user.encode("utf-8"))
    pw_ok = hmac.compare_digest(password.encode("utf-8"), pw.encode("utf-8"))
    return user_ok and pw_ok


def _key() -> bytes:
    """以当前密码推导签名密钥。改密码 => 旧 token 签名对不上 => 全员强制下线。

    未配置密码时用一个固定的无效密钥：既不签发可用 token，也让已存在的 token 全部失效。
    """
    creds = _credentials()
    pw = creds[1] if creds else "\x00no-panel-password\x00"
    return hashlib.sha256(("cdc-panel:" + pw).encode("utf-8")).digest()


def _b64e(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64d(s: str) -> bytes:
    return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))


def _sign(payload_b64: str) -> str:
    return _b64e(hmac.new(_key(), payload_b64.encode("ascii"), hashlib.sha256).digest())


def issue_token(username: str) -> str:
    """签发一个会话 token: base64(payload).base64(sig)"""
    payload = {"u": username, "n": secrets.token_hex(8), "exp": int(time.time()) + SESSION_SECONDS}
    payload_b64 = _b64e(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    return f"{payload_b64}.{_sign(payload_b64)}"


def validate_token(token: str) -> str | None:
    """校验 token；有效返回用户名，无效/过期返回 None。"""
    try:
        payload_b64, sig = token.rsplit(".", 1)
    except (ValueError, AttributeError):
        return None
    if not hmac.compare_digest(sig, _sign(payload_b64)):
        return None
    try:
        payload = json.loads(_b64d(payload_b64).decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return None
    if payload.get("exp", 0) <= time.time():
        return None
    return payload.get("u", DEFAULT_USER)
