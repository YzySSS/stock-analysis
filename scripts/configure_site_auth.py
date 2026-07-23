from __future__ import annotations

import argparse
import getpass
import os
import re
import secrets
import sys
import tempfile
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.api.auth import USERNAME_PATTERN, hash_password  # noqa: E402


AUTH_KEYS = (
    "SITE_AUTH_USERNAME",
    "SITE_AUTH_PASSWORD_HASH",
    "SITE_AUTH_SESSION_SECRET",
    "SITE_AUTH_SESSION_TTL_SECONDS",
    "SITE_AUTH_COOKIE_SECURE",
)


def _read_current_values(env_path: Path) -> dict[str, str]:
    if not env_path.exists():
        return {}
    values: dict[str, str] = {}
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def _read_password(password_file: Path | None) -> str:
    if password_file is not None:
        if not password_file.is_file():
            raise SystemExit(f"密码文件不存在: {password_file}")
        mode = password_file.stat().st_mode & 0o777
        if mode & 0o077:
            raise SystemExit(f"密码文件权限必须禁止 group/other 访问，当前为 {mode:o}")
        password = password_file.read_text(encoding="utf-8").strip()
    else:
        password = getpass.getpass("新密码（至少 12 个字符）: ")
        confirmation = getpass.getpass("再次输入新密码: ")
        if not secrets.compare_digest(password, confirmation):
            raise SystemExit("两次输入的密码不一致")
    if len(password) < 12:
        raise SystemExit("密码至少需要 12 个字符")
    if len(password) > 1024:
        raise SystemExit("密码不能超过 1024 个字符")
    return password


def _replace_env_values(env_path: Path, values: dict[str, str]) -> None:
    original = env_path.read_text(encoding="utf-8") if env_path.exists() else ""
    seen: set[str] = set()
    output: list[str] = []
    for line in original.splitlines():
        match = re.match(r"^([A-Za-z_][A-Za-z0-9_]*)=", line)
        key = match.group(1) if match else None
        if key not in values:
            output.append(line)
            continue
        if key not in seen:
            output.append(f"{key}={values[key]}")
            seen.add(key)

    remaining = {key: value for key, value in values.items() if key not in seen}
    if remaining:
        if output and output[-1].strip():
            output.append("")
        output.append("# 应用登录（仅保存密码哈希，不保存明文密码）")
        for key in AUTH_KEYS:
            if key in remaining:
                output.append(f"{key}={remaining[key]}")

    payload = "\n".join(output).rstrip() + "\n"
    payload_lines = payload.splitlines()
    for key, value in values.items():
        matches = [line for line in payload_lines if line.startswith(f"{key}=")]
        if matches != [f"{key}={value}"]:
            raise RuntimeError(f"环境变量 {key} 写入校验失败")

    env_path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{env_path.name}.",
        dir=env_path.parent,
        text=True,
    )
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary_path, 0o600)
        os.replace(temporary_path, env_path)
    finally:
        if temporary_path.exists():
            temporary_path.unlink()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="安全配置股票分析站点的单管理员登录凭据。",
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=PROJECT_ROOT / ".env",
        help="要更新的 root-only 环境文件（默认：项目 .env）",
    )
    parser.add_argument(
        "--username",
        help="登录用户名；首次配置必填，后续省略时保留当前用户名",
    )
    parser.add_argument(
        "--password-file",
        type=Path,
        help="从权限为 600 的文件读取密码；省略时使用隐藏交互输入",
    )
    parser.add_argument(
        "--session-ttl-days",
        type=int,
        default=7,
        help="登录会话有效天数，1～31（默认：7）",
    )
    parser.add_argument(
        "--keep-session-secret",
        action="store_true",
        help="保留现有会话密钥；默认轮换并让全部旧会话失效",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    env_path = args.env_file.resolve()
    current = _read_current_values(env_path)
    username = str(args.username or current.get("SITE_AUTH_USERNAME", "")).strip()
    if not username:
        raise SystemExit("首次配置必须提供 --username")
    if not USERNAME_PATTERN.fullmatch(username):
        raise SystemExit("用户名只能包含字母、数字、点、下划线、@ 或连字符，最长 64 位")
    if not 1 <= args.session_ttl_days <= 31:
        raise SystemExit("--session-ttl-days 必须在 1～31 之间")

    password = _read_password(args.password_file.resolve() if args.password_file else None)
    existing_secret = str(current.get("SITE_AUTH_SESSION_SECRET", "")).strip()
    keep_existing_secret = args.keep_session_secret and len(existing_secret) >= 32
    session_secret = existing_secret if keep_existing_secret else secrets.token_urlsafe(48)

    _replace_env_values(
        env_path,
        {
            "SITE_AUTH_USERNAME": username,
            "SITE_AUTH_PASSWORD_HASH": hash_password(password),
            "SITE_AUTH_SESSION_SECRET": session_secret,
            "SITE_AUTH_SESSION_TTL_SECONDS": str(args.session_ttl_days * 24 * 60 * 60),
            "SITE_AUTH_COOKIE_SECURE": "true",
        },
    )
    print(f"站点登录配置已更新: username={username}")
    print(f"环境文件: {env_path} (mode=600)")
    print(f"旧会话失效: {'否' if keep_existing_secret else '是'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
