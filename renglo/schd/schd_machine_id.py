"""Stable per-laptop id for the local scheduler (ebe).

Automatic and transparent: OS machine id when available, otherwise a UUID
cached in ~/.renglo/schd_machine_id. Never put this in git or env_config.
"""

from __future__ import annotations

import os
import subprocess
import sys
import uuid
from pathlib import Path

_CACHE = Path.home() / ".renglo" / "schd_machine_id"
_cached: str | None = None


def schd_machine_id() -> str:
    global _cached
    if _cached:
        return _cached
    from_env = str(os.environ.get("SCHD_MACHINE_ID") or "").strip()
    if from_env:
        _cached = from_env
        return _cached
    disk = _read_cache()
    if disk:
        _cached = disk
        return _cached
    resolved = _os_machine_id() or uuid.uuid4().hex
    _write_cache(resolved)
    _cached = resolved
    return _cached


def _read_cache() -> str:
    try:
        text = _CACHE.read_text().strip()
        return text if text else ""
    except OSError:
        return ""


def _write_cache(value: str) -> None:
    try:
        _CACHE.parent.mkdir(parents=True, exist_ok=True)
        _CACHE.write_text(value + "\n")
    except OSError:
        pass


def _os_machine_id() -> str:
    if sys.platform == "darwin":
        return _macos_platform_uuid()
    if sys.platform.startswith("linux"):
        for path in (Path("/etc/machine-id"), Path("/var/lib/dbus/machine-id")):
            try:
                text = path.read_text().strip()
                if text:
                    return text
            except OSError:
                continue
        return ""
    if sys.platform == "win32":
        return _windows_machine_guid()
    return ""


def _macos_platform_uuid() -> str:
    try:
        out = subprocess.check_output(
            ["ioreg", "-rd1", "-c", "IOPlatformExpertDevice"],
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return ""
    for line in out.splitlines():
        if "IOPlatformUUID" not in line:
            continue
        parts = line.split('"', 3)
        if len(parts) >= 4:
            return parts[3].rstrip('"').strip()
        _, _, rest = line.partition("=")
        return rest.strip().strip('"')
    return ""


def _windows_machine_guid() -> str:
    try:
        import winreg
    except ImportError:
        return ""
    try:
        key = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\Microsoft\Cryptography")
        try:
            value, _typ = winreg.QueryValueEx(key, "MachineGuid")
        finally:
            winreg.CloseKey(key)
        return str(value or "").strip()
    except OSError:
        return ""
