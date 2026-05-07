import json
import os
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Union


_LOCKS: dict[str, threading.RLock] = {}


def _get_lock(path: Path) -> threading.RLock:
    key = str(path.resolve())
    if key not in _LOCKS:
        _LOCKS[key] = threading.RLock()
    return _LOCKS[key]


def read_json(path: Union[str, Path], default: Any) -> Any:
    file_path = Path(path)
    lock = _get_lock(file_path)
    with lock:
        if not file_path.exists():
            return _clone_default(default)
        try:
            with file_path.open("r", encoding="utf-8") as fh:
                return json.load(fh)
        except (OSError, json.JSONDecodeError):
            return _clone_default(default)


def write_json_atomic(path: Union[str, Path], data: Any) -> None:
    file_path = Path(path)
    lock = _get_lock(file_path)
    with lock:
        file_path.parent.mkdir(parents=True, exist_ok=True)
        fd, temp_path = tempfile.mkstemp(prefix=file_path.stem + "_", suffix=".tmp", dir=str(file_path.parent))
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(data, fh, ensure_ascii=False, indent=4)
            last_error: Exception | None = None
            for attempt in range(10):
                try:
                    os.replace(temp_path, file_path)
                    last_error = None
                    break
                except PermissionError as exc:
                    last_error = exc
                    time.sleep(0.1 * (attempt + 1))
            if last_error is not None:
                with file_path.open("w", encoding="utf-8") as fh:
                    json.dump(data, fh, ensure_ascii=False, indent=4)
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)


def ensure_json_file(path: Union[str, Path], default: Any) -> Any:
    file_path = Path(path)
    if not file_path.exists():
        write_json_atomic(file_path, _clone_default(default))
    return read_json(file_path, default)


def _clone_default(default: Any) -> Any:
    if isinstance(default, dict):
        return default.copy()
    if isinstance(default, list):
        return default[:]
    return default
