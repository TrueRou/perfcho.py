"""Small explicit codecs for cacheable immutable domain values."""

import json
from dataclasses import fields, is_dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any


def encode_json(value: Any) -> bytes:
    return json.dumps({"v": 1, "value": _encode(value)}, separators=(",", ":")).encode()


def decode_json(raw: bytes) -> Any:
    payload = json.loads(raw)
    if payload.get("v") != 1:
        raise ValueError("unknown cache value version")
    return _decode(payload["value"])


def _encode(value: Any) -> Any:
    if is_dataclass(value):
        return {field.name: _encode(getattr(value, field.name)) for field in fields(value)}
    if isinstance(value, (tuple, frozenset, set)):
        return [_encode(item) for item in value]
    if isinstance(value, datetime):
        return {"__type__": "datetime", "value": value.isoformat()}
    if isinstance(value, Decimal):
        return {"__type__": "decimal", "value": str(value)}
    if isinstance(value, bytes):
        return {"__type__": "bytes", "value": value.hex()}
    if isinstance(value, dict):
        return {str(key): _encode(item) for key, item in value.items()}
    return value


def _decode(value: Any) -> Any:
    if isinstance(value, list):
        return [_decode(item) for item in value]
    if isinstance(value, dict):
        value_type = value.get("__type__")
        if value_type == "datetime":
            return datetime.fromisoformat(value["value"])
        if value_type == "decimal":
            return Decimal(value["value"])
        if value_type == "bytes":
            return bytes.fromhex(value["value"])
        return {key: _decode(item) for key, item in value.items()}
    return value
