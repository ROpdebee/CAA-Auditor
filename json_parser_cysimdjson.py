from typing import Any

from cysimdjson import JSONArray, JSONObject, JSONParser as _JSONParser

def parse(bjson: bytes) -> JSONObject:
    # https://github.com/TeskaLabs/cysimdjson/issues/17
    parser = _JSONParser()
    return parser.parse(bjson)

def parse_str(jsonstr: str) -> JSONObject:
    return parse(jsonstr.encode())

def safe_get(obj: JSONObject, key: str, default: Any = None) -> Any:
    if isinstance(obj, dict):
        return obj.get(key, default)
    path = key
    if not path.startswith('/'):
        path = f'/{path}'
    try:
        return obj.at_pointer(path)
    except KeyError:
        return default

def to_native(obj: Any) -> Any:
    if isinstance(obj, JSONObject):
        return {k: to_native(v) for k, v in obj.items()}
    elif isinstance(obj, JSONArray):
        return [to_native(e) for e in obj]
    return obj
