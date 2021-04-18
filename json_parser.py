from typing import Any

try:
    from json_parser_cysimdjson import JSONArray, JSONObject, parse, parse_str, safe_get, to_native
except ImportError:
    print('Using slow built-in json parsing, install cysimdjson')
    from json_parser_builtin import JSONArray, JSONObject, parse, parse_str, safe_get, to_native  # type: ignore[misc]
