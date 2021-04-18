from typing import Any

import json

parse = json.loads
parse_str = json.loads

safe_get = dict.get

def to_native(obj: Any) -> Any:
    return obj

JSONArray = list
JSONObject = dict
