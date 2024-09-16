import json

import xxhash

from flive.serialize import SerializedParams
from flive.types import Hash


def dict_hash(params: SerializedParams) -> Hash:
    # Convert parameters to a sorted, compact JSON string
    json_string = json.dumps(
        params, sort_keys=True, ensure_ascii=True, separators=(",", ":")
    )
    # Generate a hash of the JSON string
    return xxhash.xxh3_64_hexdigest(json_string)



