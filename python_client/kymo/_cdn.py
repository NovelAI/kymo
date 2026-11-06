"""Pure helpers for kymo's content-addressed CDN wire format."""

import hashlib
import json
import sys


_IMAGE_CONTENT_TYPES = {
    "png": "image/png",
    "jpg": "image/jpeg",
    "jpeg": "image/jpeg",
    "webp": "image/webp",
    "gif": "image/gif",
}


def _manifest_text(value: str) -> str:
    """Return display text that every UTF-8 JSON consumer can decode."""
    return value.encode("utf-8", errors="replace").decode("utf-8")


def _numpy_number(o):
    """Return the exact bool, int or float of a numpy scalar of dtype kind b, i, u or f, or o itself when it is none of those. Never imports numpy: such a scalar can only exist once it is loaded."""
    generic = getattr(sys.modules.get("numpy"), "generic", None)
    if not (isinstance(generic, type) and isinstance(o, generic)):
        return o
    kind = o.dtype.kind
    if kind in "biu":
        return o.item()
    if kind != "f":
        return o
    value = float(o)
    # float16 and float32 always fit a float; a longdouble often does not (most fractional ones on x86), nor one past the float range, which float() would make inf. A NaN takes the token path.
    if o == o and value != o:
        raise ValueError(f"numpy {o.dtype} {o} has no exact float64 value")
    return value


def _metadata_default(o):
    """json's default for configs and metadata, called only for values json cannot encode itself: numpy scalars as their exact Python number, arrays and tensors as .tolist() gives them, sets as lists in a fixed order, and anything else as its str(). Never imports numpy or torch."""
    number = _numpy_number(o)
    if number is not o:
        return number
    ndarray = getattr(sys.modules.get("numpy"), "ndarray", None)
    if isinstance(ndarray, type) and isinstance(o, ndarray):
        # As its scalars are: .tolist() turns units finer than a microsecond into plain ints, and the array's printed text drops elements past 1,000.
        return (o.astype(str) if o.dtype.kind in "mM" else o).tolist()
    # A stub or half-imported torch has no Tensor type.
    tensor = getattr(sys.modules.get("torch"), "Tensor", None)
    if isinstance(tensor, type) and isinstance(o, tensor):
        return o.tolist()
    if isinstance(o, (set, frozenset)):
        # Iteration order follows the hash seed; this text does not.
        return sorted(o, key=lambda e: json.dumps(e, default=_metadata_default))
    return str(o)


def _numpy_keys_converted(data):
    """Return data with every numpy scalar key converted by _numpy_number, in each container json walks: each dict, subclasses included, rebuilt as a plain dict in its order, and each list and tuple."""
    if isinstance(data, dict):
        return {_numpy_number(k): _numpy_keys_converted(v) for k, v in data.items()}
    if isinstance(data, (list, tuple)):
        return [_numpy_keys_converted(v) for v in data]
    return data


def _unique_keys(pairs: list) -> dict:
    """json.loads' object_pairs_hook: raise where keys of one dict were stored as the same text (1 and "1", inf and "Infinity", two NaN), rather than keep only the last."""
    data = dict(pairs)
    if len(data) < len(pairs):
        seen = set()
        for key, _ in pairs:
            if key in seen:
                raise ValueError(f"more than one key of a dict is stored as {key!r}")
            seen.add(key)
    return data


def _freeze_json(data, label: str):
    """Return data as the JSON tree its manifest stores, owning every value, or raise ValueError naming label for data that cannot be stored exactly."""
    encode = json.JSONEncoder(separators=(",", ":"), default=_metadata_default).encode
    try:
        try:
            text = encode(data)
        except TypeError:
            # A numpy scalar key, which json never passes to default. Input that encodes as it is pays nothing.
            text = encode(_numpy_keys_converted(data))
        return json.loads(text, object_pairs_hook=_unique_keys)
    except Exception as error:
        raise ValueError(f"{label} cannot be stored as JSON: {error}") from error


def content_id(data: bytes, extension: str) -> str:
    """Return the resource id produced by the CDN upload endpoint."""
    return f"{hashlib.sha256(data).hexdigest()}.{extension.lower()}"


# These bytes are frozen. Replay rebuilds a manifest from spooled parts and must reproduce the exact bytes, and so the content ID, that an earlier client published under the same mutation version; any difference is DATA_LOSS. test_manifest_bytes_are_frozen pins them, and a format change needs a new "v". Payload key order is the caller's (pickle keeps it through the spool), so keys are not sorted.
def gallery_manifest(items: list[dict]) -> bytes:
    """Serialize an image or resource gallery manifest."""
    return json.dumps(
        {"v": 1, "class": "image_gallery", "items": items}, allow_nan=False
    ).encode("utf-8")


def metadata_manifest(data: dict) -> bytes:
    """Serialize a metadata manifest, in Python json's dialect: NaN, Infinity and -Infinity are written as those tokens."""
    return json.dumps(
        {"v": 1, "class": "metadata", "data": data},
        indent=2,
        default=_metadata_default,
    ).encode("utf-8")


def gallery_item(
    resource_id: str,
    *,
    extension: str | None = None,
    caption: str | None = None,
    content_type: str = "application/octet-stream",
    filename: str = "",
) -> dict:
    """Build the manifest entry for an image or generic resource."""
    if extension is not None:
        item = {
            "content_type": _IMAGE_CONTENT_TYPES.get(
                extension.lower(), "application/octet-stream"
            )
        }
        if caption:
            item["caption"] = _manifest_text(caption)
    else:
        item = {
            "content_type": _manifest_text(content_type),
            "filename": _manifest_text(filename),
        }
    item["resource"] = resource_id
    return item


def encoded_gallery_item(entry: dict, resource_id: str) -> dict:
    """Build the manifest entry for one encoded gallery entry, as live upload and spool replay both store it."""
    if entry["kind"] == "image":
        return gallery_item(
            resource_id,
            extension=entry["ext"],
            caption=entry.get("caption"),
        )
    return gallery_item(
        resource_id,
        content_type=entry.get("content_type", "application/octet-stream"),
        filename=entry.get("filename", ""),
    )
