"""Pure helpers for kymo's content-addressed CDN wire format."""

import hashlib
import json


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
    """Serialize a metadata manifest; raises on non-finite or non-JSON values."""
    return json.dumps(
        {"v": 1, "class": "metadata", "data": data},
        indent=2,
        default=str,
        allow_nan=False,
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
