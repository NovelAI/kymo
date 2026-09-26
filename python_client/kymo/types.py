"""
Types for rich metric logging (images, resources, metadata).

Each type validates its arguments at construction time. Silent type
mismatches (e.g. passing a tuple as a caption) would otherwise produce
a manifest JSON the frontend can't deserialize, leaving the gallery
mysteriously empty — fail fast at the call site instead.
"""

from typing import Any


class Image:
    """Wraps image data for logging. Encoding happens in the worker process.

    Args:
        data: Image data — numpy array (H,W,C uint8), PIL.Image,
              torch.Tensor, or raw bytes (already encoded).
        caption: Optional text caption.
        format: Image format for encoding (png, jpg, webp). For raw bytes,
                the default png asks the client to detect the encoded format;
                a non-default value is kept as an explicit extension override.
    """

    def __init__(self, data: Any, caption: str = "", format: str = "png"):
        if not isinstance(caption, str):
            raise TypeError(
                f"kymo.Image: caption must be str, got "
                f"{type(caption).__name__} ({caption!r})"
            )
        if not isinstance(format, str):
            raise TypeError(
                f"kymo.Image: format must be str, got {type(format).__name__}"
            )
        self.data = data
        self.caption = caption
        self.format = format


class Metadata:
    """Arbitrary dict data stored as JSON via CDN.

    Args:
        data: Any JSON-serializable dict.
    """

    def __init__(self, data: dict):
        if not isinstance(data, dict):
            raise TypeError(
                f"kymo.Metadata: data must be dict, got {type(data).__name__}"
            )
        self.data = data


class Resource:
    """Generic binary resource for CDN storage.

    Args:
        data: Bytes-like resource data, copied to immutable bytes.
        filename: Original filename (used for extension extraction).
        content_type: MIME type of the resource.
    """

    def __init__(
        self,
        data: bytes | bytearray | memoryview,
        filename: str,
        content_type: str = "application/octet-stream",
    ):
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise TypeError(
                f"kymo.Resource: data must be bytes-like, got {type(data).__name__}"
            )
        if not isinstance(filename, str):
            raise TypeError(
                f"kymo.Resource: filename must be str, got {type(filename).__name__}"
            )
        if not isinstance(content_type, str):
            raise TypeError(
                f"kymo.Resource: content_type must be str, got "
                f"{type(content_type).__name__}"
            )
        self.data = bytes(data)
        self.filename = filename
        self.content_type = content_type
