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
                mpo (a multi-picture JPEG) is stored as jpeg, whether named
                here or detected in raw bytes. An image in a format the CDN
                does not serve is dropped with a warning.
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
    """A dict stored as JSON via CDN, frozen when ``kymo.log`` is called. ``kymo.init`` and ``kymo.update_config`` store configs by the same rules.

    What is stored:

    - JSON values as they are, NaN, Infinity and -Infinity included: Python json's dialect, which ``json.loads`` reads back as floats.
    - A numpy bool, integer or floating scalar as its exact bool, int or float. A longdouble that a float cannot hold exactly raises.
    - A numpy array or a torch tensor as ``.tolist()`` gives it, each element by these rules; a datetime64 or timedelta64 array as the text of each element.
    - A set or frozenset as a list, sorted by each element's JSON text.
    - Anything else as ``str(value)``: a Path, datetime, Enum, Decimal, bytes, dataclass, numpy datetime64, timedelta64 or complex scalar, or a mapping that is not a dict. Pass ``dataclasses.asdict(x)`` or ``OmegaConf.to_container(cfg, resolve=True)`` to store their fields.

    Keys are JSON text. An int, float, bool or None key becomes the text json writes for it (``1`` as ``"1"``, inf as ``"Infinity"``), and a numpy bool, integer or float key is converted first. Other key types raise ValueError, and so do keys of one dict that become the same text, such as ``1`` and ``"1"`` or two NaN keys, rather than keep only the last.

    Tuples read back as lists, and IntEnum and StrEnum as plain values. NaN keeps neither its sign nor its payload. A lone surrogate (from ``os.fsdecode``) is stored as its exact ``\\udXXX`` escape. The client README lists the other limits.

    Args:
        data: The dict to store.
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
