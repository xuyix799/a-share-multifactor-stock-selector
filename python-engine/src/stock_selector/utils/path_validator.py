import posixpath
from pathlib import PurePosixPath


class PathValidationError(ValueError):
    pass


def safe_object_key(key: str) -> str:
    if not isinstance(key, str) or not key:
        raise PathValidationError("object key must be a non-empty string")
    if "\\" in key:
        raise PathValidationError("object key must use POSIX separators")
    if "\x00" in key:
        raise PathValidationError("object key must not contain null bytes")

    path = PurePosixPath(key)
    if path.is_absolute():
        raise PathValidationError("object key must be relative")
    if any(part in ("", ".", "..") for part in path.parts):
        raise PathValidationError("object key must not contain empty, current, or parent segments")
    if posixpath.normpath(key) != key:
        raise PathValidationError("object key must be normalized")
    return key
