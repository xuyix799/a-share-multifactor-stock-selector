from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

from stock_selector.utils.path_validator import safe_object_key


@dataclass(frozen=True)
class AtomicWriteResult:
    bucket: str
    final_key: str
    temp_key: str


class AtomicObjectWriter:
    def __init__(self, client, tmp_dir: Path | str | None = None):
        self.client = client
        self.tmp_dir = Path(tmp_dir) if tmp_dir else Path.cwd()
        self.tmp_dir.mkdir(parents=True, exist_ok=True)

    def write_bytes_atomic(self, bucket: str, final_key: str, data: bytes) -> AtomicWriteResult:
        local_path = self.tmp_dir / f".upload-{uuid4().hex}.bin"
        local_path.write_bytes(data)
        try:
            return self.write_file_atomic(bucket=bucket, final_key=final_key, source_path=local_path)
        finally:
            local_path.unlink(missing_ok=True)

    def write_file_atomic(self, bucket: str, final_key: str, source_path: Path | str) -> AtomicWriteResult:
        final_key = safe_object_key(final_key)
        source_path = Path(source_path)
        if not source_path.is_file():
            raise FileNotFoundError(source_path)

        temp_key = self._temp_key_for(final_key)
        uploaded_temp = False
        try:
            self.client.fput_object(bucket, temp_key, str(source_path))
            uploaded_temp = True
            self.client.stat_object(bucket, temp_key)
            self.client.fput_object(bucket, final_key, str(source_path))
            self.client.stat_object(bucket, final_key)
            return AtomicWriteResult(bucket=bucket, final_key=final_key, temp_key=temp_key)
        finally:
            if uploaded_temp:
                self.client.remove_object(bucket, temp_key)

    @staticmethod
    def _temp_key_for(final_key: str) -> str:
        path = PureObjectKey(final_key)
        return path.with_name(f".tmp-{uuid4().hex}-{path.name}")


class PureObjectKey:
    def __init__(self, key: str):
        self.key = key
        parts = key.rsplit("/", 1)
        if len(parts) == 1:
            self.parent = ""
            self.name = parts[0]
        else:
            self.parent, self.name = parts

    def with_name(self, name: str) -> str:
        if not self.parent:
            return safe_object_key(name)
        return safe_object_key(f"{self.parent}/{name}")
