"""검사를 통과한 Silver 후보를 로컬/S3 Parquet으로 고정한다."""
from __future__ import annotations

import hashlib
import io
import json
import re
from dataclasses import asdict, dataclass

import pandas as pd

from pipeline.common.sink import read_bytes, write_bytes
from pipeline.silver_quality import QUALITY_RULESET_VERSION


@dataclass(frozen=True)
class CandidatePart:
    dataset: str
    partition_key: str
    object_uri: str
    row_count: int
    sha256: str
    input_fingerprint: str
    ruleset_version: str


def _safe(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.=-]+", "_", value)


class CandidateStore:
    def __init__(self, root_uri: str):
        self.root_uri = root_uri.rstrip("/")

    def _uris(self, dataset: str, partition_key: str) -> tuple[str, str]:
        stem = f"{self.root_uri}/{dataset}/{_safe(partition_key)}"
        return f"{stem}.parquet", f"{stem}.json"

    def save(
        self,
        dataset: str,
        partition_key: str,
        frame: pd.DataFrame,
        input_fingerprint: str,
    ) -> CandidatePart:
        object_uri, metadata_uri = self._uris(dataset, partition_key)
        buffer = io.BytesIO()
        frame.to_parquet(buffer, index=False)
        payload = buffer.getvalue()
        part = CandidatePart(
            dataset=dataset,
            partition_key=partition_key,
            object_uri=object_uri,
            row_count=len(frame),
            sha256=hashlib.sha256(payload).hexdigest(),
            input_fingerprint=input_fingerprint,
            ruleset_version=QUALITY_RULESET_VERSION,
        )
        write_bytes(payload, object_uri)
        write_bytes(
            json.dumps(asdict(part), ensure_ascii=False, sort_keys=True).encode(),
            metadata_uri,
        )
        return part

    def metadata(
        self,
        dataset: str,
        partition_key: str,
        input_fingerprint: str,
    ) -> CandidatePart | None:
        object_uri, metadata_uri = self._uris(dataset, partition_key)
        raw = read_bytes(metadata_uri)
        if raw is None:
            return None
        part = CandidatePart(**json.loads(raw))
        if (
            part.object_uri != object_uri
            or part.input_fingerprint != input_fingerprint
            or part.ruleset_version != QUALITY_RULESET_VERSION
        ):
            return None
        return part

    def load(self, part: CandidatePart) -> pd.DataFrame:
        payload = read_bytes(part.object_uri)
        if payload is None:
            raise RuntimeError(f"candidate object is missing: {part.object_uri}")
        actual = hashlib.sha256(payload).hexdigest()
        if actual != part.sha256:
            raise RuntimeError(
                f"candidate checksum mismatch: {part.object_uri} "
                f"expected={part.sha256}, actual={actual}"
            )
        frame = pd.read_parquet(io.BytesIO(payload))
        if len(frame) != part.row_count:
            raise RuntimeError(
                f"candidate row count mismatch: {part.object_uri} "
                f"expected={part.row_count}, actual={len(frame)}"
            )
        return frame

    def save_run_manifest(
        self,
        parts: list[CandidatePart],
        *,
        input_fingerprint: str,
    ) -> str:
        body = {
            "input_fingerprint": input_fingerprint,
            "ruleset_version": QUALITY_RULESET_VERSION,
            "parts": [asdict(part) for part in parts],
        }
        encoded = json.dumps(body, ensure_ascii=False, sort_keys=True).encode()
        uri = f"{self.root_uri}/manifest.json"
        write_bytes(encoded, uri)
        return uri
