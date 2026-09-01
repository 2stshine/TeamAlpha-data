"""Publish bundled immutable KIND support bodies to the Bronze bucket."""
from __future__ import annotations

import hashlib
import os

import boto3
from botocore.exceptions import ClientError

from pipeline.resources.kind_support_seed import BODY_SEEDS


def run() -> None:
    bucket = os.environ["S3_BRONZE_BUCKET"]
    s3 = boto3.client("s3")
    for key, payload in sorted(BODY_SEEDS.items()):
        expected = key.rsplit("sha256=", 1)[-1].removesuffix(".html")
        actual = hashlib.sha256(payload).hexdigest()
        if actual != expected:
            raise RuntimeError(f"bundled KIND support body digest drifted: {key}")
        try:
            s3.put_object(
                Bucket=bucket,
                Key=key,
                Body=payload,
                ContentType="text/html",
                IfNoneMatch="*",
            )
        except ClientError as exc:
            code = str(exc.response.get("Error", {}).get("Code") or "")
            if code not in {
                "PreconditionFailed", "412",
                "ConditionalRequestConflict", "409",
            }:
                raise
            response = s3.get_object(Bucket=bucket, Key=key)
            body = response["Body"]
            try:
                existing = body.read()
            finally:
                body.close()
            if existing != payload:
                raise RuntimeError(
                    f"immutable KIND support body conflicts with S3: {key}"
                ) from exc
        print(
            f"[kind-support-seed] verified {key} sha256={actual}",
            flush=True,
        )


if __name__ == "__main__":
    run()
