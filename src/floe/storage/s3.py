"""S3 storage client, ported from `io/storage/providers/s3.rs` (`S3Client`)."""

import os
import shutil
from datetime import UTC, datetime
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from floe.errors import FloeStorageError
from floe.storage.paths import fnv1a_hex, parse_s3_uri
from floe.storage.types import ObjectRef, StoredObject

_CONFLICT = ("PreconditionFailed", "ConditionalRequestConflict", "412", "409")
_NOT_FOUND = ("NoSuchKey", "NotFound", "404")


def resolve_path_style_access(configured: bool | None) -> bool:
    if configured is not None:
        return configured
    env = os.environ.get("AWS_S3_FORCE_PATH_STYLE", "").strip()
    return env == "1" or env.lower() == "true"


def _code(err: ClientError) -> str:
    return str(err.response.get("Error", {}).get("Code", ""))


def _is(err: ClientError, codes: tuple[str, ...]) -> bool:
    status = str(err.response.get("ResponseMetadata", {}).get("HTTPStatusCode", ""))
    return _code(err) in codes or status in codes


def _iso(ts: datetime | None) -> str | None:
    return ts and ts.astimezone(UTC).isoformat().replace("+00:00", "Z")


def temp_path_for_key(temp_dir: Path, key: str) -> Path:
    name = key.rsplit("/", 1)[-1] or "object"
    safe = "".join(
        c if c.isascii() and (c.isalnum() or c in "-_.") else "_" for c in name
    )
    return temp_dir / f"{fnv1a_hex(key)}_{safe}"


class S3Client:
    def __init__(
        self,
        bucket: str,
        region: str | None = None,
        endpoint: str | None = None,
        path_style_access: bool | None = None,
    ) -> None:
        self.bucket = bucket
        self._client = boto3.session.Session().client(  # pyright: ignore[reportUnknownMemberType]  # stubs cover s3 only
            "s3",
            region_name=region,
            endpoint_url=endpoint,
            config=Config(
                s3={"addressing_style": "path"}
                if resolve_path_style_access(path_style_access)
                else None,
                # boto3 >= 1.36 adds CRC checksums that S3-compatible services may reject.
                request_checksum_calculation="when_required",
                response_checksum_validation="when_required",
            ),
        )
        self.endpoint: str = self._client.meta.endpoint_url  # pyright: ignore[reportUnknownMemberType]  # identifies the store behind a bucket name

    def resolve_uri(self, path: str) -> str:
        key = path.lstrip("/")
        return f"s3://{self.bucket}/{key}" if key else f"s3://{self.bucket}"

    def list(self, prefix: str, start_after: str = "") -> list[ObjectRef]:
        """Keys under `prefix`; D30: only those after `start_after`."""
        try:
            paginator = self._client.get_paginator("list_objects_v2")
            pages = (
                paginator.paginate(Bucket=self.bucket, Prefix=prefix, StartAfter=start_after)
                if start_after
                else paginator.paginate(Bucket=self.bucket, Prefix=prefix)
            )  # fmt: skip
            return [
                ObjectRef(
                    f"s3://{self.bucket}/{o['Key']}",
                    o["Key"],
                    _iso(o.get("LastModified")),
                    o.get("Size"),
                )
                for page in pages
                for o in page.get("Contents", [])
                if "Key" in o
            ]
        except (ClientError, BotoCoreError) as e:
            raise FloeStorageError(
                f"s3 list objects failed for bucket {self.bucket}: {e}"
            ) from e

    def download(self, uri: str, temp_dir: Path) -> Path:
        bucket, key = parse_s3_uri(uri)
        dest = temp_path_for_key(temp_dir, key)
        try:
            body = self._client.get_object(Bucket=bucket, Key=key)["Body"]
            dest.parent.mkdir(parents=True, exist_ok=True)
            with dest.open("wb") as f:
                shutil.copyfileobj(body, f)
        except (ClientError, BotoCoreError, OSError) as e:
            raise FloeStorageError(f"s3 get object failed: {e}") from e
        return dest

    def write(self, uri: str, body: bytes) -> None:
        bucket, key = parse_s3_uri(uri)
        try:
            self._client.put_object(Bucket=bucket, Key=key, Body=body)
        except (ClientError, BotoCoreError) as e:
            raise FloeStorageError(f"s3 put object failed: {e}") from e

    def copy(self, src_uri: str, dst_uri: str) -> None:
        (sb, sk), (db, dk) = parse_s3_uri(src_uri), parse_s3_uri(dst_uri)
        try:
            self._client.copy_object(
                Bucket=db, Key=dk, CopySource={"Bucket": sb, "Key": sk}
            )
        except (ClientError, BotoCoreError) as e:
            raise FloeStorageError(f"s3 copy object failed: {e}") from e

    def delete(self, uri: str) -> None:
        bucket, key = parse_s3_uri(uri)
        try:
            self._client.delete_object(Bucket=bucket, Key=key)
        except (ClientError, BotoCoreError) as e:
            raise FloeStorageError(f"s3 delete object failed: {e}") from e

    def read(self, uri: str) -> StoredObject | None:
        bucket, key = parse_s3_uri(uri)
        try:
            resp = self._client.get_object(Bucket=bucket, Key=key)
            return StoredObject(resp["Body"].read(), resp.get("ETag", ""))
        except ClientError as e:
            if _is(e, _NOT_FOUND):
                return None
            raise FloeStorageError(f"s3 get object failed: {e}") from e
        except BotoCoreError as e:
            raise FloeStorageError(f"s3 get object failed: {e}") from e

    def write_conditional(
        self, uri: str, expected_version: str | None, body: bytes
    ) -> str | None:
        bucket, key = parse_s3_uri(uri)
        try:
            if expected_version is None:
                resp = self._client.put_object(
                    Bucket=bucket, Key=key, Body=body, IfNoneMatch="*"
                )
            else:
                resp = self._client.put_object(
                    Bucket=bucket, Key=key, Body=body, IfMatch=expected_version
                )
            return resp.get("ETag", "")
        except ClientError as e:
            if _is(e, _CONFLICT):
                return None
            raise FloeStorageError(f"s3 put object failed: {e}") from e
        except BotoCoreError as e:
            raise FloeStorageError(f"s3 put object failed: {e}") from e

    def delete_conditional(self, uri: str, expected_version: str | None) -> bool:
        """An If-Match delete; False on a conflict. `None` deletes nothing, as Rust: an object there is a concurrent runner's."""
        if expected_version is None:
            return True
        bucket, key = parse_s3_uri(uri)
        try:
            self._client.delete_object(Bucket=bucket, Key=key, IfMatch=expected_version)
            return True
        except ClientError as e:
            if _is(e, _CONFLICT):
                return False
            raise FloeStorageError(f"s3 delete object failed: {e}") from e
        except BotoCoreError as e:
            raise FloeStorageError(f"s3 delete object failed: {e}") from e
