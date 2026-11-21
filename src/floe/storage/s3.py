"""S3 storage client on obstore, ported from `io/storage/providers/s3.rs` (`S3Client`)."""

import os
from datetime import UTC, datetime, timedelta
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, Any

from floe.errors import FloeIoError, FloeStorageError
from floe.extras import missing
from floe.storage.paths import fnv1a_hex, parse_s3_uri
from floe.storage.types import ObjectRef, StoredObject

if TYPE_CHECKING:
    import obstore as obs
    from obstore import PutMode
    from obstore.auth.boto3 import Boto3CredentialProvider
    from obstore.exceptions import (
        AlreadyExistsError,
        PermissionDeniedError,
        PreconditionError,
        UnauthenticatedError,
    )
    from obstore.store import ClientConfig, ObjectStore, S3Config
else:
    try:
        import obstore as obs
        from obstore.exceptions import (
            AlreadyExistsError,
            PermissionDeniedError,
            PreconditionError,
            UnauthenticatedError,
        )
    except ImportError:
        # no s3 extra: `S3Client()` raises, so nothing below is ever caught
        AlreadyExistsError = PermissionDeniedError = PreconditionError = UnauthenticatedError = Exception  # fmt: skip

# #939: the metadata key archive copies carried before the #1271 index; still read, so older archives keep their D28 order
_DEBUG_DUMP = (
    "\n\nDebug source:"  # obstore appends the Rust `Debug` of the error to its message
)
_SOURCE_MTIME = "floe-source-mtime"
# S3 answers a failed `If-None-Match` or `If-Match` with 412, or 409 while a concurrent write is in flight: obstore raises these for all of them
_CONFLICT = (PreconditionError, AlreadyExistsError)


def resolve_path_style_access(configured: bool | None) -> bool:
    if configured is not None:
        return configured
    env = os.environ.get("AWS_S3_FORCE_PATH_STYLE", "").strip()
    return env == "1" or env.lower() == "true"


def _fail(what: str, e: Exception, where: str = "") -> FloeStorageError:
    """`what` failed with obstore error `e`; the code follows its class (a Rust debug dump after the message is cut)."""
    msg = f"s3 {what} failed{where}: {str(e).partition(_DEBUG_DUMP)[0]}"
    if isinstance(e, PermissionDeniedError | UnauthenticatedError):
        return FloeStorageError(msg, code="storage_access_denied")
    if isinstance(e, FileNotFoundError):
        return FloeStorageError(msg, code="storage_not_found")
    if isinstance(e, _CONFLICT):
        return FloeStorageError(msg, code="storage_conflict")
    return FloeStorageError(msg, code="storage_error")


def _aws_credentials() -> "Boto3CredentialProvider | None":
    """With boto3 installed, the AWS default chain (`~/.aws` profiles, `AWS_PROFILE`, SSO); else None, and obstore's own
    environment, web identity and container or instance role chain applies. No credentials at all also falls to that chain;
    any other failure (an expired SSO token, an unknown profile) is raised, not hidden behind an instance-role lookup."""
    try:
        from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError
        from obstore.auth.boto3 import Boto3CredentialProvider
    except ImportError:
        return None
    try:
        # ponytail: the ttl is fixed, not the credentials' own expiry; botocore renews SSO and assumed roles behind it, so
        # a role shorter than 5 minutes would be sent expired. Upgrade: a provider that returns botocore's `_expiry_time`.
        return Boto3CredentialProvider(ttl=timedelta(minutes=5))
    except (NoCredentialsError, ValueError):  # ValueError: boto3 found no credentials
        return None
    except (BotoCoreError, ClientError) as e:
        profile = os.environ.get("AWS_PROFILE", "default")
        raise FloeStorageError(f"aws credentials for profile {profile} failed: {e}", code="storage_access_denied") from e  # fmt: skip


def _iso(ts: datetime) -> str:
    return ts.astimezone(UTC).isoformat().replace("+00:00", "Z")


def temp_path_for_key(temp_dir: Path, key: str) -> Path:
    name = key.rsplit("/", 1)[-1] or "object"
    safe = "".join(
        c if c.isascii() and (c.isalnum() or c in "-_.") else "_" for c in name
    )
    return temp_dir / f"{fnv1a_hex(key)}_{safe}"


class S3Client:
    """The `bucket` is the only one a client reaches: URIs are reduced to their key.

    Threads: one client is shared by the entity threads (`resolve.py`). An obstore store, like a boto3 client, holds no mutable state (a Rust
    `object_store` handle over a pooled HTTP client, `Send + Sync`) and its calls release the GIL, so it needs no lock.
    """

    def __init__(
        self,
        bucket: str,
        region: str | None = None,
        endpoint: str | None = None,
        path_style_access: bool | None = None,
    ) -> None:
        try:
            from obstore.store import S3Store
        except ImportError as e:
            raise missing("s3", "an s3 storage") from e
        self.bucket = bucket
        self._list_args = (
            region,
            endpoint,
            resolve_path_style_access(path_style_access),
        )
        env = os.environ
        custom = endpoint or env.get("AWS_ENDPOINT_URL_S3") or env.get("AWS_ENDPOINT_URL")  # fmt: skip
        # identifies the store behind a bucket name
        self.endpoint: str = (custom or f"https://s3.{region or env.get('AWS_REGION') or env.get('AWS_DEFAULT_REGION') or 'us-east-1'}.amazonaws.com").rstrip("/")  # fmt: skip
        credentials = _aws_credentials()
        # without boto3 the credentials and the rest (AWS_ACCESS_KEY_ID, ...) are read from the environment by obstore
        config: S3Config = (
            credentials.config.copy() if credentials else {}
        )  # the session's region; ours wins
        # boto3's `auto`: virtual-hosted on AWS (a dotted bucket name breaks its TLS name), else path style
        config["virtual_hosted_style_request"] = not resolve_path_style_access(path_style_access) and custom is None and "." not in bucket  # fmt: skip
        if region:
            config["region"] = region
        if custom:
            config["endpoint"] = custom
        options: ClientConfig = {"connect_timeout": timedelta(seconds=10)}
        if self.endpoint.startswith("http://"):
            options["allow_http"] = True
        self._store: ObjectStore = S3Store(
            bucket,
            config=config,
            client_options=options,
            # near boto3's old 5 attempts and 60 s timeouts (obstore: 10 retries, 180 s)
            retry_config={"max_retries": 4, "retry_timeout": timedelta(seconds=60)},
            credential_provider=credentials,
        )

    def resolve_uri(self, path: str) -> str:
        key = path.lstrip("/")
        return f"s3://{self.bucket}/{key}" if key else f"s3://{self.bucket}"

    @cached_property
    def _lister(self) -> Any:
        """boto3 lists with the raw prefix; obstore only lists directories (`in/data_` becomes `in/data_/`), which an IAM
        `s3:prefix` condition on `in/data_*` refuses. Built on first use: a boto3 session costs 55-120 ms."""
        import boto3
        from botocore.config import Config

        region, endpoint, path_style = self._list_args
        config = Config(
            s3={"addressing_style": "path"} if path_style else None,
            # boto3 >= 1.36 adds CRC checksums that S3-compatible services may reject.
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        )
        make = boto3.session.Session().client  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]  # stubs cover s3 only
        return make("s3", region_name=region, endpoint_url=endpoint, config=config)

    def list(self, prefix: str, start_after: str = "") -> list[ObjectRef]:
        """Keys under `prefix`; D30: only those after `start_after`."""
        from botocore.exceptions import BotoCoreError, ClientError

        args: dict[str, str] = {"Bucket": self.bucket, "Prefix": prefix}
        if start_after:
            args["StartAfter"] = start_after
        try:
            pages = self._lister.get_paginator("list_objects_v2").paginate(**args)
            return [
                ObjectRef(f"s3://{self.bucket}/{o['Key']}", o["Key"], _iso(o["LastModified"]), o["Size"])
                for page in pages
                for o in page.get("Contents", [])
            ]  # fmt: skip
        except ClientError as e:
            code = str(e.response.get("Error", {}).get("Code", ""))
            msg = f"s3 list objects failed for bucket {self.bucket}: {e}"
            if code in ("AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch"):
                raise FloeStorageError(msg, code="storage_access_denied") from e
            if code == "NoSuchBucket":
                raise FloeStorageError(msg, code="storage_not_found") from e
            raise FloeStorageError(msg, code="storage_error") from e
        except BotoCoreError as e:
            msg = f"s3 list objects failed for bucket {self.bucket}: {e}"
            raise FloeStorageError(msg, code="storage_error") from e

    def download(self, uri: str, temp_dir: Path) -> Path:
        key = parse_s3_uri(uri)[1]
        dest = temp_path_for_key(temp_dir, key)
        try:
            dest.parent.mkdir(parents=True, exist_ok=True)
            with dest.open("wb") as f:
                for chunk in obs.get(self._store, key):
                    f.write(chunk)
        except Exception as e:
            raise _fail("get object", e) from e
        return dest

    def write(self, uri: str, body: bytes) -> None:
        try:
            obs.put(self._store, parse_s3_uri(uri)[1], body)
        except Exception as e:
            raise _fail("put object", e) from e

    def copy(self, src_uri: str, dst_uri: str) -> None:
        try:
            obs.copy(self._store, parse_s3_uri(src_uri)[1], parse_s3_uri(dst_uri)[1])
        except Exception as e:
            raise _fail("copy object", e) from e

    def source_mtime(self, uri: str) -> str | None:
        """#939: the source mtime in the metadata of an archive copy made before the index (#1271); None otherwise."""
        try:
            head = obs.get(self._store, parse_s3_uri(uri)[1], options={"head": True})
            return head.attributes.get(_SOURCE_MTIME)
        except Exception as e:
            raise _fail("head object", e, f" for {uri}") from e

    def delete(self, uri: str) -> None:
        try:
            obs.delete(self._store, parse_s3_uri(uri)[1])
        except Exception as e:
            raise _fail("delete object", e) from e

    def read(self, uri: str) -> StoredObject | None:
        try:
            got = obs.get(self._store, parse_s3_uri(uri)[1])
            return StoredObject(got.bytes().to_bytes(), got.meta["e_tag"] or "")
        except FileNotFoundError:
            return None
        except Exception as e:
            raise _fail("get object", e) from e

    def write_conditional(
        self, uri: str, expected_version: str | None, body: bytes
    ) -> str | None:
        """Create only if absent (`expected_version` None), else replace only if the ETag still matches; None on a lost race."""
        mode: PutMode = (
            "create" if expected_version is None else {"e_tag": expected_version}
        )
        try:
            return obs.put(self._store, parse_s3_uri(uri)[1], body, mode=mode)["e_tag"] or ""  # fmt: skip
        except _CONFLICT:
            return None
        except Exception as e:
            raise _fail("put object", e) from e


def read_s3(uri: str, what: str) -> bytes:
    # S3 settings come from the environment (AWS_ENDPOINT_URL, ...), as for `-o s3://`
    found = S3Client(parse_s3_uri(uri)[0]).read(uri)
    if found is None:
        raise FloeIoError(f"{what} not found: {uri}", code="io_not_found")
    return found.body
