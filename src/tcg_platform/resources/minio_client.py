import io
import os
from typing import Optional

from dagster import ConfigurableResource
from dagster._config.pythonic_config.resource import InitResourceContext
from minio import Minio
from minio.error import S3Error
from pydantic import model_validator


class MinioClientResource(ConfigurableResource):
    endpoint: str
    access_key: str
    secret_key: str
    bucket_name: str
    secure: bool = False

    _client: Optional[Minio] = None

    @model_validator(mode="after")
    def check_credentials(self) -> "MinioClientResource":
        if not self.access_key or not self.secret_key:
            raise ValueError("MINIO_ACCESS_KEY and MINIO_SECRET_KEY must be set")
        return self

    def create_resource(self, context: InitResourceContext) -> "MinioClientResource":
        self.setup_for_execution(context)
        return self

    def setup_for_execution(self, context: InitResourceContext) -> None:
        import sys
        sys.stderr.write(f"[MinioClientResource] setup_for_execution called. endpoint={self.endpoint} bucket={self.bucket_name}\n")
        sys.stderr.flush()
        self._client = Minio(
            self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=self.secure,
        )
        sys.stderr.write(f"[MinioClientResource] _client set to: {self._client}\n")
        sys.stderr.flush()
        self._ensure_bucket_exists()
        sys.stderr.write(f"[MinioClientResource] setup done. _client={getattr(self, '_client', 'MISSING')}\n")
        sys.stderr.flush()

    def _ensure_bucket_exists(self) -> None:
        if not self._client:
            raise RuntimeError("MinIO client not initialized")
        try:
            if not self._client.bucket_exists(self.bucket_name):
                self._client.make_bucket(self.bucket_name)
        except S3Error as e:
            raise RuntimeError(f"Failed to create bucket '{self.bucket_name}': {e}")

    def put_object(
        self,
        bucket_name: str,
        object_name: str,
        data: bytes,
        length: int,
        content_type: str = "application/octet-stream",
    ) -> None:
        if not self._client:
            raise RuntimeError("MinIO client not initialized")
        try:
            data_io = io.BytesIO(data)
            self._client.put_object(
                bucket_name,
                object_name,
                data_io,
                length,
                content_type=content_type,
            )
        except S3Error as e:
            raise RuntimeError(f"Failed to put object '{object_name}': {e}")

    def get_object(self, bucket_name: str, object_name: str) -> bytes:
        if not self._client:
            raise RuntimeError("MinIO client not initialized")
        try:
            response = self._client.get_object(bucket_name, object_name)
            data = response.read()
            response.close()
            response.release_conn()
            return data
        except S3Error as e:
            raise RuntimeError(f"Failed to get object '{object_name}': {e}")

    def list_objects(self, bucket_name: str, prefix: str = "") -> list[str]:
        if not self._client:
            raise RuntimeError("MinIO client not initialized")
        try:
            objects = self._client.list_objects(bucket_name, prefix=prefix, recursive=True)
            return [obj.object_name for obj in objects]
        except S3Error as e:
            raise RuntimeError(f"Failed to list objects: {e}")

    @property
    def client(self) -> Minio:
        if not self._client:
            raise RuntimeError("MinIO client not initialized")
        return self._client
