"""Load validated process configuration from environment variables."""

from typing import Literal

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Defines process-wide infrastructure and Stable protocol configuration."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_host: str = Field(default="127.0.0.1")
    app_port: int = Field(default=8000, ge=1, le=65535)
    app_debug: bool = Field(default=False)
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(default="INFO")

    database_url: str = Field(default="postgresql+asyncpg://perfcho:perfcho@127.0.0.1:55432/perfcho")
    database_pool_size: int = Field(default=20, ge=1)
    database_max_overflow: int = Field(default=30, ge=0)

    password_pepper: SecretStr = Field(default=SecretStr("perfcho-development-password-pepper"))
    password_pepper_version: int = Field(default=1, ge=1)
    token_hmac_key: SecretStr = Field(default=SecretStr("perfcho-development-token-hmac-key"))
    device_hmac_key: SecretStr = Field(default=SecretStr("perfcho-development-device-hmac-key"))

    redis_state_url: str = Field(default="redis://127.0.0.1:56379/0")
    redis_state_prefix: str = Field(default="perfcho:state")
    redis_socket_timeout: float = Field(default=5.0, gt=0)
    redis_presence_ttl_seconds: int = Field(default=360, ge=60)
    redis_mailbox_ttl_seconds: int = Field(default=600, ge=60)
    redis_mailbox_max_packets: int = Field(default=4096, ge=128)

    s3_endpoint_url: str = Field(default="http://127.0.0.1:59000")
    s3_region: str = Field(default="us-east-1")
    s3_access_key: SecretStr = Field(default=SecretStr("perfcho"))
    s3_secret_key: SecretStr = Field(default=SecretStr("perfcho-development"))
    s3_bucket: str = Field(default="perfcho")
    s3_addressing_style: Literal["path", "virtual"] = Field(default="path")
    object_stream_chunk_size: int = Field(default=1024 * 1024, ge=64 * 1024, le=16 * 1024 * 1024)

    taskiq_broker_url: str = Field(default="redis://127.0.0.1:56379/1")
    taskiq_queue_name: str = Field(default="perfcho:tasks")
    taskiq_consumer_group: str = Field(default="perfcho:workers")
    taskiq_stream_max_length: int = Field(default=100_000, ge=1000)

    outbox_batch_size: int = Field(default=100, ge=1, le=1000)
    outbox_poll_interval: float = Field(default=1.0, gt=0)
    outbox_lease_seconds: int = Field(default=300, ge=30)
    outbox_max_attempts: int = Field(default=10, ge=1)

    cors_origins: list[str] = Field(default=["http://localhost:3000", "http://localhost:5173"])


settings = Settings()
