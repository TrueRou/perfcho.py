"""Load validated process configuration from environment variables."""

from ipaddress import ip_network
from math import ceil
from typing import Literal, Self

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Defines process-wide infrastructure and Stable protocol configuration."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_host: str = Field(default="127.0.0.1")
    app_port: int = Field(default=8000, ge=1, le=65535)
    app_debug: bool = Field(default=False)
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(default="INFO")
    log_format: Literal["human", "json"] = Field(default="human")
    log_http_success_sample_rate: float = Field(default=0.05, ge=0, le=1)
    log_stable_poll_sample_rate: float = Field(default=0.01, ge=0, le=1)
    log_hot_path_sample_rate: float = Field(default=0.001, ge=0, le=1)
    log_slow_request_ms: int = Field(default=1000, ge=1)
    trusted_proxy_cidrs: list[str] = Field(default_factory=list)

    database_url: str = Field(default="postgresql+asyncpg://perfcho:perfcho@127.0.0.1:55432/perfcho")
    database_pool_size: int = Field(default=20, ge=1)
    database_max_overflow: int = Field(default=30, ge=0)

    password_pepper: SecretStr = Field(default=SecretStr("perfcho-development-password-pepper"))
    password_pepper_version: int = Field(default=1, ge=1)
    argon2_time_cost: int = Field(default=3, ge=1)
    argon2_memory_cost_kib: int = Field(default=65_536, ge=8)
    argon2_parallelism: int = Field(default=4, ge=1)
    token_hmac_key: SecretStr = Field(default=SecretStr("perfcho-development-token-hmac-key"))
    device_hmac_key: SecretStr = Field(default=SecretStr("perfcho-development-device-hmac-key"))
    match_password_hmac_key: SecretStr = Field(default=SecretStr("perfcho-development-match-password-hmac-key"))
    admission_hmac_key: SecretStr = Field(default=SecretStr("perfcho-development-admission-hmac-key"))

    redis_state_url: str = Field(default="redis://127.0.0.1:56379/0")
    redis_state_prefix: str = Field(default="perfcho:state")
    redis_socket_timeout: float = Field(default=5.0, gt=0)
    redis_session_ttl_seconds: int = Field(default=360, ge=60)
    redis_presence_ttl_seconds: int = Field(default=360, ge=60)
    redis_mailbox_ttl_seconds: int = Field(default=600, ge=60)
    redis_mailbox_max_packets: int = Field(default=4096, ge=128)
    redis_mailbox_max_bytes: int = Field(default=16 * 1024 * 1024, ge=1024)
    redis_realtime_max_channels_per_session: int = Field(default=256, ge=1, le=4096)
    redis_spectator_max_frames: int = Field(default=4096, ge=1)
    redis_spectator_max_bytes: int = Field(default=16 * 1024 * 1024, ge=1024)
    redis_spectator_max_viewers: int = Field(default=4096, ge=1, le=32768)
    redis_multiplayer_ttl_seconds: int = Field(default=12 * 60 * 60, ge=300)
    redis_multiplayer_max_rooms: int = Field(default=4096, ge=1, le=32767)

    stable_build: str = Field(default="b20260711.1", min_length=1, max_length=64)
    stable_protocol_version: int = Field(default=19, ge=1)
    stable_max_body_bytes: int = Field(default=1024 * 1024, ge=1024, le=16 * 1024 * 1024)
    stable_max_response_bytes: int = Field(default=1024 * 1024, ge=7, le=16 * 1024 * 1024)
    stable_session_lifetime_seconds: int = Field(default=12 * 60 * 60, ge=60)
    stable_session_stale_grace_seconds: int = Field(default=120, ge=30, le=30 * 60)
    stable_mailbox_batch_size: int = Field(default=256, ge=1, le=4096)
    stable_mailbox_lease_seconds: int = Field(default=10, ge=1, le=60)
    stable_welcome_notification: str = Field(default="Welcome to perfcho.py.", max_length=1024)
    stable_beatmap_download_base_url: str = Field(
        default="https://osu.ppy.sh/beatmapsets",
        min_length=1,
        max_length=512,
    )
    stable_beatmap_file_base_url: str = Field(
        default="https://osu.ppy.sh/web/maps",
        min_length=1,
        max_length=512,
    )
    stable_score_submission_max_bytes: int = Field(default=20 * 1024 * 1024, ge=1024, le=64 * 1024 * 1024)
    stable_replay_max_bytes: int = Field(default=16 * 1024 * 1024, ge=0, le=64 * 1024 * 1024)
    stable_spectator_frame_batch_size: int = Field(default=256, ge=1, le=4096)
    stable_lobby_match_limit: int = Field(default=100, ge=1, le=256)
    stable_presence_batch_size: int = Field(default=2048, ge=1, le=8192)

    s3_endpoint_url: str = Field(default="http://127.0.0.1:59000")
    s3_presign_endpoint_url: str | None = Field(default=None)
    s3_region: str = Field(default="us-east-1")
    s3_access_key: SecretStr = Field(default=SecretStr("perfcho"))
    s3_secret_key: SecretStr = Field(default=SecretStr("perfcho-development"))
    s3_bucket: str = Field(default="perfcho")
    s3_addressing_style: Literal["path", "virtual"] = Field(default="path")
    object_stream_chunk_size: int = Field(default=1024 * 1024, ge=64 * 1024, le=16 * 1024 * 1024)

    osu_api_base_url: str = Field(default="https://osu.ppy.sh/api/v2", min_length=1, max_length=512)
    osu_oauth_token_url: str = Field(default="https://osu.ppy.sh/oauth/token", min_length=1, max_length=512)
    osu_api_client_id: int = Field(default=0, ge=0)
    osu_api_client_secret: SecretStr = Field(default=SecretStr(""))
    upstream_beatmap_file_base_url: str = Field(default="https://osu.ppy.sh/osu", min_length=1, max_length=512)
    upstream_beatmap_file_max_bytes: int = Field(default=16 * 1024 * 1024, ge=1024, le=64 * 1024 * 1024)

    taskiq_broker_url: str = Field(default="redis://127.0.0.1:56379/1")
    taskiq_queue_name: str = Field(default="perfcho:tasks")
    taskiq_consumer_group: str = Field(default="perfcho:workers")
    taskiq_stream_max_length: int = Field(default=100_000, ge=1000)

    durable_relay_poll_interval_seconds: float = Field(default=1.0, gt=0)
    outbox_delivery_batch_size: int = Field(default=100, ge=1, le=1000)
    outbox_delivery_lease_seconds: int = Field(default=300, ge=30)
    outbox_delivery_max_attempts: int = Field(default=10, ge=1)
    outbox_delivery_max_retry_seconds: int = Field(default=300, ge=1)

    performance_calculator_urls: dict[str, str] = Field(default_factory=dict)
    performance_http_timeout_seconds: float = Field(default=30.0, gt=0)
    performance_beatmap_url_expiry_seconds: int = Field(default=600, ge=30, le=3600)
    performance_calculation_batch_size: int = Field(default=32, ge=1, le=1000)
    performance_calculation_lease_seconds: int = Field(default=300, ge=30)
    performance_calculation_max_attempts: int = Field(default=5, ge=1)
    performance_calculation_max_retry_seconds: int = Field(default=300, ge=1)

    cors_origins: list[str] = Field(default=["http://localhost:3000", "http://localhost:5173"])

    @field_validator("trusted_proxy_cidrs")
    @classmethod
    def validate_trusted_proxy_cidrs(cls, values: list[str]) -> list[str]:
        """Require canonical strict IPv4 or IPv6 proxy networks."""
        normalized: list[str] = []
        for value in values:
            try:
                normalized.append(str(ip_network(value, strict=True)))
            except ValueError as error:
                raise ValueError(f"invalid trusted proxy CIDR: {value}") from error
        return normalized

    @model_validator(mode="after")
    def validate_performance_timing(self) -> Self:
        """Keep execution leases and signed URLs beyond the HTTP timeout window."""
        minimum_window = ceil(self.performance_http_timeout_seconds) + 30
        if self.performance_calculation_lease_seconds < minimum_window:
            raise ValueError("performance calculation lease must exceed the HTTP timeout by at least 30 seconds")
        if self.performance_beatmap_url_expiry_seconds < minimum_window:
            raise ValueError("performance Beatmap URL expiry must exceed the HTTP timeout by at least 30 seconds")
        return self


settings = Settings()
