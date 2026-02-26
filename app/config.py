from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_name: str = "ETF Filing Detection Engine"
    host: str = "0.0.0.0"
    port: int = 8000
    auto_start_stream: bool = False

    pds_host: str = "127.0.0.1"
    pds_port: int = 9000
    pds_chunk_size: int = 65536
    pds_reconnect_seconds: int = 5

    sec_user_agent: str = "ETF-Filings-Monitor/1.0 (reporter@example.com)"
    request_timeout_seconds: int = 30

    gemini_api_key: str = ""
    gemini_model: str = "gemini-1.5-pro"

    smtp_host: str = ""
    smtp_port: int = 587
    smtp_username: str = ""
    smtp_password: str = ""
    smtp_use_tls: bool = True
    from_email: str = "alerts@localhost"

    resend_api_key: str = ""
    resend_from_email: str = ""

    reporter_email: str = "reporter@example.com"
    alerts_retention: int = 200
    pdf_output_dir: Path = Path("generated_pdfs")

    crypto_keywords: list[str] = Field(
        default_factory=lambda: [
            "Bitcoin",
            "Ethereum",
            "Digital Asset",
            "Spot",
            "Coinbase Custody",
        ]
    )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
