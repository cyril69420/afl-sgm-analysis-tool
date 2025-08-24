"""
Pydantic models for Bronze layer rows.

These models define the expected shape of the raw data written by
ingestion scripts. They serve as data contracts between the scraping
logic and the downstream processing stages. Using Pydantic v2
validated models ensures that each field is present and correctly
typed before the record is persisted to disk.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional, Union

from pydantic import BaseModel, Field, field_validator


class BronzeEventUrl(BaseModel):
    bookmaker: str = Field(..., description="Name of the bookmaker (e.g. sportsbet)")
    season: int = Field(..., description="AFL season (e.g. 2025)")
    round: Union[int, str] = Field(..., description="Round identifier or special value like 'finals'")
    event_url: str = Field(..., description="Fully qualified URL of the event page")
    competition: str = Field(..., description="Competition code (e.g. afl)")
    first_seen_utc: datetime = Field(..., description="UTC timestamp when the URL was first discovered")
    last_seen_utc: datetime = Field(..., description="UTC timestamp when the URL was most recently seen")
    source_page: str = Field(..., description="Landing page from which the URL was discovered")
    status: str = Field(..., description="Status of the event (upcoming or past)")
    discovery_run_id: str = Field(..., description="Unique identifier for this discovery run")

    @field_validator('event_url')
    @classmethod
    def url_must_be_non_empty(cls, v: str) -> str:
        if not v:
            raise ValueError("event_url must be non-empty")
        return v


class BronzeFixtureRow(BaseModel):
    game_key: str = Field(..., description="Deterministic slug for the game")
    season: int
    round: Union[int, str]
    home: str
    away: str
    venue: str
    scheduled_time_utc: datetime
    source_tz: str
    discovered_utc: datetime
    bookmaker_event_url: Optional[str] = Field(default=None, description="Source URL from bookmaker if available")


class BronzeOddsSnapshotRow(BaseModel):
    captured_at_utc: datetime
    bookmaker: str
    event_url: str
    market_group: str
    market_name: str
    selection: str
    line: Optional[float] = Field(default=None)
    decimal_odds: float
    raw_payload: Any
    hash_key: str

    @field_validator('decimal_odds')
    @classmethod
    def odds_must_be_positive(cls, v: float) -> float:
        if v <= 1.0:
            raise ValueError("decimal_odds must be greater than 1.0")
        return v


class BronzeOddsHistoryRow(BaseModel):
    captured_at_utc: datetime
    bookmaker: str
    event_url: str
    market_group: str
    market_name: str
    selection: str
    line: Optional[float] = Field(default=None)
    decimal_odds: float
    raw_payload: Any
    source_kind: str
    hash_key: str

    @field_validator('source_kind')
    @classmethod
    def valid_source_kind(cls, v: str) -> str:
        if v != 'historical':
            raise ValueError("source_kind must be 'historical'")
        return v


class BronzeWeatherRow(BaseModel):
    provider: str
    venue: str
    lat: float
    lon: float
    run_time_utc: datetime
    valid_time_utc: datetime
    lead_time_hr: int
    temp_c: Optional[float]
    wind_speed_ms: Optional[float]
    wind_gust_ms: Optional[float]
    rain_prob: Optional[float]
    pressure_hpa: Optional[float]
    cloud_pct: Optional[float]
    raw_payload: Any

    @field_validator('rain_prob')
    @classmethod
    def rain_prob_range(cls, v: Optional[float]) -> Optional[float]:
        if v is not None and not (0.0 <= v <= 1.0):
            raise ValueError('rain_prob must be between 0 and 1')
        return v