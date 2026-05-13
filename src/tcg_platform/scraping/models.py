from pydantic import BaseModel
from typing import Optional
from datetime import datetime


class CardRecord(BaseModel):
    card_id: str
    card_version: Optional[str] = None
    card_name: str
    set_code: str
    rarity: str
    card_type: str
    attribute: Optional[str] = None
    power: Optional[int] = None
    cost: Optional[int] = None
    color: Optional[str] = None
    source_url: str
    scraped_at: datetime


class PriceRecord(BaseModel):
    card_id: str
    card_version: Optional[str] = None
    event_type: str
    price: float
    currency: str
    sold_date: Optional[str] = None
    scraped_from: str
    source: str
    source_url: str
    scraped_at: datetime


class ImageRecord(BaseModel):
    card_id: str
    card_version: Optional[str] = None
    object_key: str
    size_bytes: Optional[int] = None
    mimetype: str = "image/jpeg"
    source_url: str
    scraped_at: datetime