from pydantic import BaseModel
from typing import Optional, List

class TradeCard(BaseModel):
    """
    Pydantic model for the data expected when an advisor creates a trade card.
    """
    stock_symbol: str
    stop_loss: float
    target: float
    trade_direction: str
    trade_category: str
    trade_segment: str
    entry_zone: Optional[str] = None
    exit_zone: Optional[str] = None


class LivePriceRequest(BaseModel):
    """
    Pydantic model for the live price request from the Flutter app.
    """
    symbols: List[str]

class SubscriptionRequest(BaseModel):
    symbol: str
    action: str = "add"

class BulkSubscribeRequest(BaseModel):
    symbols: List[str]
    action: str