from sqlalchemy import Table, Column, String, Integer, Enum, ForeignKey, DateTime
from sqlalchemy.dialects.postgresql import UUID
from datetime import datetime
import uuid
import enum
from market.db import metadata


class UserRole(str, enum.Enum):
    USER = "USER"
    ADMIN = "ADMIN"


class Direction(str, enum.Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderStatus(str, enum.Enum):
    NEW = "NEW"
    EXECUTED = "EXECUTED"
    PARTIALLY_EXECUTED = "PARTIALLY_EXECUTED"
    CANCELLED = "CANCELLED"


users = Table(
    "users", metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
    Column("name", String, nullable=False),
    Column("role", Enum(UserRole), nullable=False, default=UserRole.USER),
    Column("api_key", String, unique=True, nullable=False),
)

instruments = Table(
    "instruments", metadata,
    Column("ticker", String, primary_key=True),
    Column("name", String, nullable=False),
)

balances = Table(
    "balances", metadata,
    Column("user_id", UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), primary_key=True),
    Column("ticker", String, ForeignKey("instruments.ticker", ondelete="CASCADE"), primary_key=True),
    Column("amount", Integer, nullable=False, default=0),
)

orders = Table(
    "orders", metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
    Column("user_id", UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
    Column("ticker", String, ForeignKey("instruments.ticker", ondelete="CASCADE"), nullable=False),
    Column("direction", Enum(Direction), nullable=False),
    Column("qty", Integer, nullable=False),
    Column("price", Integer),  # NULL = рыночная заявка
    Column("status", Enum(OrderStatus), nullable=False, default=OrderStatus.NEW),
    Column("filled", Integer, nullable=False, default=0),
    Column("timestamp", DateTime, nullable=False, default=datetime.utcnow),
)

transactions = Table(
    "transactions", metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
    Column("ticker", String, ForeignKey("instruments.ticker", ondelete="CASCADE"), nullable=False),
    Column("amount", Integer, nullable=False),
    Column("price", Integer, nullable=False),
    Column("timestamp", DateTime, nullable=False, default=datetime.utcnow),
)
