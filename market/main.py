from fastapi import FastAPI, HTTPException, Header, Path, Query, Body, status
from pydantic import BaseModel, Field, constr, StringConstraints
from uuid import uuid4, UUID
from enum import Enum
from typing import Optional, List, Dict, Union
from datetime import datetime
from market.models import users, instruments, balances, orders, transactions, UserRole, Direction, OrderStatus
from market.db import database
from contextlib import asynccontextmanager
from typing import Annotated
from sqlalchemy import and_, select


async def match_orders(order_id: UUID):
    query = orders.select().where(orders.c.id == order_id)
    order = await database.fetch_one(query)
    if not order or order["status"] != OrderStatus.NEW:
        return

    is_buy = order["direction"] == Direction.BUY
    opposite_dir = Direction.SELL if is_buy else Direction.BUY

    cond = [
        orders.c.ticker == order["ticker"],
        orders.c.status == OrderStatus.NEW,
        orders.c.direction == opposite_dir,
    ]

    if order["price"] is not None:
        cond.append(
            orders.c.price <= order["price"] if is_buy else orders.c.price >= order["price"]
        )

    candidates_query = orders.select().where(and_(*cond)).order_by(
        orders.c.price.asc() if is_buy else orders.c.price.desc(),
        orders.c.timestamp.asc()
    )
    candidates = await database.fetch_all(candidates_query)

    qty_to_fill = order["qty"] - order["filled"]
    for counter_order in candidates:
        counter_qty = counter_order["qty"] - counter_order["filled"]
        deal_qty = min(qty_to_fill, counter_qty)
        if deal_qty <= 0:
            continue

        deal_price = counter_order["price"] if counter_order["price"] is not None else order["price"]
        if deal_price is None:
            continue  # оба рыночные — нельзя обработать

        # Обновление балансов
        buyer_id = order["user_id"] if is_buy else counter_order["user_id"]
        seller_id = counter_order["user_id"] if is_buy else order["user_id"]
        total = deal_price * deal_qty

        await database.execute(
            balances.update().where(and_(balances.c.user_id == buyer_id, balances.c.ticker == order["RUB"]))
            .values(amount=balances.c.amount + deal_qty).prefix_with("ON CONFLICT DO NOTHING")
        )
        await database.execute(
            balances.update().where(and_(balances.c.user_id == seller_id, balances.c.ticker == order["RUB"]))
            .values(amount=balances.c.amount - deal_qty)
        )

        # Обновление заявок
        await database.execute(
            orders.update().where(orders.c.id == order["id"])
            .values(filled=order["filled"] + deal_qty)
        )
        await database.execute(
            orders.update().where(orders.c.id == counter_order["id"])
            .values(filled=counter_order["filled"] + deal_qty)
        )

        # Запись сделки
        await database.execute(
            transactions.insert().values(
                id=uuid4(),
                ticker=order["ticker"],
                amount=deal_qty,
                price=deal_price,
                timestamp=datetime.utcnow()
            )
        )

        qty_to_fill -= deal_qty
        if qty_to_fill <= 0:
            break

    # Статусы
    new_order = await database.fetch_one(orders.select().where(orders.c.id == order["id"]))
    if new_order["filled"] == new_order["qty"]:
        await database.execute(
            orders.update().where(orders.c.id == order["id"]).values(status=OrderStatus.EXECUTED)
        )
    elif new_order["filled"] > 0:
        await database.execute(
            orders.update().where(orders.c.id == order["id"]).values(status=OrderStatus.PARTIALLY_EXECUTED)
        )


@asynccontextmanager
async def lifespan(app: FastAPI):
    await database.connect()
    yield
    await database.disconnect()

    
app = FastAPI(title="Toy exchange", version="0.1.0", lifespan=lifespan)


class NewUser(BaseModel):
    name: Annotated[str, constr(min_length=3)]


class User(BaseModel):
    id: UUID
    name: str
    role: UserRole = UserRole.USER
    api_key: str


class Instrument(BaseModel):
    name: str
    ticker: Annotated[str, StringConstraints(pattern="^[A-Z]{2,10}$")]


class Level(BaseModel):
    price: int
    qty: int


class L2OrderBook(BaseModel):
    bid_levels: List[Level]
    ask_levels: List[Level]


class Transaction(BaseModel):
    ticker: str
    amount: int
    price: int
    timestamp: datetime


class MarketOrderBody(BaseModel):
    direction: Direction
    ticker: str
    qty: int = Field(..., ge=1)


class LimitOrderBody(MarketOrderBody):
    price: int = Field(..., gt=0)


class CreateOrderResponse(BaseModel):
    success: bool = True
    order_id: UUID


class Ok(BaseModel):
    success: bool = True


class DepositWithdrawBody(BaseModel):
    user_id: UUID
    ticker: str
    amount: int = Field(..., gt=0)


async def get_user_id(token: str) -> UUID:
    key = token.replace("TOKEN ", "")
    query = users.select().where(users.c.api_key == key)
    user = await database.fetch_one(query)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid or missing token")
    return user["id"]


async def get_admin_id(token: str) -> UUID:
    key = token.replace("TOKEN ", "")
    query = users.select().where(users.c.api_key == key)
    user = await database.fetch_one(query)
    if not user or user["role"] != UserRole.ADMIN:
        raise HTTPException(status_code=403, detail="Admin access required")
    return user["id"]

# Public

@app.post("/api/v1/public/register", response_model=User)
async def register(user: NewUser):
    user_id = uuid4()
    api_key = f"key-{uuid4()}"
    query = users.insert().values(
        id=user_id,
        name=user.name,
        role=UserRole.USER,
        api_key=api_key
    )
    await database.execute(query)
    return User(id=user_id, name=user.name, role=UserRole.USER, api_key=api_key)


@app.get("/api/v1/public/instrument", response_model=List[Instrument])
async def list_instruments():
    query = instruments.select()
    rows = await database.fetch_all(query)
    return [Instrument(**row) for row in rows]


@app.get("/api/v1/public/orderbook/{ticker}", response_model=L2OrderBook)
async def get_orderbook(ticker: str, limit: int = Query(10, le=25)):
    bid_query = select([orders.c.price, orders.c.qty]).where(
        and_(orders.c.ticker == ticker, orders.c.status == OrderStatus.NEW, orders.c.direction == Direction.BUY)
    ).order_by(orders.c.price.desc()).limit(limit)

    ask_query = select([orders.c.price, orders.c.qty]).where(
        and_(orders.c.ticker == ticker, orders.c.status == OrderStatus.NEW, orders.c.direction == Direction.SELL)
    ).order_by(orders.c.price.asc()).limit(limit)

    bid_levels = await database.fetch_all(bid_query)
    ask_levels = await database.fetch_all(ask_query)

    return L2OrderBook(
        bid_levels=[Level(price=row["price"], qty=row["qty"]) for row in bid_levels],
        ask_levels=[Level(price=row["price"], qty=row["qty"]) for row in ask_levels],
    )


@app.get("/api/v1/public/transactions/{ticker}", response_model=List[Transaction])
async def get_transactions(ticker: str, limit: int = Query(10, le=100)):
    query = transactions.select().where(transactions.c.ticker == ticker).order_by(transactions.c.timestamp.desc()).limit(limit)
    rows = await database.fetch_all(query)
    return [Transaction(**row) for row in rows]

# Balance

@app.get("/api/v1/balance")
async def get_balance(authorization: Optional[str] = Header(None)):
    if authorization is None:
        raise HTTPException(status_code=401, detail="Authorization header missing")
    user_id = await get_user_id(authorization)
    query = balances.select().where(balances.c.user_id == user_id)
    rows = await database.fetch_all(query)
    return {row["ticker"]: row["amount"] for row in rows}

# Orders 

@app.post("/api/v1/order", response_model=CreateOrderResponse)
async def create_order(order: Union[LimitOrderBody, MarketOrderBody], authorization: Optional[str] = Header(None)):
    user_id = await get_user_id(authorization)
    order_id = uuid4()
    query = orders.insert().values(
        id=order_id,
        user_id=user_id,
        ticker=order.ticker,
        direction=order.direction.value,
        qty=order.qty,
        price=getattr(order, "price", None),
        status=OrderStatus.NEW.value,
        filled=0,
        timestamp=datetime.utcnow()
    )
    await database.execute(query)
    await match_orders(order_id)
    return CreateOrderResponse(order_id=order_id)



@app.get("/api/v1/order")
async def list_orders(authorization: Optional[str] = Header(None)):
    user_id = await get_user_id(authorization)
    query = orders.select().where(orders.c.user_id == user_id)
    rows = await database.fetch_all(query)

    result = []
    for order in rows:
        body = {
            "direction": order["direction"],
            "ticker": order["ticker"],
            "qty": order["qty"]
        }
        if order["price"] is not None:
            body["price"] = order["price"]

        order_data = {
            "id": order["id"],
            "status": order["status"],
            "user_id": order["user_id"],
            "timestamp": order["timestamp"],
            "filled": order["filled"],
            "body": body
        }
        result.append(order_data)

    return result



@app.get("/api/v1/order/{order_id}")
async def get_order(order_id: UUID, authorization: Optional[str] = Header(None)):
    user_id = await get_user_id(authorization)
    query = orders.select().where(orders.c.id == order_id)
    order = await database.fetch_one(query)
    if not order or order["user_id"] != user_id:
        raise HTTPException(status_code=404, detail="Order not found")

    body = {
        "direction": order["direction"],
        "ticker": order["ticker"],
        "qty": order["qty"]
    }
    if order["price"] is not None:
        body["price"] = order["price"]

    response = {
        "id": order["id"],
        "status": order["status"],
        "user_id": order["user_id"],
        "timestamp": order["timestamp"],
        "filled": order["filled"],
        "body": body
    }
    return response



@app.delete("/api/v1/order/{order_id}", response_model=Ok)
async def cancel_order(order_id: UUID, authorization: Optional[str] = Header(None)):
    user_id = await get_user_id(authorization)
    query = orders.select().where(orders.c.id == order_id)
    order = await database.fetch_one(query)
    if not order or order["user_id"] != user_id:
        raise HTTPException(status_code=404, detail="Order not found")
    update_query = orders.update().where(orders.c.id == order_id).values(status=OrderStatus.CANCELLED.value)
    await database.execute(update_query)
    return Ok()

# Admin

@app.post("/api/v1/admin/balance/deposit", response_model=Ok)
async def deposit(body: DepositWithdrawBody, authorization: Optional[str] = Header(None)):
    await get_admin_id(authorization)
    query = balances.select().where((balances.c.user_id == body.user_id) & (balances.c.ticker == body.ticker))
    row = await database.fetch_one(query)
    if row:
        update_query = balances.update().where((balances.c.user_id == body.user_id) & (balances.c.ticker == body.ticker)).values(amount=row["amount"] + body.amount)
        await database.execute(update_query)
    else:
        insert_query = balances.insert().values(user_id=body.user_id, ticker=body.ticker, amount=body.amount)
        await database.execute(insert_query)
    return Ok()


@app.post("/api/v1/admin/balance/withdraw", response_model=Ok)
async def withdraw(body: DepositWithdrawBody, authorization: Optional[str] = Header(None)):
    await get_admin_id(authorization)
    query = balances.select().where((balances.c.user_id == body.user_id) & (balances.c.ticker == body.ticker))
    row = await database.fetch_one(query)
    if not row or row["amount"] < body.amount:
        raise HTTPException(status_code=400, detail="Insufficient funds")
    update_query = balances.update().where((balances.c.user_id == body.user_id) & (balances.c.ticker == body.ticker)).values(amount=row["amount"] - body.amount)
    await database.execute(update_query)
    return Ok()


@app.post("/api/v1/admin/instrument", response_model=Ok)
async def add_instrument(instr: Instrument, authorization: Optional[str] = Header(None)):
    await get_admin_id(authorization)
    query = instruments.insert().values(ticker=instr.ticker, name=instr.name)
    await database.execute(query)
    return Ok()


@app.delete("/api/v1/admin/instrument/{ticker}", response_model=Ok)
async def delete_instrument(ticker: str, authorization: Optional[str] = Header(None)):
    await get_admin_id(authorization)
    query = instruments.delete().where(instruments.c.ticker == ticker)
    await database.execute(query)
    return Ok()


@app.delete("/api/v1/admin/user/{user_id}", response_model=User)
async def delete_user(user_id: UUID, authorization: Optional[str] = Header(None)):
    await get_admin_id(authorization)
    query = users.select().where(users.c.id == user_id)
    user = await database.fetch_one(query)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    await database.execute(balances.delete().where(balances.c.user_id == user_id))
    await database.execute(users.delete().where(users.c.id == user_id))
    return User(**user)

