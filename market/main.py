from fastapi import FastAPI, HTTPException, Header, Query, status, Request
from pydantic import BaseModel, Field, constr, StringConstraints
from uuid import uuid4, UUID
from typing import Optional, List, Union, Annotated
from datetime import datetime, timezone
from models import users, instruments, balances, orders, transactions, UserRole, Direction, OrderStatus
from db import database
from contextlib import asynccontextmanager
from sqlalchemy import and_, select, or_, nullsfirst, func
import asyncio
from fastapi.exceptions import RequestValidationError
import logging
from fastapi.responses import JSONResponse
from sqlalchemy.dialects.postgresql import insert


class ValidationError(BaseModel):
    loc: List[Union[str, int]]
    msg: str
    type: str

class HTTPValidationError(BaseModel):
    detail: List[ValidationError]


semaphore = asyncio.Semaphore(1)
async def match_orders(order_id: UUID):
    async with database.transaction():  # Все операции в одной транзакции
        # 1. Получение текущего ордера
        order = await database.fetch_one(
            orders.select().where(orders.c.id == order_id)
        )
        if not order:
            raise HTTPException(status_code=422, detail="Order not found")

        if order["status"] == OrderStatus.EXECUTED:
            return

        if order["status"] == OrderStatus.CANCELLED:
            return

        is_buy = order["direction"] == Direction.BUY.value
        opposite_dir = Direction.SELL if is_buy else Direction.BUY


        # 2. Формирование условий для поиска встречных ордеров
        cond = [
            orders.c.ticker == order["ticker"],
            or_(
                orders.c.status == OrderStatus.NEW,
                orders.c.status == OrderStatus.PARTIALLY_EXECUTED
            ),
            orders.c.direction == opposite_dir.value,
        ]

        if order["price"] is not None:
            price_condition = (
                orders.c.price <= order["price"] if is_buy
                else orders.c.price >= order["price"]
            )
            cond.append(or_(
                    orders.c.price.is_(None),
                    price_condition
                ))

        # 3. Сортировка по приоритету цены и времени
        # Для BUY: сначала ордера с меньшей ценой (лучшая цена для покупки)
        # Для SELL: сначала ордера с большей ценой (лучшая цена для продажи)
        order_by = [
            nullsfirst(orders.c.price.asc()) if is_buy else nullsfirst(orders.c.price.desc()),
            orders.c.timestamp.asc()
        ]

        candidates_query = orders.select().where(and_(*cond)).order_by(*order_by).with_for_update(skip_locked=True)
        candidates = await database.fetch_all(candidates_query)
        qty_to_fill = order["qty"] - order["filled"]

        counter_quantity = sum([counter_order["qty"] - counter_order["filled"] for counter_order in candidates])
        if order["price"] is None and counter_quantity < qty_to_fill:
            raise HTTPException(
                status_code=422,
                detail=[
                    {
                        "loc": ["body", "order"],
                        "msg": "Insufficient liquidity to execute Market Sell",
                        "type": "value_error.insufficient_liquidity",
                    }
                ],
            )


        # 4. Процесс исполнения встречных ордеров
        for counter_order in candidates:
            if qty_to_fill <= 0:
                break
            counter_qty = counter_order["qty"] - counter_order["filled"]
            deal_qty = min(qty_to_fill, counter_qty)
            if deal_qty <= 0:
                continue
            # Цена исполнения - берем цену встречного ордера (price-taking)
            deal_price = counter_order["price"] if counter_order["price"] is not None else order["price"]
            if deal_price is None:
                continue

            # 5. Проверка и обновление балансов
            buyer_id = order["user_id"] if is_buy else counter_order["user_id"]
            seller_id = counter_order["user_id"] if is_buy else order["user_id"]
            # Проверка баланса продавца и покупателя
            seller_balance = await database.fetch_one(
                select(balances.c.amount).where(and_(
                    balances.c.user_id == seller_id,
                    balances.c.ticker == order["ticker"]
                ))
            )

            buyer_balance = await database.fetch_one(
                select(balances.c.amount).where(and_(
                    balances.c.user_id == buyer_id,
                    balances.c.ticker == "RUB"
                ))
            )

            if not seller_balance or seller_balance["amount"] < deal_qty:
                continue

            if not buyer_balance or buyer_balance["amount"] < deal_qty * deal_price:
                continue
            # Обновление баланса покупателя
            await database.execute(
                insert(balances)
                .values(
                    user_id=buyer_id,
                    ticker=order["ticker"],
                    amount=deal_qty
                )
                .on_conflict_do_update(
                    index_elements=['user_id', 'ticker'],
                    set_={'amount': balances.c.amount + deal_qty}
                )
            )

            await database.execute(
                balances.update()
                .where(and_(
                    balances.c.user_id == buyer_id,
                    balances.c.ticker == "RUB"
                ))
                .values(amount=balances.c.amount - deal_price * deal_qty)
            )
            # Обновление баланса продавца   
            await database.execute(
                balances.update()
                .where(and_(
                    balances.c.user_id == seller_id,
                    balances.c.ticker == order["ticker"]
                ))
                .values(amount=balances.c.amount - deal_qty)
            )

            await database.execute(
                balances.update()
                .where(and_(
                    balances.c.user_id == seller_id,
                    balances.c.ticker == "RUB"
                ))
                .values(amount=balances.c.amount + deal_price * deal_qty)
            )

            # 6. Обновление статусов ордеров
            first_id, second_id = sorted([order["id"], counter_order["id"]])
            # Явно блокируем строки перед UPDATE
            await database.fetch_one(
                orders.select().where(orders.c.id == first_id).with_for_update()
            )
            await database.fetch_one(
                orders.select().where(orders.c.id == second_id).with_for_update()
            )


            # После этого — обновляем заполненность ордеров
            order = await database.fetch_one(
            orders.select().where(orders.c.id == order["id"]).with_for_update()
            )
            await database.execute(
                orders.update()
                .where(orders.c.id == order["id"])
                .values(filled=order["filled"] + deal_qty)
            )

            await database.execute(
                orders.update()
                .where(orders.c.id == counter_order["id"])
                .values(filled=counter_order["filled"] + deal_qty)
            )

            # 7. Логирование сделки
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
            ## 8. Финализация статуса основного ордера
            updated_order = await database.fetch_one(
            orders.select().where(orders.c.id == order["id"]).with_for_update()
            )

            new_status = (
                OrderStatus.EXECUTED if updated_order["filled"] >= updated_order["qty"] else
                OrderStatus.PARTIALLY_EXECUTED if updated_order["filled"] > 0 else
                updated_order["status"]
            )

            await database.execute(
                orders.update()
                .where(orders.c.id == order["id"])
                .values(status=new_status)
            )

            # 9. Финализация статуса противоположного ордера
            updated_counter_order = await database.fetch_one(
                orders.select().where(orders.c.id == counter_order["id"]).with_for_update()
            )

            # Определяем новый статус для противоположного ордера
            new_counter_status = (
                OrderStatus.EXECUTED if updated_counter_order["filled"] >= updated_counter_order["qty"] else
                OrderStatus.PARTIALLY_EXECUTED if updated_counter_order["filled"] > 0 else
                updated_counter_order["status"]
            )

            # Обновляем статус противоположного ордера
            await database.execute(
                orders.update()
                .where(orders.c.id == counter_order["id"])
                .values(status=new_counter_status)
            )
        order = await database.fetch_one(
            orders.select().where(orders.c.id == order_id)
        )
        if order["price"] is None:
            if order["filled"] == order["qty"]:
                update_query = orders.update().where(orders.c.id == order_id).values(status=OrderStatus.EXECUTED.value)
            else:
                update_query = orders.update().where(orders.c.id == order_id).values(status=OrderStatus.CANCELLED.value)
            await database.execute(update_query)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await database.connect()
    yield
    await database.disconnect()


app = FastAPI(title="Exchange", version="0.1.0", lifespan=lifespan)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    logging.warning(f"Validation error: {exc.errors()}")
    return JSONResponse(
        status_code=422,
        content={
            "detail": exc.errors()
        }
    )

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


class MarketOrder(BaseModel):
    id: UUID
    status: OrderStatus
    user_id: UUID
    timestamp: datetime
    body: MarketOrderBody


class LimitOrder(BaseModel):
    id: UUID
    status: OrderStatus
    user_id: UUID
    timestamp: datetime
    body: LimitOrderBody
    filled: int


class CreateOrderResponse(BaseModel):
    success: bool = True
    order_id: UUID


class Ok(BaseModel):
    success: bool = True


class DepositWithdrawBody(BaseModel):
    user_id: UUID
    ticker: str
    amount: int = Field(..., ge=0)


async def get_user_id(token: str) -> UUID:
    key = token.replace("TOKEN ", "")
    query = users.select().where(users.c.api_key == key)
    user = await database.fetch_one(query)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid or missing token")
    return user["id"]

async def get_admin_id(token: str) -> UUID:
    token = str(token)
    key = token.replace("TOKEN ", "")
    query = users.select().where(users.c.api_key == key)
    user = await database.fetch_one(query)
    if not user or user["role"] != UserRole.ADMIN:
        raise HTTPException(status_code=403, detail=f"Admin access required {token}")
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
    query = balances.insert().values(
        user_id=user_id,
        ticker="RUB",
        amount=0
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
    # Провераем, что тикер существуют в instruments
    ticker_exists = await database.fetch_val(
        select(instruments.c.ticker)
        .where(instruments.c.ticker == ticker)
        .limit(1)
    )

    if not ticker_exists:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Ticker not found"
        )

    valid_statuses = [OrderStatus.NEW.value, OrderStatus.PARTIALLY_EXECUTED.value]

    bid_query = (
        select(orders.c.price, func.sum(orders.c.qty - orders.c.filled).label("total_qty"))
        .where(
            and_(
                orders.c.ticker == ticker,
                orders.c.status.in_(valid_statuses),
                orders.c.direction == Direction.BUY.value,
                orders.c.price.isnot(None)
            )
        )
        .group_by(orders.c.price)
        .having(func.sum(orders.c.qty - orders.c.filled) > 0)
        .order_by(orders.c.price.desc())  # топ с самой высокой ценой
        .limit(limit)
    )

    ask_query = (
        select(orders.c.price, func.sum(orders.c.qty - orders.c.filled).label("total_qty"))
        .where(
            and_(
                orders.c.ticker == ticker,
                orders.c.status.in_(valid_statuses),
                orders.c.direction == Direction.SELL.value,
                orders.c.price.isnot(None)
            )
        )
        .group_by(orders.c.price)
        .having(func.sum(orders.c.qty - orders.c.filled) > 0)
        .order_by(orders.c.price.asc())  # топ с самой низкой ценой
        .limit(limit)
    )

    bid_levels = await database.fetch_all(bid_query)
    ask_levels = await database.fetch_all(ask_query)

    return L2OrderBook(
        bid_levels=[
            Level(price=row.price, qty=int(row.total_qty)) for row in bid_levels
        ],
        ask_levels=[
            Level(price=row.price, qty=int(row.total_qty)) for row in ask_levels
        ],
    )




@app.get("/api/v1/public/transactions/{ticker}", response_model=List[Transaction])
async def get_transactions(ticker: str, limit: int = Query(10, le=100)):
    query = transactions.select().where(transactions.c.ticker == ticker) \
        .order_by(transactions.c.timestamp.desc()).limit(limit)
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

@app.post("/api/v1/order", response_model=CreateOrderResponse,
    responses={
        422: {"model": HTTPValidationError}
    }
)
async def create_order(order: Union[LimitOrderBody, MarketOrderBody], authorization: Optional[str] = Header(None)):
    user_id = await get_user_id(authorization)
    order_id = uuid4()
    balance = await database.fetch_one(
        select(balances.c.amount).where(and_(
            balances.c.user_id == user_id,
            balances.c.ticker == ("RUB" if order.direction == Direction.BUY else order.ticker)
        ))
    )
    available = balance["amount"] if balance else 0
    

    if order.direction == Direction.BUY:
        reserved_query = select(
            func.sum(orders.c.qty * orders.c.price)
        ).where(and_(
            orders.c.user_id == user_id,
            orders.c.direction == order.direction.value,
            or_(
                orders.c.status == OrderStatus.NEW.value,
                orders.c.status == OrderStatus.PARTIALLY_EXECUTED.value
            ),
            orders.c.ticker == order.ticker
        ))
        new_required = order.qty * getattr(order, "price", 0)
    else:
        reserved_query = select(
            func.sum(orders.c.qty)
        ).where(and_(
            orders.c.user_id == user_id,
            orders.c.direction == order.direction.value,
            or_(
                orders.c.status == OrderStatus.NEW.value,
                orders.c.status == OrderStatus.PARTIALLY_EXECUTED.value
            ),
            orders.c.ticker == order.ticker
        ))
        new_required = order.qty

    reserved = await database.fetch_val(reserved_query) or 0  
    if reserved + new_required > available:
        raise HTTPException(
            status_code=422,
            detail=[
                {
                    "loc": ["body", "order"],
                    "msg": "Insufficient funds",
                    "type": "value_error.insufficient_funds",
                }
            ],
        )
    query = orders.insert().values(
        id=order_id,
        user_id=user_id,
        ticker=order.ticker,
        direction=order.direction.value,
        qty=order.qty,
        price=getattr(order, "price", None),
        status=OrderStatus.NEW.value,
        filled=0,
        timestamp=datetime.now(timezone.utc)
    )
    await database.execute(query)
    async with semaphore:
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


@app.get(
    "/api/v1/order/{order_id}",
    response_model=Union[LimitOrder, MarketOrder],
    responses={422: {"model": HTTPValidationError}},
)
async def get_order(order_id: UUID, authorization: Optional[str] = Header(None)):
    user_id = await get_user_id(authorization)
    query = orders.select().where(orders.c.id == order_id)
    order = await database.fetch_one(query)
    if not order or order["user_id"] != user_id:
        raise HTTPException(status_code=404, detail='Not found')

    if order["price"] is not None:
        return LimitOrder(
            id=order["id"],
            status=OrderStatus(order["status"]),
            user_id=order["user_id"],
            timestamp=order["timestamp"],
            body=LimitOrderBody(
                direction=Direction(order["direction"]),
                ticker=order["ticker"],
                qty=order["qty"],
                price=order["price"]
            ),
            filled=order["filled"]
        )
    else:
        return MarketOrder(
            id=order["id"],
            status=OrderStatus(order["status"]),
            user_id=order["user_id"],
            timestamp=order["timestamp"],
            body=MarketOrderBody(
                direction=Direction(order["direction"]),
                ticker=order["ticker"],
                qty=order["qty"]
            )
        )



@app.delete("/api/v1/order/{order_id}",
    response_model=Ok,
    responses={
        422: {"model": HTTPValidationError}
    }
)
async def cancel_order(order_id: UUID, authorization: Optional[str] = Header(None)):
    user_id = await get_user_id(authorization)
    query = orders.select().where(orders.c.id == order_id)
    order = await database.fetch_one(query)
    if not order or order["user_id"] != user_id:
        raise HTTPException(status_code=404, detail="Order not found")
    # Проверка статуса
    status_check_query = orders.select().where(
        and_(
            orders.c.id == order_id,
            or_(
                orders.c.status == OrderStatus.NEW.value,
                orders.c.status == OrderStatus.PARTIALLY_EXECUTED.value
            ),
            orders.c.price.isnot(None)
        )
    )
    status_check = await database.fetch_one(status_check_query)
    if not status_check:
        # Если условие не выполнено
        raise HTTPException(
            status_code=400,
            detail="Order cannot be canceled in its current state"
        )
    update_query = orders.update().where(orders.c.id == order_id).values(status=OrderStatus.CANCELLED.value)
    await database.execute(update_query)
    return Ok()


# Admin


@app.post("/api/v1/admin/balance/deposit",
    response_model=Ok,
    responses={
        422: {"model": HTTPValidationError}
    }
)
async def deposit(body: DepositWithdrawBody, authorization: Optional[str] = Header(None)):
    await get_admin_id(authorization)
    query = balances.select().where((balances.c.user_id == body.user_id) & (balances.c.ticker == body.ticker))
    row = await database.fetch_one(query)
    if row:
        update_query = balances.update() \
            .where((balances.c.user_id == body.user_id) & (balances.c.ticker == body.ticker)) \
            .values(amount=row["amount"] + body.amount)
        await database.execute(update_query)
    else:
        insert_query = balances.insert().values(user_id=body.user_id, ticker=body.ticker, amount=body.amount)
        await database.execute(insert_query)
    return Ok()


@app.post(
    "/api/v1/admin/balance/withdraw",
    response_model=Ok,
    responses={
        422: {"model": HTTPValidationError}
    }
)
async def withdraw(body: DepositWithdrawBody, authorization: Optional[str] = Header(None)):
    await get_admin_id(authorization)
    query = balances.select().where((balances.c.user_id == body.user_id) & (balances.c.ticker == body.ticker))
    row = await database.fetch_one(query)
    if not row or row["amount"] < body.amount:
        raise HTTPException(status_code=400, detail="Insufficient funds")
    update_query = balances.update() \
        .where((balances.c.user_id == body.user_id) & (balances.c.ticker == body.ticker)) \
        .values(amount=row["amount"] - body.amount)
    await database.execute(update_query)
    return Ok()

@app.post("/api/v1/admin/instrument",
    response_model=Ok,
    responses={
        422: {"model": HTTPValidationError}
    }
)
async def add_instrument(instr: Instrument, authorization: Optional[str] = Header(None)):
    await get_admin_id(authorization)
    query = instruments.select().where(instruments.c.ticker == instr.ticker)
    existing = await database.fetch_one(query)
    if existing:
        raise HTTPException(status_code=400, detail=f"Instrument with ticker {instr.ticker} already exists")
    query = instruments.insert().values(ticker=instr.ticker, name=instr.name)
    await database.execute(query)
    return Ok()


@app.delete("/api/v1/admin/instrument/{ticker}",
    response_model=Ok,
    responses={
        422: {"model": HTTPValidationError}
    }
)
async def delete_instrument(ticker: str, authorization: Optional[str] = Header(None)):
    await get_admin_id(authorization)
    query = instruments.delete().where(instruments.c.ticker == ticker)
    await database.execute(query)
    return Ok()


@app.delete("/api/v1/admin/user/{user_id}",
    response_model=Ok,
    responses={
        422: {"model": HTTPValidationError}
    }
)
async def delete_user(user_id: UUID, authorization: Optional[str] = Header(None)):
    await get_admin_id(authorization)
    query = users.select().where(users.c.id == user_id)
    user = await database.fetch_one(query)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    #await database.execute(balances.delete().where(balances.c.user_id == user_id))
    await database.execute(users.delete().where(users.c.id == user_id))
    return User(**user)