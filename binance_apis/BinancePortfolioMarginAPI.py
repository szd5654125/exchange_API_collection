import time
from decimal import Decimal, ROUND_HALF_UP, ROUND_DOWN
import requests
import hashlib
import hmac
from utils.utils import config
import aiohttp
from urllib.parse import urlencode
from warning_error_handlers import initial_retry_decorator, email_error_handler, log_error_handler, exponential_backoff
from binance_apis.errors import SymbolClosedError


async def _get_no_sign(path, params):
    query = urlencode(params)
    url = "%s?%s" % (path, query)
    async with aiohttp.ClientSession() as session:
        async with session.get(url, timeout=30, ssl=True) as response:
            return await response.json()


class BinancePortfolioMarginAPI:
    BASE_PAPI_URL_V1 = "https://papi.binance.com/papi/v1"
    BASE_FAPI_URL_V1 = "https://fapi.binance.com/fapi/v1"

    def __init__(self, key, secret, futures_exchange_info=None):
        self.key = key
        self.secret = secret
        self.futures_exchange_info = futures_exchange_info  # 由 create 方法注入

    @classmethod
    async def create(cls, key, secret):
        self = cls(key, secret)
        self.futures_exchange_info = await self.get_futures_exchange_info()
        return self

    async def get_futures_exchange_info(self):
        path = f"{self.BASE_FAPI_URL_V1}/exchangeInfo"
        return await self._get(path, {})

    def _get_symbol_filters(self, symbol):
        for s in self.futures_exchange_info["symbols"]:
            if s["symbol"] == symbol:
                return {f["filterType"]: f for f in s["filters"]}
        raise ValueError(f"filters not found for {symbol}")

    def market_lot_limits(self, symbol):
        f = self._get_symbol_filters(symbol).get("MARKET_LOT_SIZE")
        if not f:
            raise ValueError(f"MARKET_LOT_SIZE not found for {symbol}")
        return float(f["minQty"]), float(f["maxQty"]), float(f["stepSize"])

    def lot_limits(self, symbol):
        f = self._get_symbol_filters(symbol).get("LOT_SIZE")
        if not f:
            raise ValueError(f"LOT_SIZE not found for {symbol}")
        return float(f["minQty"]), float(f["maxQty"]), float(f["stepSize"])

    def _max_position_limit(self, symbol):
        f = self._get_symbol_filters(symbol).get("MAX_POSITION")
        return float(f["maxPosition"]) if f and "maxPosition" in f else None

    async def get_um_max_leverage(self, symbol: str, notional: float = 0.0) -> int:
        path = f"{self.BASE_PAPI_URL_V1}/um/leverageBracket"
        response = await self._get(path, {"symbol": symbol})
        if isinstance(response, dict) and response.get("code") == -4141:
            # 明确告诉上层：这个是不可恢复的“品种关闭”
            raise SymbolClosedError(f"Symbol closed: {symbol} (code=-4141)")
        if "error" in response:
            raise RuntimeError(f"API 返回错误: {response}")
        items = response if isinstance(response, list) else [response]
        for item in items:
            if item["symbol"] == symbol:
                brackets = item["brackets"]
                if notional == 0:
                    # 取所有层级中的最大 initialLeverage
                    return max(int(b["initialLeverage"]) for b in brackets)
                else:
                    # 找到对应名义价值所在的层级
                    for b in brackets:
                        if b["notionalFloor"] <= notional < b["notionalCap"]:
                            return int(b["initialLeverage"])
                    # 如果超过所有区间，则使用最低层
                    return int(brackets[-1]["initialLeverage"])
        raise ValueError(f"未找到交易对 {symbol} 的杠杆分层信息")

    # 获取某个币种的当前杠杆
    async def get_current_leverage_um(self, symbol: str) -> int:
        """获取某个币种当前设置的杠杆倍数"""
        account_info = await self.get_account_um()
        positions = account_info.get('positions', [])
        for pos in positions:
            if pos.get("symbol") == symbol:
                return int(pos.get("leverage", 1))  # leverage 字段包含当前杠杆
        raise ValueError(f"未找到交易对 {symbol} 的持仓信息")

    async def set_leverage_um(self, symbol: str, leverage: int):
        path = f"{self.BASE_PAPI_URL_V1}/um/leverage"
        params = {
            "symbol": symbol,
            "leverage": leverage
        }
        return await self._post(path, params)

    async def get_server_time(self) -> int:
        """直接返回 Binance 服务器时间，单位毫秒（int类型）"""
        path = "%s/time" % self.BASE_FAPI_URL_V1
        async with aiohttp.ClientSession() as session:
            async with session.get(path, timeout=30, ssl=True) as response:
                data = await response.json()
                server_time = data.get('serverTime')
                if server_time is None:
                    return int(1000 * time.time())
                return int(server_time)

    @initial_retry_decorator(retry_count=10, initial_delay=1, max_delay=30, error_handler=log_error_handler,
                             backoff_strategy=exponential_backoff)
    async def get_balance(self):
        path = f"{self.BASE_PAPI_URL_V1}/balance"
        # 使用 await 等待异步 _get 方法的结果
        return await self._get(path, {})

    async def get_all_usdt_m_funding_rates(self):
        path = f"{self.BASE_FAPI_URL_V1}/premiumIndex"
        result = await self._get(path, {})
        return result

    # await get_wallet_balance("USDC", "crossMarginFree")
    async def get_wallet_balance(self, *args):
        if len(args) % 2 != 0:
            raise ValueError("Arguments must be provided in pairs of asset_name and wallet_type.")
        # 提取参数对
        queries = [(args[i], args[i + 1]) for i in range(0, len(args), 2)]
        valid_wallet_types = [
            'totalWalletBalance', 'crossMarginAsset', 'crossMarginBorrowed', 'crossMarginFree',
            'crossMarginInterest', 'crossMarginLocked', 'umWalletBalance', 'umUnrealizedPNL',
            'cmWalletBalance', 'cmUnrealizedPNL', 'negativeBalance'
        ]
        for _, wallet_type in queries:
            if wallet_type not in valid_wallet_types:
                raise ValueError(f"Invalid wallet type: {wallet_type}. Valid types are: {valid_wallet_types}")
        balance_data = await self.get_balance()
        # 构建结果列表
        results = []
        for asset_name, wallet_type in queries:
            for item in balance_data:
                if item['asset'] == asset_name:
                    results.append(float(item.get(wallet_type, 0.0)))
                    break
            else:
                # 如果没有匹配到数据，设置为 0.0
                results.append(0.0)
        return tuple(results)

    @initial_retry_decorator(retry_count=10, initial_delay=1, max_delay=30, error_handler=log_error_handler,
                             backoff_strategy=exponential_backoff)
    async def get_account(self):
        path = f"{self.BASE_PAPI_URL_V1}/account"
        # 使用 await 等待异步 _get 方法的结果
        return await self._get(path, {})

    async def get_account_usdt_value(self):
        account_info = await self.get_account()
        # 直接从 account_info 字典中获取 'actualEquity' 的值
        actual_equity = account_info.get('actualEquity', '0')
        return float(actual_equity)

    @initial_retry_decorator(retry_count=10, initial_delay=1, max_delay=30, error_handler=log_error_handler,
                             backoff_strategy=exponential_backoff)
    async def get_account_um(self):
        path = f"{self.BASE_PAPI_URL_V1}/um/account"
        # 使用 await 等待异步 _get 方法的结果
        return await self._get(path, {})

    @initial_retry_decorator(retry_count=10, initial_delay=5, max_delay=60, error_handler=email_error_handler,
                             backoff_strategy=exponential_backoff)
    async def check_all_open_orders_um(self, symbol):
        path = f"{self.BASE_PAPI_URL_V1}/um/openOrders"
        params = {"symbol": symbol}
        return await self._get(path, params)

    # 会对不同方向的仓位进行累加
    async def get_position_amount_um(self, symbol):
        account_info = await self.get_account_um()
        positions = account_info.get('positions', [])

        total_symbol_amt = 0.0  # 累加所有相同符号的仓位
        for pos in positions:
            if pos.get("symbol") == symbol:
                total_symbol_amt += float(pos.get("positionAmt"))
        return total_symbol_amt

    async def get_asset_amount_um(self, symbol):
        account_info = await self.get_account_um()
        assets = account_info.get('assets', [])

        asset_amt = 0.0  # 累加所有相同符号的仓位
        for asset in assets:
            if asset.get('asset') == symbol:
                asset_amt += float(asset.get("crossWalletBalance"))
        return asset_amt

    @initial_retry_decorator(retry_count=10, initial_delay=1, max_delay=30, error_handler=log_error_handler,
                             backoff_strategy=exponential_backoff)
    async def bnb_transfer(self, amount, transfer_side):
        path = "%s/bnb-transfer" % self.BASE_PAPI_URL_V1
        params = {'amount': amount, 'transferSide': transfer_side}
        return await self._post(path, params)

    # await trading.binance_portfolio_margin_api_instance.get_liquidation_price_um("MOVEUSDT")
    async def get_liquidation_price_um(self, symbol: str) -> float | None:
        """
        查询某合约币种当前持仓的爆仓价格（仅适用于 USDT-M futures + Portfolio Margin 模式）
        """
        path = f"{self.BASE_PAPI_URL_V1}/um/positionRisk"
        result = await self._get(path, {})
        for position in result:
            if position["symbol"] == symbol:
                liq_price = position.get("liquidationPrice")
                return float(liq_price) if liq_price not in ("", None) else None
        return None  # 没找到该 symbol

    @initial_retry_decorator(retry_count=10, initial_delay=1, max_delay=30, error_handler=email_error_handler,
                             backoff_strategy=exponential_backoff)
    async def buy_limit_um(self, symbol, quantity, price, client_order_id=None):
        path = "%s/um/order" % self.BASE_PAPI_URL_V1
        params = self._order(symbol, quantity, "BUY", price)
        if client_order_id:
            params["newClientOrderId"] = client_order_id
        return await self._post(path, params)

    @initial_retry_decorator(retry_count=10, initial_delay=1, max_delay=30, error_handler=email_error_handler,
                             backoff_strategy=exponential_backoff)
    async def sell_limit_um(self, symbol, quantity, price, client_order_id=None):
        path = "%s/um/order" % self.BASE_PAPI_URL_V1
        params = self._order(symbol, quantity, "SELL", price)
        if client_order_id:
            params["newClientOrderId"] = client_order_id
        return await self._post(path, params)

    async def _cap_qty_for_market(self, symbol, qty):
        min_q, max_q, step = self.market_lot_limits(symbol)
        cap = min(float(qty), max_q)  # 只向下截到 max，不做步进
        max_pos = self._max_position_limit(symbol)
        if max_pos is not None:
            cur = await self.get_position_amount_um(symbol)  # signed
            allow = max(0.0, max_pos - abs(cur))  # 账户剩余额度
            cap = min(cap, allow)
        if cap < min_q:
            # 不强行把 cap 提到 min_q，避免“超买”。由上层决定是否继续/放弃。
            return 0.0, {"minQty": min_q, "maxQty": max_q, "step": step, "maxPosition": max_pos}
        return cap, {"minQty": min_q, "maxQty": max_q, "step": step, "maxPosition": max_pos}

    async def _market_order_split(self, symbol, side, quantity):
        filled = 0.0
        last_resp = None
        remain = float(quantity)
        # 防御性：避免极端情况下死循环
        max_loops = 100
        while remain > 0 and max_loops > 0:
            max_loops -= 1
            cap_req, meta = await self._cap_qty_for_market(symbol, remain)
            if cap_req <= 0:
                if filled == 0.0:
                    # 达到持仓最大值
                    break
            cap_fmt = self.futures_format_quantity_market(symbol, cap_req)
            # 落格后如果 < minQty，同样按“是否已有成交”处理
            if cap_fmt < meta["minQty"]:
                if filled == 0.0:
                    return {"code": -4005, "msg": "Formatted quantity < minQty",
                            "meta": {**meta, "requestedQty": quantity, "filledQty": filled, "remain": remain,
                                     "capReq": cap_req}}
                else:
                    break
            # 3) 发送本笔
            params = self._order(symbol, cap_fmt, side)  # MARKET
            last_resp = await self._post(f"{self.BASE_PAPI_URL_V1}/um/order", params)

            # 交易所返回负码，原样抛给上层；若已部分成交，也把上下文带回去
            if isinstance(last_resp, dict) and last_resp.get("code", 0) < 0:
                if filled > 0.0:
                    last_resp.setdefault("meta", {})
                    last_resp["meta"].update({"requestedQty": quantity, "filledQty": filled, "lastTryCapReq": cap_req,
                                              "lastTryCapFmt": cap_fmt, **meta})
                return last_resp
            filled += cap_fmt
            remain -= cap_fmt

        # 给一个统一的 meta，标注是否部分成交
        result = last_resp if isinstance(last_resp, dict) else {"code": 0, "data": last_resp}
        result.setdefault("meta", {})
        result["meta"].update({"requestedQty": quantity, "filledQty": filled, "remainderDropped": max(0.0, remain),
                               "partialFilled": filled < quantity})
        return result

    @initial_retry_decorator(retry_count=10, initial_delay=1, max_delay=30,
                             error_handler=email_error_handler, backoff_strategy=exponential_backoff)
    async def buy_market_um(self, symbol, quantity):
        return await self._market_order_split(symbol, "BUY", quantity)

    @initial_retry_decorator(retry_count=10, initial_delay=1, max_delay=30,
                             error_handler=email_error_handler, backoff_strategy=exponential_backoff)
    async def sell_market_um(self, symbol, quantity):
        return await self._market_order_split(symbol, "SELL", quantity)

    @initial_retry_decorator(retry_count=10, initial_delay=5, max_delay=60, error_handler=email_error_handler,
                             backoff_strategy=exponential_backoff)
    async def set_stop_market_order_um(self, symbol, side, quantity, stopPrice):
        path = f"{self.BASE_PAPI_URL_V1}/um/conditional/order"
        formatted_quantity = self.futures_format_quantity_market(symbol, quantity)
        formatted_stop_price = self.futures_format_price(symbol, stopPrice)
        params = {"symbol": symbol, "side": side.upper(), "strategyType": "STOP_MARKET", "quantity": formatted_quantity,
                  "stopPrice": formatted_stop_price}
        return await self._post(path, params)

    # 撤销um特定交易对订单
    @initial_retry_decorator(retry_count=10, initial_delay=5, max_delay=60, error_handler=email_error_handler,
                             backoff_strategy=exponential_backoff)
    async def cancel_orders_um(self, symbol, order_id=None, client_order_id=None):
        path = f"{self.BASE_PAPI_URL_V1}/um/order"
        params = {"symbol": symbol}
        if order_id:
            params["orderId"] = order_id
        if client_order_id:
            params["origClientOrderId"] = client_order_id
        return await self._delete(path, params)

    @initial_retry_decorator(retry_count=10, initial_delay=5, max_delay=60, error_handler=email_error_handler,
                             backoff_strategy=exponential_backoff)
    async def cancel_all_conditional_orders_um(self, symbol):
        path = f"{self.BASE_PAPI_URL_V1}/um/conditional/allOpenOrders"
        params = {"symbol": symbol}
        return await self._delete(path, params)

    @initial_retry_decorator(retry_count=10, initial_delay=5, max_delay=60, error_handler=email_error_handler,
                             backoff_strategy=exponential_backoff)
    async def cancel_all_orders_um(self, symbol):
        path = f"{self.BASE_PAPI_URL_V1}/um/allOpenOrders"
        params = {"symbol": symbol}
        return await self._delete(path, params)

    async def _sign(self, params):
        data = params.copy()

        ts = await self.get_server_time()
        data.update({"timestamp": ts})
        h = urlencode(data)
        b = bytearray()
        b.extend(self.secret.encode())
        signature = hmac.new(b, msg=h.encode('utf-8'), digestmod=hashlib.sha256).hexdigest()
        data.update({"signature": signature})
        return data

    def _sign_sync(self, params):
        data = params.copy()
        ts = int(1000 * time.time())
        data.update({"timestamp": ts})
        h = urlencode(data)
        b = bytearray()
        b.extend(self.secret.encode())
        signature = hmac.new(b, msg=h.encode('utf-8'), digestmod=hashlib.sha256).hexdigest()
        data.update({"signature": signature})
        return data

    async def _get(self, path, params):
        params.update({"recvWindow": config['recv_window']})
        query = urlencode(await self._sign(params))
        url = f"{path}?{query}"
        headers = {"X-MBX-APIKEY": self.key}
        # 使用aiohttp创建异步的HTTP session
        async with aiohttp.ClientSession() as session:
            # 发送GET请求
            async with session.get(url, headers=headers, ssl=True) as response:
                return await response.json()

    def _get_sync(self, path, params):
        params.update({"recvWindow": config['recv_window']})
        query = urlencode(self._sign_sync(params))
        url = f"{path}?{query}"
        headers = {"X-MBX-APIKEY": self.key}
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        return response.json()

    async def _post(self, path, params):
        params.update({"recvWindow": config['recv_window']})
        query = urlencode(await self._sign(params))
        url = path
        # 特殊协议
        headers = {"X-MBX-APIKEY": self.key, "Content-Type": "application/x-www-form-urlencoded"}
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, data=query, timeout=30) as response:
                # 直接返回解析为 JSON 的响应数据，不进行异常处理
                return await response.json()

    def _order(self, market, quantity, side, rate=None):
        params = {"symbol": market, "side": side}
        if rate is not None:
            params["type"] = "LIMIT"
            params["price"] = self.futures_format_price(market, rate)
            params["timeInForce"] = "GTC"
            params["quantity"] = self.futures_format_quantity_limit(market, quantity)
        else:
            params["type"] = "MARKET"
            params["quantity"] = self.futures_format_quantity_market(market, quantity)
        return params

    async def _delete(self, path, params):
        params.update({"recvWindow": config['recv_window']})
        query = urlencode(await self._sign(params))
        url = "%s?%s" % (path, query)
        headers = {"X-MBX-APIKEY": self.key}

        async with aiohttp.ClientSession() as session:
            async with session.delete(url, headers=headers) as response:
                return await response.json()

    def _get_tick_size(self, symbol):
        for s in self.futures_exchange_info["symbols"]:
            if s["symbol"] == symbol:
                for f in s["filters"]:
                    if f["filterType"] == "PRICE_FILTER":
                        return f["tickSize"]
        raise ValueError(f"tickSize not found for symbol {symbol}")

    def _get_step_size_limit(self, symbol):
        for s in self.futures_exchange_info["symbols"]:
            if s["symbol"] == symbol:
                for f in s["filters"]:
                    if f["filterType"] == "LOT_SIZE":
                        return f["stepSize"]
        raise ValueError(f"stepSize not found for symbol {symbol}")

    def _get_step_size_market(self, symbol):
        f = self._get_symbol_filters(symbol).get("MARKET_LOT_SIZE")
        if not f:
            raise ValueError(f"MARKET_LOT_SIZE not found for {symbol}")
        return f["stepSize"], f["minQty"], f["maxQty"]

    def futures_format_price(self, symbol, price):
        tick_size = Decimal(self._get_tick_size(symbol))
        price = Decimal(str(price))
        # 四舍五入为 tick_size 的倍数
        steps = (price / tick_size).to_integral_value(rounding=ROUND_HALF_UP)
        return float(steps * tick_size)

    def futures_format_quantity_limit(self, symbol, quantity):
        step_size = Decimal(self._get_step_size_limit(symbol))
        quantity = Decimal(str(quantity))
        steps = (quantity / step_size).to_integral_value(rounding=ROUND_DOWN)
        return float(steps * step_size)

    def futures_format_quantity_market(self, symbol, quantity):
        step, _, _ = self._get_step_size_market(symbol)
        step_size = Decimal(str(step))
        q = Decimal(str(quantity))
        steps = (q / step_size).to_integral_value(rounding=ROUND_DOWN)
        return float(steps * step_size)
