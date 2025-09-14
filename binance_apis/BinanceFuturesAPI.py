import time
import hashlib
import hmac
from decimal import Decimal, ROUND_HALF_UP, ROUND_DOWN

import requests
from utils.utils import config
import aiohttp
from urllib.parse import urlencode
from warning_error_handlers import initial_retry_decorator, email_error_handler, exponential_backoff, log_error_handler


class BinanceFuturesAPI:
    BASE_FAPI_URL_V1 = "https://fapi.binance.com/fapi/v1"
    BASE_FAPI_URL_V2 = "https://fapi.binance.com/fapi/v2"
    BASE_FAPI_URL_V3 = "https://fapi.binance.com/fapi/v3"

    def __init__(self, key, secret, symbol=None, futures_exchange_info=None, multi_assets_margin="true"):
        self.key = key
        self.secret = secret
        self.futures_exchange_info = futures_exchange_info  # 由 create 方法注入
        self.symbol = symbol

        if multi_assets_margin not in ["true", "false"]:
            raise ValueError("margin_mode must be 'true' or 'false'.")
        self.multi_assets_margin = multi_assets_margin

    @classmethod
    async def create(cls, key, secret, symbol=None, multi_assets_margin="true"):
        self = cls(key, secret, symbol, multi_assets_margin=multi_assets_margin)
        self.futures_exchange_info = await self.get_futures_exchange_info()
        return self

    async def get_futures_exchange_info(self):
        path = f"{self.BASE_FAPI_URL_V1}/exchangeInfo"
        return await self._get(path, {})

    @initial_retry_decorator(retry_count=10, initial_delay=1, max_delay=30, error_handler=log_error_handler,
                             backoff_strategy=exponential_backoff)
    async def get_assets_margin_mode(self):
        path = f"{self.BASE_FAPI_URL_V1}/multiAssetsMargin"
        # 使用 await 等待异步 _get 方法的结果
        return await self._get(path, {})

    @initial_retry_decorator(retry_count=10, initial_delay=1, max_delay=30, error_handler=log_error_handler,
                             backoff_strategy=exponential_backoff)
    async def get_balance(self):
        path = f"{self.BASE_FAPI_URL_V3}/balance"
        # 使用 await 等待异步 _get 方法的结果
        return await self._get(path, {})

    async def get_wallet_balance(self, asset_name, wallet_type):
        valid_wallet_types = ['balance', 'crossWalletBalance', 'crossUnPnl', 'availableBalance', 'maxWithdrawAmount',
                              'marginAvailable']
        if wallet_type not in valid_wallet_types:
            raise ValueError(f"Invalid wallet type: {wallet_type}. Valid types are: {valid_wallet_types}")
        balance_data = await self.get_balance()
        for item in balance_data:
            if item['asset'] == asset_name:
                return float(item[wallet_type])
        return 0.0

    @initial_retry_decorator(retry_count=5, initial_delay=1, max_delay=30, error_handler=email_error_handler,
                             backoff_strategy=exponential_backoff)
    async def get_klines(self, market, interval, limit, start_time=None, end_time=None):
        path = "%s/klines" % self.BASE_FAPI_URL_V1
        params = {"symbol": market, "interval": interval, "limit": limit}
        if start_time:
            params["startTime"] = start_time
        if end_time:
            params["endTime"] = end_time
        return await self._get_no_sign(path, params)

    @initial_retry_decorator(retry_count=10, initial_delay=1, max_delay=30, error_handler=log_error_handler,
                             backoff_strategy=exponential_backoff)
    async def get_account(self):
        path = f"{self.BASE_FAPI_URL_V2}/account"
        # 使用 await 等待异步 _get 方法的结果
        return await self._get(path, {})

    @initial_retry_decorator(retry_count=10, initial_delay=5, max_delay=60, error_handler=email_error_handler,
                             backoff_strategy=exponential_backoff)
    async def check_all_open_orders(self, symbol):
        path = f"{self.BASE_FAPI_URL_V1}/openOrders"
        params = {"symbol": symbol}
        return await self._get(path, params)

    async def get_account_usdt_value(self):
        account_info = await self.get_account()
        # 直接从 account_info 字典中获取 'actualEquity' 的值
        actual_equity = account_info.get('totalWalletBalance', '0')
        return float(actual_equity)

    async def get_position_amount(self, market):
        account_info = await self.get_account()
        positions = account_info.get('positions', [])
        for position in positions:
            if position.get("symbol") == market:
                return float(position.get("positionAmt"))
        return 0.0

    def _max_position_limit(self, symbol):
        f = self._get_symbol_filters(symbol).get("MAX_POSITION")
        return float(f["maxPosition"]) if f and "maxPosition" in f else None

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

    async def get_exchange_info(self):
        path = "%s/exchangeInfo" % self.BASE_FAPI_URL_V1
        async with aiohttp.ClientSession() as session:
            async with session.get(path, timeout=30, ssl=True) as response:
                return await response.json()

    def _get_symbol_filters(self, symbol):
        for s in self.futures_exchange_info["symbols"]:
            if s["symbol"] == symbol:
                return {f["filterType"]: f for f in s["filters"]}
        raise ValueError(f"filters not found for {symbol}")

    async def get_current_leverage(self, symbol: str) -> int:
        account_info = await self.get_account()
        positions = account_info.get('positions', [])
        for pos in positions:
            if pos.get("symbol") == symbol:
                return int(pos.get("leverage", 1))
        raise ValueError(f"未找到交易对 {symbol} 的持仓信息")

    async def get_um_max_leverage(self, symbol: str, notional: float = 0.0) -> int:
        path = f"{self.BASE_FAPI_URL_V1}/leverageBracket"
        response = await self._get(path, {"symbol": symbol})
        if isinstance(response, dict) and "code" in response:
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

    async def set_leverage(self, symbol: str, leverage: int):
        path = f"{self.BASE_FAPI_URL_V1}/leverage"
        params = {
            "symbol": symbol,
            "leverage": leverage
        }
        return await self._post(path, params)

    def check_if_um_future_trading(self, symbol):
        """检查指定交易对是否处于交易状态"""
        if not self.futures_exchange_info or 'symbols' not in self.futures_exchange_info:
            return False
        # 使用 next() 函数查找匹配的交易对
        symbol_info = next(
            (s for s in self.futures_exchange_info['symbols'] if s['symbol'] == symbol),
            None
        )
        return symbol_info is not None and symbol_info['status'] == 'TRADING'

    @initial_retry_decorator(retry_count=10, initial_delay=1, max_delay=30, error_handler=log_error_handler,
                             backoff_strategy=exponential_backoff)
    async def change_assets_margin_mode(self, multi_assets_margin: str):
        if multi_assets_margin not in ["true", "false"]:
            raise ValueError("multi_assets_margin must be 'true' or 'false'.")
        path = f"{self.BASE_FAPI_URL_V1}/multiAssetsMargin"
        params = {"multiAssetsMargin": multi_assets_margin}
        return await self._post(path, params)

    async def ensure_margin_mode(self):
        # 查询当前的保证金模式
        current_mode_response = await self.get_assets_margin_mode()
        current_mode = str(current_mode_response.get("multiAssetsMargin", "false")).lower()
        if current_mode != self.multi_assets_margin:
            print(f"Current margin mode: {current_mode}, switching to {self.multi_assets_margin}...")
            await self.change_assets_margin_mode(self.multi_assets_margin)
        else:
            print(f"Margin mode already set to {self.multi_assets_margin}. No action needed.")

    @initial_retry_decorator(retry_count=5, initial_delay=1, max_delay=30,
                             error_handler=email_error_handler, backoff_strategy=exponential_backoff)
    async def get_order_book_depth_sum(self, symbol: str, side: str, limit: int = 20) -> float:
        allowed = {5, 10, 20, 50, 100, 500, 1000}
        api_limit = min(x for x in allowed if x >= limit) if any(x >= limit for x in allowed) else max(allowed)
        path = f"{self.BASE_FAPI_URL_V1}/depth"
        params = {"symbol": symbol, "limit": api_limit}
        data = await self._get_no_sign(path, params)
        bids = [(float(p), float(q)) for p, q in data.get("bids", [])]
        asks = [(float(p), float(q)) for p, q in data.get("asks", [])]
        if side == "asks":
            levels = asks
        elif side == "bids":
            levels = bids
        else:
            raise ValueError("side 必须是 'bids' 或 'asks'")
        return sum(q for _, q in levels[:limit])

    def lot_limits(self, symbol):
        f = self._get_symbol_filters(symbol).get("LOT_SIZE")
        if not f:
            raise ValueError(f"LOT_SIZE not found for {symbol}")
        return float(f["minQty"]), float(f["maxQty"]), float(f["stepSize"])

    @initial_retry_decorator(retry_count=10, initial_delay=1, max_delay=30, error_handler=log_error_handler,
                             backoff_strategy=exponential_backoff)
    async def buy_limit(self, market, quantity, rate, client_order_id=None):
        path = "%s/order" % self.BASE_FAPI_URL_V1
        params = self._order(market, quantity, "BUY", rate)
        if client_order_id:
            params["newClientOrderId"] = client_order_id
        return await self._post(path, params)

    @initial_retry_decorator(retry_count=10, initial_delay=1, max_delay=30, error_handler=email_error_handler,
                             backoff_strategy=exponential_backoff)
    async def sell_limit(self, market, quantity, rate, client_order_id=None):
        path = "%s/order" % self.BASE_FAPI_URL_V1
        params = self._order(market, quantity, "SELL", rate)
        if client_order_id:
            params["newClientOrderId"] = client_order_id
        return await self._post(path, params)

    def market_lot_limits(self, symbol):
        f = self._get_symbol_filters(symbol).get("MARKET_LOT_SIZE")
        if not f:
            raise ValueError(f"MARKET_LOT_SIZE not found for {symbol}")
        return float(f["minQty"]), float(f["maxQty"]), float(f["stepSize"])

    @initial_retry_decorator(retry_count=10, initial_delay=1, max_delay=30, error_handler=email_error_handler,
                             backoff_strategy=exponential_backoff)
    async def buy_market(self, market, quantity, client_order_id=None):
        path = f"{self.BASE_FAPI_URL_V1}/order"
        params = self._order(market, quantity, "BUY")
        if client_order_id:
            params["newClientOrderId"] = client_order_id
        return await self._post(path, params)

    @initial_retry_decorator(retry_count=10, initial_delay=1, max_delay=30, error_handler=email_error_handler,
                             backoff_strategy=exponential_backoff)
    async def sell_market(self, market, quantity, client_order_id=None):
        path = f"{self.BASE_FAPI_URL_V1}/order"
        params = self._order(market, quantity, "SELL")
        if client_order_id:
            params["newClientOrderId"] = client_order_id
        return await self._post(path, params)

    async def get_best_bid_ask(self, symbol):
        path = f"{self.BASE_FAPI_URL_V1}/ticker/bookTicker"
        params = {"symbol": symbol}
        response = await self._get_no_sign(path, params)
        try:
            best_bid = float(response["bidPrice"])
            best_ask = float(response["askPrice"])
            return best_bid, best_ask
        except KeyError:
            raise ValueError(f"Failed to retrieve best bid/ask for {symbol}. Response: {response}")

    @initial_retry_decorator(retry_count=10, initial_delay=5, max_delay=60, error_handler=email_error_handler,
                             backoff_strategy=exponential_backoff)
    async def set_stop_market_order(self, symbol, side, quantity, stopPrice, closePosition=False):
        path = "%s/order" % self.BASE_FAPI_URL_V1
        stop_price = self.futures_format_price(symbol, stopPrice)
        qty_fmt = self.futures_format_quantity_market(symbol, quantity)
        params = {
            "symbol": symbol,
            "side": side,
            "type": "STOP_MARKET",
            "quantity": qty_fmt,
            "stopPrice": stop_price,
            "closePosition": closePosition
        }
        return await self._post(path, params)

    @initial_retry_decorator(retry_count=5, initial_delay=1, max_delay=30, error_handler=email_error_handler,
                             backoff_strategy=exponential_backoff)
    async def last_price(self, market):
        path = "%s/ticker/price" % self.BASE_FAPI_URL_V1
        params = {"symbol": market}
        return await self._get_no_sign(path, params)

    async def get_all_prices(self):
        path = f"{self.BASE_FAPI_URL_V1}/ticker/price"
        return await self._get_no_sign(path)

    async def get_last_24hr_price_change(self):
        path = "%s/ticker/24hr" % self.BASE_FAPI_URL_V1
        return await self._get_no_sign(path)

    async def get_24h_usd_volume(self, symbol: str) -> float:
        path = f"{self.BASE_FAPI_URL_V1}/ticker/24hr"
        params = {"symbol": symbol}
        data = await self._get_no_sign(path, params)
        return float(data["quoteVolume"])

    async def get_usdt_24hr_price_change_ranking(self):
        try:
            # 获取所有币种的24小时涨跌数据
            data = await self.get_last_24hr_price_change()
            # 过滤出 USDT 合约（可根据你需求定制，比如 endswith('USDT')）
            usdt_symbols = [item for item in data if item['symbol'].endswith('USDT')]
            # 构建排序列表，包含 symbol 和涨幅百分比（转 float）
            ranked_list = sorted(
                [
                    {
                        'symbol': item['symbol'],
                        'priceChangePercent': float(item['priceChangePercent']),
                        'lastPrice': float(item['lastPrice']),
                        'volume': float(item['volume']),
                        'quoteVolume': float(item['quoteVolume']),
                    }
                    for item in usdt_symbols
                ],
                key=lambda x: x['priceChangePercent'],
                reverse=True  # 最大涨幅在前
            )
            return ranked_list
        except Exception as e:
            print(f"获取24小时涨跌幅数据失败: {e}")
            return []

    @initial_retry_decorator(retry_count=10, initial_delay=5, max_delay=60, error_handler=email_error_handler,
                             backoff_strategy=exponential_backoff)
    async def cancel_order(self, market, order_id=None, client_order_id=None):
        path = "%s/order" % self.BASE_FAPI_URL_V1
        params = {"symbol": market}
        if order_id is not None:
            params["orderId"] = order_id
        if client_order_id is not None:
            params["origClientOrderId"] = client_order_id
        return await self._delete(path, params)

    @initial_retry_decorator(retry_count=10, initial_delay=5, max_delay=60, error_handler=email_error_handler,
                             backoff_strategy=exponential_backoff)
    async def cancel_stop_loss_orders(self, symbol):
        try:
            open_orders = await self.check_all_open_orders(symbol)
            conditional_types = {
                "STOP", "STOP_MARKET",
                "TAKE_PROFIT", "TAKE_PROFIT_MARKET",
                "TRAILING_STOP_MARKET"
            }
            results = []
            for od in open_orders:
                if od.get("type") in conditional_types:
                    order_id = od.get("orderId")
                    client_oid = od.get("clientOrderId")
                    res = await self.cancel_order(symbol, order_id=order_id, client_order_id=client_oid)
                    results.append(res)
            return results
        except Exception as e:
            return {"error": str(e)}

    @initial_retry_decorator(retry_count=10, initial_delay=5, max_delay=60, error_handler=email_error_handler,
                             backoff_strategy=exponential_backoff)
    async def cancel_all_orders(self, symbol):
        path = f"{self.BASE_FAPI_URL_V1}/allOpenOrders"
        params = {"symbol": symbol}
        return await self._delete(path, params)

    async def _get_no_sign(self, path, params=None):
        query = urlencode(params)
        url = "%s?%s" % (path, query)
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=30, ssl=True) as response:
                return await response.json()

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
        headers = {"X-MBX-APIKEY": self.key}
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
        query = urlencode(await self._sign(params))  # 确保这里正确地生成了签名
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

    def _get_step_size(self, symbol):
        for s in self.futures_exchange_info["symbols"]:
            if s["symbol"] == symbol:
                for f in s["filters"]:
                    if f["filterType"] == "LOT_SIZE":
                        return f["stepSize"]
        raise ValueError(f"stepSize not found for symbol {symbol}")

    def futures_format_price(self, symbol, price):
        tick_size = Decimal(self._get_tick_size(symbol))
        price = Decimal(str(price))
        # 四舍五入为 tick_size 的倍数
        steps = (price / tick_size).to_integral_value(rounding=ROUND_HALF_UP)
        return float(steps * tick_size)

    def _get_step_size_limit(self, symbol):
        _, _, step = self.lot_limits(symbol)
        return step

    def _get_step_size_market(self, symbol):
        step = self._get_symbol_filters(symbol).get("MARKET_LOT_SIZE", {}).get("stepSize")
        if step is None:
            raise ValueError(f"MARKET_LOT_SIZE stepSize not found for {symbol}")
        min_q = self._get_symbol_filters(symbol).get("MARKET_LOT_SIZE", {}).get("minQty")
        max_q = self._get_symbol_filters(symbol).get("MARKET_LOT_SIZE", {}).get("maxQty")
        return step, min_q, max_q

    def futures_format_quantity_limit(self, symbol, quantity):
        step_size = Decimal(str(self._get_step_size_limit(symbol)))
        q = Decimal(str(quantity))
        steps = (q / step_size).to_integral_value(rounding=ROUND_DOWN)
        return float(steps * step_size)

    def futures_format_quantity_market(self, symbol, quantity):
        step, _, _ = self._get_step_size_market(symbol)
        step_size = Decimal(str(step))
        q = Decimal(str(quantity))
        steps = (q / step_size).to_integral_value(rounding=ROUND_DOWN)
        return float(steps * step_size)
