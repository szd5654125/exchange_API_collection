import time
from decimal import Decimal, ROUND_HALF_UP, ROUND_DOWN

import requests
import hashlib
import hmac
from utils import config
import aiohttp
from urllib.parse import urlencode
from warning_error_handlers import initial_retry_decorator, email_error_handler, log_error_handler, exponential_backoff


async def _get_no_sign(path, params={}):
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
        return await  self._get(path, {})

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
        for symbol in positions:
            if symbol.get("symbol") == symbol:
                total_symbol_amt += float(symbol.get("positionAmt"))
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

    @initial_retry_decorator(retry_count=10, initial_delay=1, max_delay=30, error_handler=email_error_handler,
                             backoff_strategy=exponential_backoff)
    async def buy_limit_um(self, symbol, quantity, price):
        path = "%s/um/order" % self.BASE_PAPI_URL_V1
        params = self._order(symbol, quantity, "BUY", price)
        return await self._post(path, params)

    @initial_retry_decorator(retry_count=10, initial_delay=1, max_delay=30, error_handler=email_error_handler,
                             backoff_strategy=exponential_backoff)
    async def sell_limit_um(self, symbol, quantity, price):
        path = "%s/um/order" % self.BASE_PAPI_URL_V1
        params = self._order(symbol, quantity, "SELL", price)
        return await self._post(path, params)

    @initial_retry_decorator(retry_count=10, initial_delay=1, max_delay=30, error_handler=email_error_handler,
                             backoff_strategy=exponential_backoff)
    async def buy_market_um(self, symbol, quantity):
        path = f"{self.BASE_PAPI_URL_V1}/um/order"
        params = self._order(symbol, quantity, "BUY")
        return await self._post(path, params)

    @initial_retry_decorator(retry_count=10, initial_delay=1, max_delay=30, error_handler=email_error_handler,
                             backoff_strategy=exponential_backoff)
    async def sell_market_um(self, symbol, quantity):
        path = f"{self.BASE_PAPI_URL_V1}/um/order"
        params = self._order(symbol, quantity, "SELL")
        return await self._post(path, params)

    @initial_retry_decorator(retry_count=10, initial_delay=5, max_delay=60, error_handler=email_error_handler,
                             backoff_strategy=exponential_backoff)
    async def set_stop_market_order_um(self, symbol, side, quantity, stopPrice):
        path = "%s/um/conditional/order" % self.BASE_PAPI_URL_V1
        formatted_quantity = self._futures_format_quantity(symbol, quantity)
        formatted_stop_price = self._futures_format_price(symbol, stopPrice)
        params = {"symbol": symbol, "side": side, "strategyType": "STOP_MARKET", "quantity": formatted_quantity,
                  "stopPrice": formatted_stop_price}
        return await self._post(path, params)

    # 撤销um特定交易对订单
    @initial_retry_decorator(retry_count=10, initial_delay=5, max_delay=60, error_handler=email_error_handler,
                             backoff_strategy=exponential_backoff)
    async def cancel_orders_um(self, symbol, orderId):
        path = f"{self.BASE_PAPI_URL_V1}/um/order"
        params = {"symbol": symbol, "orderId": orderId}
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

    async def _sign(self, params={}):
        data = params.copy()

        ts = await self.get_server_time()
        data.update({"timestamp": ts})
        h = urlencode(data)
        b = bytearray()
        b.extend(self.secret.encode())
        signature = hmac.new(b, msg=h.encode('utf-8'), digestmod=hashlib.sha256).hexdigest()
        data.update({"signature": signature})
        return data

    def _sign_sync(self, params={}):
        data = params.copy()
        ts = int(1000 * time.time())
        data.update({"timestamp": ts})
        h = urlencode(data)
        b = bytearray()
        b.extend(self.secret.encode())
        signature = hmac.new(b, msg=h.encode('utf-8'), digestmod=hashlib.sha256).hexdigest()
        data.update({"signature": signature})
        return data

    async def _get(self, path, params={}):
        params.update({"recvWindow": config['recv_window']})
        query = urlencode(await self._sign(params))
        url = f"{path}?{query}"
        headers = {"X-MBX-APIKEY": self.key}
        # 使用aiohttp创建异步的HTTP session
        async with aiohttp.ClientSession() as session:
            # 发送GET请求
            async with session.get(url, headers=headers, ssl=True) as response:
                return await response.json()

    def _get_sync(self, path, params={}):
        params.update({"recvWindow": config['recv_window']})
        query = urlencode(self._sign_sync(params))
        url = f"{path}?{query}"
        headers = {"X-MBX-APIKEY": self.key}
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        return response.json()

    async def _post(self, path, params={}):
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
        params = {}

        if rate is not None:
            params["type"] = "LIMIT"
            params["price"] = self._futures_format_price(market, rate)
            params["timeInForce"] = "GTC"
        else:
            params["type"] = "MARKET"

        params["symbol"] = market
        params["side"] = side
        params["quantity"] = self._futures_format_quantity(market, quantity)

        return params

    async def _delete(self, path, params={}):
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

    def _get_step_size(self, symbol):
        for s in self.futures_exchange_info["symbols"]:
            if s["symbol"] == symbol:
                for f in s["filters"]:
                    if f["filterType"] == "LOT_SIZE":
                        return f["stepSize"]
        raise ValueError(f"stepSize not found for symbol {symbol}")

    def _futures_format_price(self, symbol, price):
        tick_size = Decimal(self._get_tick_size(symbol))
        price = Decimal(str(price))
        # 四舍五入为 tick_size 的倍数
        steps = (price / tick_size).to_integral_value(rounding=ROUND_HALF_UP)
        return float(steps * tick_size)

    def _futures_format_quantity(self, symbol, quantity):
        step_size = Decimal(self._get_step_size(symbol))
        quantity = Decimal(str(quantity))
        steps = (quantity / step_size).to_integral_value(rounding=ROUND_DOWN)
        return float(steps * step_size)
