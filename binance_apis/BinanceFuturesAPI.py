import time
import hashlib
import hmac
from decimal import Decimal, ROUND_HALF_UP, ROUND_DOWN

import requests
from utils import config
import aiohttp
from urllib.parse import urlencode
from warning_error_handlers import initial_retry_decorator, email_error_handler, exponential_backoff, log_error_handler


class BinanceFuturesAPI:
    BASE_FAPI_URL_V1 = "https://fapi.binance.com/fapi/v1"
    BASE_FAPI_URL_V2 = "https://fapi.binance.com/fapi/v2"
    BASE_FAPI_URL_V3 = "https://fapi.binance.com/fapi/v3"

    def __init__(self, key, secret, futures_exchange_info=None, multi_assets_margin="true"):
        self.key = key
        self.secret = secret
        self.futures_exchange_info = futures_exchange_info  # 由 create 方法注入

        if multi_assets_margin not in ["true", "false"]:
            raise ValueError("margin_mode must be 'true' or 'false'.")
        self.multi_assets_margin = multi_assets_margin

    @classmethod
    async def create(cls, key, secret, multi_assets_margin="true"):
        self = cls(key, secret, multi_assets_margin=multi_assets_margin)
        self.futures_exchange_info = await self.get_futures_exchange_info()
        return self

    async def get_futures_exchange_info(self):
        path = f"{self.BASE_FAPI_URL_V1}/exchangeInfo"
        return await  self._get(path, {})

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
        return '0'

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

    @initial_retry_decorator(retry_count=10, initial_delay=1, max_delay=30,
                             error_handler=log_error_handler, backoff_strategy=exponential_backoff)
    async def buy_limit(self, market, quantity, rate):
        path = "%s/order" % self.BASE_FAPI_URL_V1
        params = self._order(market, quantity, "BUY", rate)
        return await self._post(path, params)

    @initial_retry_decorator(retry_count=10, initial_delay=1, max_delay=30, error_handler=email_error_handler,
                             backoff_strategy=exponential_backoff)
    async def sell_limit(self, market, quantity, rate):
        path = "%s/order" % self.BASE_FAPI_URL_V1
        params = self._order(market, quantity, "SELL", rate)
        return await self._post(path, params)

    @initial_retry_decorator(retry_count=10, initial_delay=1, max_delay=30, error_handler=email_error_handler,
                             backoff_strategy=exponential_backoff)
    async def buy_market(self, market, quantity):
        path = f"{self.BASE_FAPI_URL_V1}/order"
        params = self._order(market, quantity, "BUY")
        return await self._post(path, params)

    @initial_retry_decorator(retry_count=10, initial_delay=1, max_delay=30, error_handler=email_error_handler,
                             backoff_strategy=exponential_backoff)
    async def sell_market(self, market, quantity):
        path = f"{self.BASE_FAPI_URL_V1}/order"
        params = self._order(market, quantity, "SELL")
        return await self._post(path, params)

    @initial_retry_decorator(retry_count=10, initial_delay=5, max_delay=60, error_handler=email_error_handler,
                             backoff_strategy=exponential_backoff)
    async def set_stop_market_order(self, symbol, side, quantity, stopPrice, closePosition=False):
        path = "%s/order" % self.BASE_FAPI_URL_V1
        stop_price = self._futures_format_price(symbol, stopPrice)
        quantity = self._futures_format_quantity(symbol, quantity)
        params = {"symbol": symbol, "side": side, "type": "STOP_MARKET", "quantity": quantity, "stopPrice": stop_price,
                  "closePosition": closePosition}
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

    async def get_24hr_price_change_ranking(self):
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
    async def cancel_order(self, market, order_id):
        path = "%s/order" % self.BASE_FAPI_URL_V1
        params = {"symbol": market, "orderId": order_id}
        return await self._delete(path, params)

    @initial_retry_decorator(retry_count=10, initial_delay=5, max_delay=60, error_handler=email_error_handler,
                             backoff_strategy=exponential_backoff)
    async def cancel_all_orders(self, symbol):
        path = f"{self.BASE_FAPI_URL_V1}/allOpenOrders"
        params = {"symbol": symbol}
        return await self._delete(path, params)

    async def _get_no_sign(self, path, params={}):
        query = urlencode(params)
        url = "%s?%s" % (path, query)
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=30, ssl=True) as response:
                return await response.json()

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
        headers = {"X-MBX-APIKEY": self.key}
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
