import time
import hashlib
import requests
import hmac
from utils import config
import aiohttp
from urllib.parse import urlencode


class BinanceCMFuturesAPI:
    BASE_URL = "https://dapi.binance.com/dapi/v1"
    BASE_URL_V2 = "https://dapi.binance.com/dapi/v2"

    def __init__(self, key, secret):
        self.key = key
        self.secret = secret

    '''def get_klines(self, market, interval, limit, startTime=None, endTime=None):
        path = "%s/klines" % self.BASE_URL
        params = {"symbol": market, "interval": interval, "limit": limit}
        if startTime:
            params["startTime"] = startTime
        if endTime:
            params["endTime"] = endTime
        return self._get_no_sign(path, params)'''

    async def get_klines(self, market, interval, limit, startTime=None, endTime=None):
        path = "%s/klines" % self.BASE_URL
        params = {"symbol": market, "interval": interval, "limit": limit}
        if startTime:
            params["startTime"] = startTime
        if endTime:
            params["endTime"] = endTime
        async with aiohttp.ClientSession() as session:  # 使用 aiohttp 的 ClientSession
            async with session.get(path, params=params) as response:  # 发起异步 GET 请求
                return await response.json()

    '''def get_position_amount(self, market):
        account_info = self.get_account()
        positions = account_info.get('positions', [])
        for position in positions:
            if position.get("symbol") == market:
                return position.get("positionAmt")
        return 0'''

    async def get_position_amount(self, market):
        account_info = await self.get_account()
        positions = account_info.get('positions', [])
        for position in positions:
            if position.get("symbol") == market:
                return float(position.get("positionAmt"))
        return '0'

    '''def get_account(self):
        path = "%s/account" % self.BASE_URL_V2
        return self._get(path, {})'''

    async def get_account(self):
        path = f"{self.BASE_URL}/account"
        # 使用 await 等待异步 _get 方法的结果
        return await self._get(path, {})

    def get_server_time(self):
        path = "%s/time" % self.BASE_URL
        return requests.get(path, timeout=30, verify=True).json()

    def get_exchange_info(self):
        path = "%s/exchangeInfo" % self.BASE_URL
        return requests.get(path, timeout=30, verify=True).json()

    def buy_limit(self, market, quantity, rate):
        path = "%s/order" % self.BASE_URL
        params = self._order(market, quantity, "BUY", rate)
        return self._post(path, params)

    def sell_limit(self, market, quantity, rate):
        path = "%s/order" % self.BASE_URL
        params = self._order(market, quantity, "SELL", rate)
        return self._post(path, params)

    '''def buy_market(self, market, quantity):
        path = "%s/order" % self.BASE_URL
        params = self._order(market, quantity, "BUY")
        return self._post(path, params)'''

    async def buy_market(self, market, quantity):
        path = f"{self.BASE_URL}/order"
        params = self._order(market, quantity, "BUY")
        return await self._post(path, params)

    '''def sell_market(self, market, quantity):
        path = "%s/order" % self.BASE_URL
        params = self._order(market, quantity, "SELL")
        return self._post(path, params)'''

    async def sell_market(self, market, quantity):
        path = f"{self.BASE_URL}/order"
        params = self._order(market, quantity, "SELL")
        return await self._post(path, params)

    async def set_stop_market_order(self, symbol, side, stopPrice, quantity=None, closePosition='true'):
        path = "%s/order" % self.BASE_URL
        params = {"symbol": symbol, "side": side, "type": "STOP_MARKET", "quantity": quantity, "stopPrice": stopPrice,
                  "closePosition": closePosition}
        return await self._post(path, params)

    def last_price(self, market):
        path = "%s/ticker/price" % self.BASE_URL
        params = {"symbol": market}
        return self._get_no_sign(path, params)

    def cancel(self, market, order_id):
        path = "%s/order" % self.BASE_URL
        params = {"symbol": market, "orderId": order_id}
        return self._delete(path, params)

    def _get_no_sign(self, path, params={}):
        query = urlencode(params)
        url = "%s?%s" % (path, query)
        return requests.get(url, timeout=30, verify=True).json()

    def _sign(self, params={}):
        data = params.copy()

        ts = int(1000 * time.time())
        data.update({"timestamp": ts})
        h = urlencode(data)
        b = bytearray()
        b.extend(self.secret.encode())
        signature = hmac.new(b, msg=h.encode('utf-8'), digestmod=hashlib.sha256).hexdigest()
        data.update({"signature": signature})
        return data


    '''def _get(self, path, params={}):
        params.update({"recvWindow": config.recv_window})
        query = urlencode(self._sign(params))
        url = "%s?%s" % (path, query)
        header = {"X-MBX-APIKEY": self.key}
        return requests.get(url, headers=header, timeout=30, verify=True).json()'''

    async def _get(self, path, params={}):
        params.update({"recvWindow": config['recv_window']})
        query = urlencode(self._sign(params))
        url = f"{path}?{query}"
        headers = {"X-MBX-APIKEY": self.key}
        # 使用aiohttp创建异步的HTTP session
        async with aiohttp.ClientSession() as session:
            # 发送GET请求
            async with session.get(url, headers=headers, ssl=True) as response:
                return await response.json()

    '''def _post(self, path, params={}):
        params.update({"recvWindow": config.recv_window})
        query = urlencode(self._sign(params))
        url = "%s" % (path)
        header = {"X-MBX-APIKEY": self.key}
        return requests.post(url, headers=header, data=query, \
                             timeout=30, verify=True).json()
'''
    async def _post(self, path, params={}):
        params.update({"recvWindow": config['recv_window']})
        query = urlencode(self._sign(params))
        url = "%s" % (path)
        headers = {"X-MBX-APIKEY": self.key}
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, data=query, timeout=30) as response:
                # 直接返回解析为 JSON 的响应数据，不进行异常处理
                return await response.json()

    def _order(self, market, quantity, side, rate=None):
        params = {}

        if rate is not None:
            params["type"] = "LIMIT"
            params["price"] = self._format(rate)
            params["timeInForce"] = "GTC"
        else:
            params["type"] = "MARKET"

        params["symbol"] = market
        params["side"] = side
        params["quantity"] = '%.3f' % quantity

        return params

    '''def _delete(self, path, params={}):
        params.update({"recvWindow": config.recv_window})
        query = urlencode(self._sign(params))
        url = "%s?%s" % (path, query)
        header = {"X-MBX-APIKEY": self.key}
        return requests.delete(url, headers=header, timeout=30, verify=True).json()'''

    async def _delete(self, path, params={}):
        params.update({"recvWindow": config['recv_window']})
        query = urlencode(self._sign(params))  # 确保这里正确地生成了签名
        url = "%s?%s" % (path, query)
        headers = {"X-MBX-APIKEY": self.key}

        async with aiohttp.ClientSession() as session:
            async with session.delete(url, headers=headers) as response:
                return await response.json()

    async def cancel_all_orders(self, symbol):
        path = f"{self.BASE_URL}/allOpenOrders"
        params = {"symbol": symbol}
        return await self._delete(path, params)

    def _format(self, price):
        return "{:.2f}".format(price)