import time
import hashlib
import hmac
import asyncio
from utils import config
import aiohttp
from urllib.parse import urlencode
from warning_error_handlers import (initial_retry_decorator, infinite_retry_decorator, exponential_backoff,
                                    log_error_handler, email_error_handler)


class BinanceSpotAPI:
    BASE_SAPI_URL_V1 = "https://api.binance.com/sapi/v1"
    BASE_API_URL_V3 = "https://api.binance.com/api/v3"
    PUBLIC_URL = "https://www.binance.com/exchange/public/product"
    STATUS_MAP = {0: 'Email Sent', 1: 'Cancelled', 2: 'Awaiting Approval', 3: 'Rejected', 4: 'Processing', 5: 'Failure',
                  6: 'Completed'}

    def __init__(self, key, secret):
        self.key = key
        self.secret = secret

    async def get_server_time(self) -> int:
        """直接返回 Binance 服务器时间，单位毫秒（int类型）"""
        path = "%s/time" % self.BASE_API_URL_V3
        async with aiohttp.ClientSession() as session:
            async with session.get(path, timeout=30, ssl=True) as response:
                data = await response.json()
                server_time = data.get('serverTime')
                if server_time is None:
                    return int(1000 * time.time())
                return int(server_time)

    @initial_retry_decorator(retry_count=10, initial_delay=1, max_delay=30, error_handler=log_error_handler,
                             backoff_strategy=exponential_backoff)
    async def get_account(self):
        path = "%s/account" % self.BASE_API_URL_V3
        return await self._get(path)

    async def get_network_depositable(self, coin: str, network: str = None):
        path = f"{self.BASE_SAPI_URL_V1}/capital/deposit/address"
        params = {"coin": coin.upper()}
        if network:
            params["network"] = network.upper()
        return await self._get(path, params)

    # 获取特定币种特点网络的存钱地址
    async def get_deposit_address_by_network(self, coin: str, network: str = None) -> str:
        result = await self.get_network_depositable(coin, network)
        address = result.get('address')
        return address

    async def get_network_withdrawable(self):
        path = f"{self.BASE_SAPI_URL_V1}/capital/config/getall"
        # 使用 await 等待异步 _get 方法的结果
        return await self._get(path, {})

    async def verify_coin_network_withdrawable(self, coin: str, network: str) -> bool:
        withdrawable_info = await self.get_network_withdrawable()
        for item in withdrawable_info:
            if item['coin'].upper() == coin.upper():
                for net in item.get("networkList", []):
                    if net["network"].upper() == network.upper():
                        return net.get("withdrawEnable", False)
        return False

    async def withdraw(self, coin: str, address: str, amount: float, network: str, address_tag: str = None):
        path = f"{self.BASE_SAPI_URL_V1}/capital/withdraw/apply"
        params = {
            "coin": coin.upper(),
            "address": address,
            "amount": str(amount),  # API 要求字符串格式
        }
        if network:
            params["network"] = network
        if address_tag:
            params["addressTag"] = address_tag

        # POST 请求是完整 URL 而不是拼接 base_url
        return await self._post(path, params)

    async def get_withdraw_history(self):
        path = f"{self.BASE_SAPI_URL_V1}/capital/withdraw/history"
        # 使用 await 等待异步 _get 方法的结果
        return await self._get(path, {})

    async def check_withdraw_status(self, withdraw_id: str):
        all_withdraw_history = await self.get_withdraw_history()
        for tx in all_withdraw_history:
            if tx.get("id") == withdraw_id:
                return self.STATUS_MAP.get(tx.get("status"), f"Unknown Status ({tx.get('status')})")
        print(f'没有找到该id{withdraw_id}的提现')
        return None

    async def poll_withdraw_status(self, withdraw_id: str, max_attempts: int = 10, delay_sec: int = 10):
        attempt = 0
        while True:
            all_history = await self.get_withdraw_history()
            record = next((item for item in all_history if item["id"] == withdraw_id), None)
            attempt += 1
            if not record:
                print(f"[错误] 未找到提现记录 ID: {withdraw_id}")
                return
            status_code = record["status"]
            status_name = self.STATUS_MAP.get(status_code, f"未知状态码: {status_code}")
            if status_code in (3, 5):
                print(status_name)
                return status_name
            elif status_code == 6:
                return status_name
            elif status_code in (0, 1, 2, 4):
                if attempt == max_attempts:
                    print(f"[提示] 网络可能拥堵，轮询 {max_attempts} 次状态仍未终止。")
                await asyncio.sleep(delay_sec)
            else:
                print(f'{status_code}不在已知列表中')
                return status_name

    async def get_asset_amount_balances(self, market):
        account_info = await self.get_account()
        balances = account_info.get('balances', [])

        asset_amt = 0.0
        for asset in balances:
            if asset.get('asset') == market:
                asset_amt = float(asset.get("free"))
        return asset_amt

    # client_spot.asset_transfer({'type': 'MAIN_PORTFOLIO_MARGIN', 'asset': 'BNB', 'amount': 0.1})
    @initial_retry_decorator(retry_count=10, initial_delay=1, max_delay=30, error_handler=email_error_handler,
                             backoff_strategy=exponential_backoff)
    async def asset_transfer(self, params={}):
        path = "%s/asset/transfer" % self.BASE_SAPI_URL_V1
        return await self._post(path, params)

    @infinite_retry_decorator(retry_interval=120)
    @initial_retry_decorator(retry_count=10, initial_delay=1, max_delay=30, error_handler=email_error_handler,
                             backoff_strategy=exponential_backoff)
    async def last_price(self, market):
        path = "%s/ticker/price" % self.BASE_API_URL_V3
        params = {"symbol": market}
        return await self._get_no_sign(path, params)

    @initial_retry_decorator(retry_count=10, initial_delay=1, max_delay=30, error_handler=email_error_handler,
                             backoff_strategy=exponential_backoff)
    async def buy_market(self, market, quantity):
        path = "%s/order" % self.BASE_API_URL_V3
        params = self._order(market, quantity, "BUY")
        return await self._post(path, params)

    @initial_retry_decorator(retry_count=10, initial_delay=1, max_delay=30, error_handler=email_error_handler,
                             backoff_strategy=exponential_backoff)
    async def use_usdt_buy_bfusd(self, amount):
        path = "%s/portfolio/mint" % self.BASE_SAPI_URL_V1
        params = {"fromAsset": 'USDT', 'targetAsset': 'BFUSD', 'amount': amount}
        return await self._post(path, params)

    @initial_retry_decorator(retry_count=10, initial_delay=1, max_delay=30, error_handler=email_error_handler,
                             backoff_strategy=exponential_backoff)
    async def redeem_bfusd_to_usdt(self, amount):
        path = "%s/portfolio/redeem" % self.BASE_SAPI_URL_V1
        params = {"fromAsset": 'BFUSD', 'targetAsset': 'USDT', 'amount': amount}
        return await self._post(path, params)

    @initial_retry_decorator(retry_count=10, initial_delay=1, max_delay=30, error_handler=email_error_handler,
                             backoff_strategy=exponential_backoff)
    async def fund_auto_collection(self):
        path = "%s/portfolio/auto-collection" % self.BASE_SAPI_URL_V1
        return await self._post(path)

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

    async def _post(self, path, params={}):
        params.update({"recvWindow": config['recv_window']})
        query = urlencode(await self._sign(params))
        url = "%s" % path
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
            params["price"] = self._format(rate)
            params["timeInForce"] = "GTC"
        else:
            params["type"] = "MARKET"

        params["symbol"] = market
        params["side"] = side
        params["quantity"] = '%.3f' % quantity

        return params

    async def _delete(self, path, params={}):
        params.update({"recvWindow": config['recv_window']})
        query = urlencode(await self._sign(params))
        url = "%s?%s" % (path, query)
        headers = {"X-MBX-APIKEY": self.key}

        async with aiohttp.ClientSession() as session:
            async with session.delete(url, headers=headers) as response:
                return await response.json()

    def _format(self, price):
        return "{:.8f}".format(price)
