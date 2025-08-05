import json
import logging
from datetime import datetime as dt, timezone
import time
import hmac
import hashlib
import asyncio

import aiohttp
import requests
from Crypto.Hash import SHA256
from Crypto.PublicKey import RSA
from Crypto.Signature import PKCS1_v1_5
import base64
import uuid
from decimal import Decimal, ROUND_DOWN
from pybit.exceptions import FailedRequestError, InvalidRequestError

try:
    from simplejson.errors import JSONDecodeError
except ImportError:
    from json.decoder import JSONDecodeError

HTTP_URL = 'https://api.bybit.com'


class BybitV5API:
    """
    A unified wrapper for Bybit v5 REST API.

    This client supports SPOT, linear (USDT-margined), inverse (coin-margined),
    and options trading on both Unified Account and Classic Account structures.

    Common Parameters:
        category (str): Market type for the order/query. One of:
            - 'linear'  → USDT-margined perpetual contract
            - 'inverse' → Coin-margined perpetual contract
            - 'spot'    → Spot market
            - 'option'  → Options trading
            Notes:
                - Unified Account supports all four.
                - Classic Account supports: 'linear', 'inverse', 'spot'.

        symbol (str): Trading pair, e.g., 'BTCUSDT'.

        coin (str): Coin name, e.g., 'USDT', 'BTC'. Used in balance/transfer APIs.

        accountType (str): Used in balance-related endpoints.
            One of: 'UNIFIED', 'FUND', 'SPOT'.

        positionIdx (int): Used in hedge-mode to identify position side.
            - 0: one-way mode
            - 1: hedge-mode long
            - 2: hedge-mode short

        recv_window (int): Request validity window in milliseconds.
            Default is 5000.

    Configuration Options:
        max_retries (int): Maximum number of retries on request failure. Default is 3.
        retry_delay (int): Time delay (in seconds) before retrying a failed request.
        timeout (int): Timeout for each HTTP request, in seconds.
        log_requests (bool): Enable request/response logging for debugging.
        force_retry (bool): Retry on network-related errors (timeouts, disconnects).

    Notes:
        - Use `auth=True` when calling private endpoints (e.g., trading, balance).
        - Responses from Bybit are returned as Python dictionaries (parsed JSON).
    """
    recv_window = 5000
    max_retries = 3
    api_key = ''
    api_secret = ''
    rsa_authentication = ''
    log_requests = False
    timeout = 10
    force_retry = False
    retry_delay = 3
    retry_codes = {}
    ignore_codes = {}
    return_response_headers = False
    record_request_time = False

    def __init__(self, api_key, api_secret, linear_exchange_info=None, spot_exchange_info=None):
        self.client = requests.Session()
        self.logger = logging.getLogger(__name__)
        self.api_key = api_key
        self.api_secret = api_secret
        self.linear_exchange_info = linear_exchange_info
        self.spot_exchange_info = spot_exchange_info

    @classmethod
    async def create(cls, key, secret):
        self = cls(key, secret)
        self.linear_exchange_info = await self.get_instruments_info('linear')
        self.spot_exchange_info = await self.get_instruments_info('spot')
        return self

    async def get_server_timestamp(self):
        try:
            data = await self.get_server_time()
            return int(data.get("time"))
        except Exception:
            # 如果取服务器时间失败，fallback本地时间
            return self.generate_timestamp()

    # 获取合约交易对信息
    async def get_instruments_info(self, category):
        """
        Get USDT-margined (linear) perpetual contract metadata including max leverage.

        Returns:
            dict: API response containing list of symbols and metadata.
        """
        return await self._request(
            method="GET",
            path="/v5/market/instruments-info",
            query={"category": category},
            auth=False
        )

    # bit.get_kline(category='linear', symbol='BTCUSDT', interval='1', limit=10)
    async def get_kline(self, category: str, symbol: str, interval: str, start: int = None, end: int = None,
                        limit: int = None) -> dict:
        """
        Get historical Kline (candlestick) data.

        Parameters:
            category (str): Required. Market category. One of:
                            'linear' (USDT perpetual), 'inverse' (coin perpetual), 'spot'
            symbol (str): Required. Trading pair symbol, e.g., 'BTCUSDT'.
            interval (str): Required. Candlestick interval.
                            One of: ['1', '3', '5', '15', '30', '60', '120', '240', '360', '720', 'D', 'M', 'W']
            start (int, optional): Start timestamp in milliseconds (e.g., 1669852800000 for 1 DEC 2022 UTC 0:00).
            end (int, optional): End timestamp in milliseconds (e.g., 1671062400000 for 15 DEC 2022 UTC 0:00).
            limit (int, optional): Number of data points to return. Default is 200. Max is 1000.

        Returns:
            dict: API response with list of OHLCV candles.
        """
        query = {"category": category, "symbol": symbol, "interval": interval}
        if start is not None:
            query["start"] = start
        if end is not None:
            query["end"] = end
        if limit is not None:
            query["limit"] = limit

        return await self._request(
            method="GET",
            path="/v5/market/kline",
            query=query,
        )

    # bit.get_wallet_balance(coin='USDT')
    async def get_wallet_balance(self, coin: str = None) -> dict:
        query = {"accountType": "UNIFIED"}
        if coin:
            query["coin"] = coin.upper()

        return await self._request(
            method="GET",
            path="/v5/account/wallet-balance",
            query=query,
            auth=True,
        )

    # bybit_api_instance.get_coin_value('USDC', 'walletBalance')
    async def get_coin_value(self, coin: str, value_type: str) -> float:
        valid_fields = {
            'walletBalance', 'equity', 'usdValue', 'unrealisedPnl', 'cumRealisedPnl',
            'locked', 'availableToWithdraw', 'borrowAmount', 'bonus',
            'availableToBorrow', 'accruedInterest', 'totalOrderIM',
            'totalPositionIM', 'totalPositionMM', 'spotHedgingQty'
        }
        if value_type not in valid_fields:
            raise ValueError(f"Invalid value_type '{value_type}'. Valid options are: {valid_fields}")
        # 异步请求数据
        data = await self.get_wallet_balance(coin=coin)
        coin_list = data.get('result', {}).get('list', [])[0].get('coin', [])
        for item in coin_list:
            if item.get('coin') == coin.upper():
                val = item.get(value_type, '0')
                try:
                    return float(val) if val != '' else 0.0
                except ValueError:
                    return 0.0
        return 0.0

    async def get_coin_borrow_amount(self, coin: str = "USDT") -> float:
        try:
            # 调用 Bybit API 获取账户余额信息
            response = await self.get_wallet_balance()
            # 检查 API 响应的有效性
            if response.get("retCode") != 0:
                print(f"API返回错误: {response.get('retMsg')}")
                return 0.0
            # 提取币种列表
            coins = response.get("result", {}).get("list", [])[0].get("coin", [])
            # 遍历所有币种，查找指定币种
            for item in coins:
                if item.get("coin") == coin.upper():
                    # 提取欠款金额
                    borrow_amount = item.get("borrowAmount", "0")
                    try:
                        # 将欠款金额转换为浮点数
                        borrow_amount = float(borrow_amount)
                        return borrow_amount if borrow_amount > 0 else 0.0
                    except (ValueError, TypeError):
                        print(f"欠款金额格式错误: {borrow_amount}")
                        return 0.0
            # 没有找到指定币种
            print(f"未找到币种: {coin}")
            return 0.0

        except Exception as e:
            print(f"获取欠款金额时发生错误: {e}")
            return 0.0

    async def get_fund_balance(self, account_type: str = "FUND", coin: str = None, with_bonus: int = 0) -> dict:
        query = {"accountType": account_type, "withBonus": with_bonus}
        if coin:
            query["coin"] = coin.upper()
        return await self._request(
            method="GET",
            path="/v5/asset/transfer/query-account-coins-balance",
            query=query,
            auth=True,
        )

    async def get_fund_balance_value(self, account_type: str = "FUND", coin: str = None,
                                     value_type: str = "transferBalance") -> float | None:
        result = await self.get_fund_balance(account_type, coin)
        valid_fields = {'transferBalance', 'walletBalance'}
        if value_type not in valid_fields:
            raise ValueError(f"Invalid value_type '{value_type}'. Valid options are: {valid_fields}")
        balances = result.get("result", {}).get("balance", [])
        for entry in balances:
            if entry.get("coin") == coin.upper():
                value_str = entry.get(value_type)
                return float(value_str) if value_str not in (None, "") else 0.0
        # 没有找到该币种
        return None

    # print(bit.account_info())
    async def account_info(self):
        return await self._request(
            method="GET",
            path="/v5/account/info",
            auth=True,
        )

    async def get_bybit_coin_info(self):
        return await self._request(
            method="GET",
            path="/v5/asset/coin/query-info",
            auth=True,
        )

    async def verify_coin_network_withdrawable(self, coin: str, network: str) -> bool:
        try:
            response = await self.get_bybit_coin_info()
            if response.get("retCode") != 0:
                raise Exception(f"Bybit 返回错误: {response.get('retMsg')}")

            all_coins = response.get("result", {}).get("rows", [])
            for coin_info in all_coins:
                if coin_info.get("coin") == coin.upper():
                    chains = coin_info.get("chains", [])
                    for chain_info in chains:
                        if chain_info.get("chainType", "").lower() == network.lower():
                            return chain_info.get("chainWithdraw") == "1"
            return False
        except Exception as e:
            print(f"查询提现网络出错: {e}")
            return False

    async def withdrawable_amount(self, coin: str):
        return await self._request(
            query={"coin": coin.upper()},
            method="GET",
            path="/v5/asset/withdraw/withdrawable-amount",
            auth=True,
        )

    # print(bybit_api_instance.check_deposit_address('USDC'))
    async def check_deposit_address(self, coin):
        return await self._request(
            query={"coin": coin},
            method="GET",
            path="/v5/asset/deposit/query-address",
            auth=True,
        )

    async def get_deposit_address_by_network(self, coin: str, network: str) -> str | None:
        try:
            response = await self.check_deposit_address(coin)
            if response.get("retCode") != 0:
                raise Exception(f"Bybit 返回错误: {response.get('retMsg')}")

            chains = response.get("result", {}).get("chains", [])
            for chain_info in chains:
                if chain_info.get("chainType", "").lower() == network.lower():
                    return chain_info.get("addressDeposit")
            return None  # 如果没有找到对应网络
        except Exception as e:
            print(f"查询充值地址时出错: {e}")
            return None

    async def query_asset_info(self, coin: str = None):
        """
            Query SPOT account asset information.

            Note:
                This function only works with 'SPOT' accountType.
                If your account is UTA 2.0 and does not include a SPOT wallet,
                this endpoint will return unuseful data.

            Parameters:
                coin (str, optional): The coin symbol (uppercase). If omitted,
                                      returns all available spot assets.

            Returns:
                dict: Bybit API response with spot asset details.
            """
        query = {"accountType": "SPOT"}
        if coin:
            query["coin"] = coin.upper()  # 转为大写
        return await self._request(
            method="GET",
            path="/v5/asset/transfer/query-asset-info",
            query=query,
            auth=True,
        )

    # print(bit.query_account_coins_balance(accountType='UNIFIED', coin='USDT'))
    async def query_account_coins_balance(self, accountType: str, coin: str = None):
        """
        Query account coins balance.

        Parameters:
            accountType (str): Required. Must be 'UNIFIED' or 'FUND'.
            coin (str, optional): Coin name (uppercase). Optional.

        Returns:
            dict: Bybit API response.
        """
        query = {"accountType": accountType}
        if coin:
            query["coin"] = coin.upper()
        return await self._request(
            method="GET",
            path="/v5/asset/transfer/query-account-coins-balance",
            query=query,
            auth=True,
        )

    async def get_transfer_balance(self, account_type='UNIFIED', coin='USDC', balancetype='transferBalance'):
        response = await self.query_account_coins_balance(account_type, coin)
        try:
            # 从结果中提取 transferBalance（字符串 -> 浮点数）
            balance_info = response['result']['balance']
            for item in balance_info:
                if item['coin'] == coin.upper():
                    return float(item[balancetype])
            return 0.0  # 如果没有找到指定币种，默认返回 0
        except (KeyError, TypeError, ValueError) as e:
            print(f"[Error] Failed to parse transferBalance: {e}")
            return 0.0

    # 获取所有USDT结算的合约持仓
    async def get_all_positions(self, category: str = "linear", settle_coins: list = None) -> list:
        """
        获取所有持仓信息，支持多个结算币种

        Parameters:
            category (str): 交易市场类型, 默认是 'linear'.
            settle_coins (list): 结算货币类型列表，默认是 ['USDT', 'USDC'].

        Returns:
            list: 包含所有持仓信息的列表
        """
        if settle_coins is None:
            settle_coins = ['USDT', 'USDC']

        all_positions = []

        for settle_coin in settle_coins:
            try:
                response = await self._request(
                    method="GET",
                    path="/v5/position/list",
                    query={"category": category, "settleCoin": settle_coin},
                    auth=True
                )
                positions = response.get("result", {}).get("list", [])
                # 为每个仓位添加结算币种信息，便于后续处理
                for pos in positions:
                    pos['settleCoin'] = settle_coin
                all_positions.extend(positions)
            except Exception as e:
                print(f"获取{settle_coin}结算的持仓出错: {e}")
                continue

        return all_positions

    # 获得当前仓位爆仓价
    # await trading.bybit_api_instance.get_liquidation_price("linear", "MOVEUSDT")
    async def get_liquidation_price(self, category: str, symbol: str) -> float | None:
        resp = await self._request(
            auth=True,
            method="GET",
            path="/v5/position/list",
            query={"category": category, "symbol": symbol},
        )
        positions = resp.get("result", {}).get("list", [])
        if not positions:
            return None  # 无持仓
        liq_price_str = positions[0].get("liqPrice")
        return float(liq_price_str) if liq_price_str not in (None, "") else None

    # buyLeverage和sellLeverage设置了相同的数值
    async def set_leverage(self, category: str, symbol: str, leverage: float):
        leverage_str = str(leverage)
        return await self._request(
            auth=True,
            method="POST",
            path="/v5/position/set-leverage",
            query={
                "category": category,
                "symbol": symbol,
                "buyLeverage": leverage_str,
                "sellLeverage": leverage_str,
            }
        )

    async def set_all_linear_max_leverage(self):
        symbol_list = self.linear_exchange_info["result"]["list"]

        for symbol_info in symbol_list:
            symbol = symbol_info["symbol"]
            max_leverage = float(symbol_info["leverageFilter"]["maxLeverage"])
            try:
                await self.set_leverage("linear", symbol, max_leverage)
                print(f"已设置 {symbol} 杠杆为最大值 {max_leverage}")
            except Exception as e:
                print(f"设置 {symbol} 杠杆失败: {e} ")
                continue

    # print(bit.get_server_time())
    async def get_server_time(self, **kwargs):
        return await self._request(
            method="GET",
            path="/v5/market/time",
            query=kwargs,
        )

    # print(bit.buy_limit('linear','FILUSDT', 5, 2.4))
    async def buy_limit(self, category, symbol, quantity, price):
        # 当前仅对合约调整了精度
        price = await self.adjust_price_to_tick(category, symbol, price)
        quantity = await self.adjust_qty_to_step(category, symbol, quantity)
        return await self._request(
            auth=True,
            method="POST",
            path="/v5/order/create",
            query={
                'symbol': symbol,
                'category': category,
                'side': 'Buy',
                'orderType': 'Limit',
                'qty': quantity,
                'price': price
            },
        )

    async def sell_limit(self, category, symbol, quantity, price):
        # 当前仅对合约调整了精度
        price = await self.adjust_price_to_tick(category, symbol, price)
        quantity = await self.adjust_qty_to_step(category, symbol, quantity)
        return await self._request(
            auth=True,
            method="POST",
            path="/v5/order/create",
            query={
                'symbol': symbol,
                'category': category,
                'side': 'Sell',
                'orderType': 'Limit',
                'qty': quantity,
                'price': price
            },
        )

    # 注意：bybit下超过余额的单会报错（但是因为bybit杠杠比hyperliquid高很多，所以一般没关系）
    # print(bit.buy_market('linear', 'FILUSDT', 5))
    async def buy_market(self, category, symbol, quantity):
        quantity = await self.adjust_qty_to_step(category, symbol, quantity)
        return await self._request(
            auth=True,
            method="POST",
            path="/v5/order/create",
            query={
                'category': category,
                'symbol': symbol,
                'side': 'Buy',
                'orderType': 'Market',
                'qty': quantity
            },
        )

    # print(bit.sell_market('FILUSDT', 5, 'linear'))
    async def sell_market(self, category, symbol, quantity):
        quantity = await self.adjust_qty_to_step(category, symbol, quantity)
        return await self._request(
            auth=True,
            method="POST",
            path="/v5/order/create",
            query={
                'category': category,
                'symbol': symbol,
                'side': 'Sell',
                'orderType': 'Market',
                'qty': quantity
            },
        )

    # print(bit.last_price('linear', None))
    async def last_price(self, category, symbol=None):
        """
        Retrieve the latest market tickers from Bybit.

        This function queries the latest market information such as price,
        volume, funding rate, and order book data for one or more symbols
        within a specific category (market type).

        Behavior:
            - If `symbol` is **not provided**, it returns a list of all trading pairs
              under the specified category (e.g., all linear contracts), including:
                - lastPrice, indexPrice, markPrice
                - 24h volume, turnover, high/low price
                - fundingRate, openInterest, bid/ask data, etc.
            - If `symbol` is provided (e.g., 'FILUSDT'), it returns only the ticker info for that symbol.
        Note: If category=option, symbol or baseCoin must be passed.
        Returns:
            dict: Bybit API response containing a list of market tickers with detailed trading data.
        """
        return await self._request(  # 加 await
            method="GET",
            path="/v5/market/tickers",
            query={'category': category, 'symbol': symbol},
        )

    # bybit_api_instance.get_best_bid_ask('linear', 'BTCUSDT')
    async def get_best_bid_ask(self, category: str, symbol: str):
        response = await self.last_price(category, symbol)
        # 提取买一卖一价格
        result = response.get("result", {}).get("list", [])
        if not result:
            print(f"未找到 {symbol} 的买一卖一价格")
            return {}
        best_bid = float(result[0].get("bid1Price", 0))
        best_ask = float(result[0].get("ask1Price", 0))
        return best_bid, best_ask

    async def cancel_order(self, category, symbol, order_id):
        return await self._request(  # 加 await
            auth=True,
            method="POST",
            path="/v5/order/cancel",
            query={'category': category, 'symbol': symbol, 'orderId': order_id},
        )

    # print(bit.cancel_all_orders('linear', 'FILUSDT'))
    async def cancel_all_orders(self, category, symbol):
        return await self._request(  # 注意这里加 await
            auth=True,
            method="POST",
            path="/v5/order/cancel-all",
            query={'category': category, 'symbol': symbol},
        )

    # print(bit.position_trading_stop('linear', 'FILUSDT', stopLoss=3))
    async def position_trading_stop(self, category, symbol, takeProfit=0, stopLoss=0, positionIdx=0):
        """
        Set take profit (TP) and/or stop loss (SL) for an open position.

        This endpoint allows you to configure or cancel TP/SL settings.
        To cancel TP or SL, set the value to 0. The price must be greater than 0 if enabled.

        Parameters:
            category (str): Required. Product type. One of: 'linear', 'inverse'.
                            - UTA/Classic accounts support only linear and inverse.
            symbol (str): Required. Trading pair symbol, e.g., 'BTCUSDT'.
            takeProfit (float or str): Optional. TP price. Use 0 to cancel take profit.
            stopLoss (float or str): Optional. SL price. Use 0 to cancel stop loss.
            positionIdx (int): Required. Position mode identifier:
                               - 0: one-way mode (default)
                               - 1: hedge-mode Buy side
                               - 2: hedge-mode Sell side

        Returns:
            dict: Bybit API response (empty 'result' on success).

        Raises:
            InvalidRequestError: If stopLoss/takeProfit values are invalid (e.g., violate position direction).
        """
        # 当前仅对合约调整了精度
        if takeProfit:
            takeProfit = await self.adjust_price_to_tick(category, symbol, float(takeProfit))
        if stopLoss:
            stopLoss = await self.adjust_price_to_tick(category, symbol, float(stopLoss))
        return await self._request(  # 注意加 await
            auth=True,
            method="POST",
            path="/v5/position/trading-stop",
            query={
                'category': category,
                'symbol': symbol,
                'positionIdx': positionIdx,
                'takeProfit': takeProfit,
                'stopLoss': stopLoss,
            },
        )

    # print(bit.asset_withdraw('USDT', 'BSC', '0xf663dded85cf7d1e9af810ce952ca4004fe772c9', 10))
    # 提币地址必须在地址簿中
    async def asset_withdraw(self, coin, chain, address, amount, force_chain=1):
        amount = await self.adjust_qty_to_step('spot', f'{coin}USDT', amount)
        payload = {
            "coin": coin.upper(),
            "chain": chain,
            "address": address,
            "amount": str(amount),
            "timestamp": int(time.time() * 1000),
            "forceChain": force_chain
        }

        return await self._request(  # 注意这里加了 await
            auth=True,
            method="POST",
            path="/v5/asset/withdraw/create",
            query=payload
        )

    # 单次查询提现状态
    async def query_withdraw_status(self, withdraw_id: str) -> dict:
        return await self._request(
            method="GET",
            path="/v5/asset/withdraw/query-record",
            query={"withdrawID": withdraw_id},
            auth=True,
        )

    # 轮询提现状态
    async def poll_withdraw_status(self, withdraw_id: str, max_attempts: int = 10, delay_sec: int = 10):
        attempt = 0
        while True:
            response = await self.query_withdraw_status(withdraw_id)
            if not response:
                print(f"[错误] 未找到提现记录 ID: {withdraw_id}")
                return
            attempt += 1
            rows = response.get("result", {}).get("rows", [])
            if not rows:
                print(f"[错误] 查询结果为空，ID: {withdraw_id}")
                return
            first_record = rows[0]
            status = first_record.get("status")

            if status in {"Reject", "Fail", "MoreInformationRequired", "Unknown"}:
                print(status)
                return status
            elif status == "success":
                return status
            elif status in {"SecurityCheck", "Pending", "CancelByUser", "BlockchainConfirmed"}:
                print(status)
                if attempt == max_attempts:
                    print(f"[提示] 网络可能拥堵，轮询 {max_attempts} 次状态仍未终止。")
                    return status
                await asyncio.sleep(delay_sec)  # 注意加 await
            else:
                print(f'{status}不在已知列表中')
                return status

    # print(bit.query_transfer_coin_list('FUND', 'UNIFIED'))
    async def query_transfer_coin_list(self, fromAccountType, toAccountType):
        return await self._request(
            auth=True,
            method="GET",
            path="/v5/asset/transfer/query-transfer-coin-list",
            query={'fromAccountType': fromAccountType, 'toAccountType': toAccountType},
        )

    # print(bit.asset_transfer('USDT', '5', 'FUND', 'UNIFIED'))
    async def asset_transfer(self, coin, amount, fromAccountType, toAccountType):
        # 避免0转账引起错误
        if amount >= 0:
            amount = await self.adjust_qty_to_step('spot', f'{coin}USDT', amount)
            return await self._request(
                auth=True,
                method="POST",
                path="/v5/asset/transfer/inter-transfer",
                query={
                    'transferId': str(uuid.uuid4()),
                    'coin': coin,
                    'amount': amount,
                    'fromAccountType': fromAccountType,
                    'toAccountType': toAccountType,
                },
            )
        else:
            print(f'转账数额为{amount}，无效')

    @staticmethod
    def generate_timestamp():
        """
        Return a millisecond integer timestamp.
        """
        return int(time.time() * 10 ** 3)

    @staticmethod
    def prepare_payload(method, parameters):
        """
        Prepares the request payload and validates parameter value types.
        """

        def cast_values():
            string_params = [
                "qty",
                "price",
                "triggerPrice",
                "takeProfit",
                "stopLoss",
                "buyLeverage",
                "sellLeverage",
                "riskId",
                "orderId",
                "amount",
                "transferId",
                "timestamp",
            ]
            integer_params = ["positionIdx"]
            for key, value in parameters.items():
                if key in string_params:
                    if type(value) is not str:
                        parameters[key] = str(value)
                elif key in integer_params:
                    if type(value) is not int:
                        parameters[key] = int(value)

        if method == "GET":
            payload = "&".join(
                [
                    str(k) + "=" + str(v)
                    for k, v in sorted(parameters.items())
                    if v is not None
                ]
            )
            return payload
        else:
            cast_values()
            return json.dumps(parameters)

    @staticmethod
    def generate_signature(use_rsa_authentication, secret, param_str):
        def generate_hmac():
            hash = hmac.new(
                bytes(secret, "utf-8"),
                param_str.encode("utf-8"),
                hashlib.sha256,
            )
            return hash.hexdigest()

        def generate_rsa():
            hash = SHA256.new(param_str.encode("utf-8"))
            encoded_signature = base64.b64encode(
                PKCS1_v1_5.new(RSA.importKey(secret)).sign(
                    hash
                )
            )
            return encoded_signature.decode()

        if not use_rsa_authentication:
            return generate_hmac()
        else:
            return generate_rsa()

    def _auth(self, payload, recv_window, timestamp):
        """
        Prepares authentication signature per Bybit API specifications.
        """
        if self.api_key is None or self.api_secret is None:
            raise PermissionError("Authenticated endpoints require keys.")

        param_str = str(timestamp) + self.api_key + str(recv_window) + payload

        return self.generate_signature(
            self.rsa_authentication, self.api_secret, param_str
        )

    async def _request(self, method=None, path=None, query=None, auth=False):
        if query is None:
            query = {}

        recv_window = self.recv_window

        # 修复：浮点转整数防止签名出错
        for k in list(query.keys()):
            if isinstance(query[k], float) and query[k] == int(query[k]):
                query[k] = int(query[k])

        query = {k: v for k, v in query.items() if v is not None}

        retries_attempted = self.max_retries
        req_params = None
        path = HTTP_URL + path

        while True:
            retries_attempted -= 1
            if retries_attempted < 0:
                raise FailedRequestError(
                    request=f"{method} {path}: {req_params}",
                    message="Bad Request. Retries exceeded maximum.",
                    status_code=400,
                    time=dt.now(timezone.utc).strftime("%H:%M:%S"),
                    resp_headers=None,
                )

            retries_remaining = f"{retries_attempted} retries remain."

            req_params = self.prepare_payload(method, query)

            # 准备签名
            headers = {}
            if auth:
                timestamp = await self.get_server_timestamp()
                signature = self._auth(
                    payload=req_params,
                    recv_window=recv_window,
                    timestamp=timestamp,
                )
                headers = {
                    "Content-Type": "application/json",
                    "X-BAPI-API-KEY": self.api_key,
                    "X-BAPI-SIGN": signature,
                    "X-BAPI-SIGN-TYPE": "2",
                    "X-BAPI-TIMESTAMP": str(timestamp),
                    "X-BAPI-RECV-WINDOW": str(recv_window),
                }

            try:
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.timeout)) as session:
                    if method == "GET":
                        url = path + (f"?{req_params}" if req_params else "")
                        async with session.get(url, headers=headers) as response:
                            s = await response.text()
                            s_json = json.loads(s)
                    else:
                        async with session.request(method, path, data=req_params, headers=headers) as response:
                            s = await response.text()
                            s_json = json.loads(s)

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                if self.force_retry:
                    self.logger.error(f"{e}. {retries_remaining}")
                    await asyncio.sleep(self.retry_delay)
                    continue
                else:
                    raise e

            except JSONDecodeError as e:
                if self.force_retry:
                    self.logger.error(f"{e}. {retries_remaining}")
                    await asyncio.sleep(self.retry_delay)
                    continue
                else:
                    raise FailedRequestError(
                        request=f"{method} {path}: {req_params}",
                        message="Conflict. Could not decode JSON.",
                        status_code=409,
                        time=dt.now(timezone.utc).strftime("%H:%M:%S"),
                        resp_headers={},
                    )

            # HTTP 状态码不是 200
            if response.status != 200:
                raise FailedRequestError(
                    request=f"{method} {path}: {req_params}",
                    message=f"HTTP status code is {response.status}",
                    status_code=response.status,
                    time=dt.now(timezone.utc).strftime("%H:%M:%S"),
                    resp_headers=dict(response.headers),
                )

            if s_json.get("retCode"):
                ret_code = s_json.get("retCode")
                ret_msg = s_json.get("retMsg")
                error_msg = f"{ret_msg} (ErrCode: {ret_code})"

                if ret_code in self.retry_codes:
                    if ret_code == 10002:
                        recv_window += 2500
                    elif ret_code == 10006:
                        limit_reset_time = int(response.headers.get("X-Bapi-Limit-Reset-Timestamp", "0"))
                        delay_time = (limit_reset_time - await self.get_server_timestamp()) / 1e3
                        await asyncio.sleep(max(delay_time, 1))
                    self.logger.error(f"{error_msg}. {retries_remaining}")
                    await asyncio.sleep(self.retry_delay)
                    continue
                elif ret_code in self.ignore_codes:
                    pass
                else:
                    raise InvalidRequestError(
                        request=f"{method} {path}: {req_params}",
                        message=ret_msg,
                        status_code=ret_code,
                        time=dt.now(timezone.utc).strftime("%H:%M:%S"),
                        resp_headers=dict(response.headers),
                    )

            if self.return_response_headers:
                return s_json, response.headers
            else:
                return s_json

    def calculate_size_and_step(self, prices_or_qty) -> str:
        max_decimal_places = 0
        for i in prices_or_qty:
            if i and i != '0' and i != '':
                prices_or_qty_str = str(i).strip()
                if '.' in prices_or_qty_str:
                    decimal_part = prices_or_qty_str.split('.')[1].rstrip('0')
                    if decimal_part:
                        decimal_places = len(decimal_part)
                        max_decimal_places = max(max_decimal_places, decimal_places)
        # 统一用一个公式处理所有情况
        if max_decimal_places == 0:
            return "1"
        else:
            return f"0.{'0' * (max_decimal_places - 1)}1"

    def analyze_bybit_data(self, data_dict: dict, price_or_qty) -> str:
        if 'result' in data_dict and 'list' in data_dict['result'] and len(data_dict['result']['list']) > 0:
            item = data_dict['result']['list'][0]
            if price_or_qty == 'price':
                # 提取指定的价格字段
                prices = [item.get('highPrice24h', ''), item.get('lowPrice24h', ''), item.get('prevPrice1h', ''),
                    item.get('bid1Price', ''), item.get('ask1Price', '')]
                return self.calculate_size_and_step(prices)
            else:
                qty = [item.get('volume24h', ''), item.get('ask1Size', ''), item.get('bid1Size', '')]
                return self.calculate_size_and_step(qty)

    async def adjust_price_to_tick(self, category: str, symbol: str, price: float) -> float:
        if category == "spot":
            for item in self.spot_exchange_info["result"]["list"]:
                if item["symbol"] == symbol:
                    # 获取价格精度：tickSize
                    tick_size = Decimal(item["priceFilter"]["tickSize"])
                    price_decimal = Decimal(str(price))
                    # 使用整除来调整价格到最接近的精度
                    adjusted_price = (price_decimal // tick_size) * tick_size
                    return float(adjusted_price.quantize(tick_size, rounding=ROUND_DOWN))
        if category == "linear":
            for item in self.linear_exchange_info["result"]["list"]:
                if item["symbol"] == symbol:
                    tick_size = Decimal(item["priceFilter"]["tickSize"])
                    price_decimal = Decimal(str(price))
                    adjusted_price = (price_decimal // tick_size) * tick_size
                    return float(adjusted_price.quantize(tick_size, rounding=ROUND_DOWN))
        last_tick = await self.last_price(category, symbol)
        tick_size = Decimal(self.analyze_bybit_data(last_tick, 'price'))
        price_decimal = Decimal(str(price))
        adjusted_price = (price_decimal // tick_size) * tick_size
        return float(adjusted_price.quantize(tick_size, rounding=ROUND_DOWN))

    async def adjust_qty_to_step(self, category: str, symbol: str, qty: float) -> float:
        if category == "spot":
            for item in self.spot_exchange_info["result"]["list"]:
                if item["symbol"] == symbol:
                    # 获取数量精度：basePrecision
                    qty_step = Decimal(item["lotSizeFilter"]["basePrecision"])
                    qty_decimal = Decimal(str(qty))
                    # 计算调整后的数量
                    adjusted_qty = (qty_decimal // qty_step) * qty_step
                    return float(adjusted_qty.quantize(qty_step, rounding=ROUND_DOWN))
        if category == "linear":
            for item in self.linear_exchange_info["result"]["list"]:
                if item["symbol"] == symbol:
                    qty_step = Decimal(item["lotSizeFilter"]["qtyStep"])
                    qty_decimal = Decimal(str(qty))
                    adjusted_qty = (qty_decimal // qty_step) * qty_step
                    return float(adjusted_qty.quantize(qty_step, rounding=ROUND_DOWN))
        last_tick = await self.last_price(category, symbol)
        tick_size = Decimal(self.analyze_bybit_data(last_tick, 'qty'))
        qty_decimal = Decimal(str(qty))
        adjusted_price = (qty_decimal // tick_size) * tick_size
        return float(adjusted_price.quantize(tick_size, rounding=ROUND_DOWN))

