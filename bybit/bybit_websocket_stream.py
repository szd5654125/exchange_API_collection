import asyncio
import hmac
import time

import websocket
import json
import websockets
from pybit._http_manager import generate_signature
import logging
import copy
from uuid import uuid4


logger = logging.getLogger(__name__)

SUBDOMAIN_TESTNET = "stream-testnet"
SUBDOMAIN_MAINNET = "stream"
DEMO_SUBDOMAIN_TESTNET = "stream-demo-testnet"
DEMO_SUBDOMAIN_MAINNET = "stream-demo"
DOMAIN_MAIN = "bybit"
DOMAIN_ALT = "bytick"
TLD_MAIN = "com"


class V5BybitWebSocketManager:
    def __init__(self, callback_function=None, ws_name='main', testnet=False, market_type="linear",
                 api_key=None, api_secret=None, ping_interval=60, ping_timeout=10,
                 retries=10, restart_on_error=True, trace_logging=False,
                 private_auth_expire=30, rsa_authentication=False):
        """
        统一的Bybit WebSocket客户端，支持V5 API

        Args:
            callback_function: 处理消息的回调函数，如果为None则使用内部处理函数
            ws_name: WebSocket连接名称，用于日志
            testnet: 是否使用测试网
            market_type: 市场类型 (linear, spot, inverse等)
            api_key: API密钥，用于私有主题订阅
            api_secret: API密钥对应的密文
            ping_interval: 心跳包发送间隔(秒)
            ping_timeout: 心跳超时时间(秒)
            retries: 重连尝试次数
            restart_on_error: 发生错误时是否自动重连
            trace_logging: 是否启用WebSocket跟踪日志
            private_auth_expire: 认证过期时间(秒)
            rsa_authentication: 是否使用RSA认证
        """
        self.testnet = testnet
        self.rsa_authentication = rsa_authentication
        self.api_key = api_key
        self.api_secret = api_secret
        self.callback = callback_function if callback_function else self._handle_incoming_message
        self.ws_name = ws_name
        if api_key:
            self.ws_name += " (Auth)"
        self.private_auth_expire = private_auth_expire
        self.callback_directory = {}
        self.subscriptions = {}
        self.ping_interval = ping_interval
        self.ping_timeout = ping_timeout
        self.custom_ping_message = json.dumps({"op": "ping"})
        self.custom_pong_message = json.dumps({"op": "pong"})
        self.retries = retries
        self.handle_error = restart_on_error
        self.data = {}  # 用于存储数据快照
        self.ws = None

        # 设置WebSocket跟踪日志
        websocket.enableTrace(trace_logging)

        # 初始化状态
        self._reset()

        # 设置异步事件
        self.attempting_connection = False
        self._connected_event = asyncio.Event()

        # 设置标准私有主题列表
        self.standard_private_topics = [
            "position",
            "execution",
            "order",
            "wallet",
            "greeks",
        ]

        # 其他私有主题
        self.other_private_topics = [
            "execution.fast"
        ]

        # 动态生成 URL
        base = 'wss://stream-testnet.bybit.com/v5' if self.testnet else 'wss://stream.bybit.com/v5'
        if self.api_key:
            # 私有流（不要再加 /linear 或 /spot 等后缀）
            self.url = f"{base}/private"
        else:
            # 公有流，仍需市场类型后缀
            self.url = f"{base}/public/{market_type}"

    def _reset(self):
        """
        重置WebSocket状态
        """
        self.exited = False
        self.auth = False
        self.data = {}
        self.ws = None  # 每次重连时初始化 WebSocket 对象

    async def connect(self):
        """
        建立WebSocket连接并启动消息循环
        """
        self.attempting_connection = True
        retries = self.retries
        while retries > 0 and not self.exited:
            try:
                async with websockets.connect(self.url) as ws:
                    self.ws = ws
                    # 公共连接直接设置连接事件
                    self._connected_event.set()
                    self.attempting_connection = False
                    # 发送初始心跳包
                    await self._send_initial_ping()
                    # 启动心跳任务
                    heartbeat_task = asyncio.create_task(self._send_heartbeat())
                    # 消息处理循环
                    async for message in ws:
                        await self._on_message(message)
                    # 如果循环退出，表示连接已关闭
                    print(f"[连接关闭] WebSocket连接已关闭")
                    self._connected_event.clear()
                    # 取消心跳任务
                    heartbeat_task.cancel()
            except Exception as e:
                print(f"[连接异常] 发生错误: {e}")
                import traceback
                traceback.print_exc()
                retries -= 1
                self._connected_event.clear()
                if self.exited:
                    break
                # 等待一段时间后重试
                await asyncio.sleep(self.ping_timeout)

        self.attempting_connection = False

        if retries <= 0:
            print(f"[连接失败] 达到最大重试次数，无法连接到 {self.url}")
            return False

        return True

    async def _auth(self):
        # 生成认证参数
        expires = int(time.time() * 1000) + (self.private_auth_expire * 1000)
        param_str = f"GET/realtime{expires}"
        # 生成签名
        if self.rsa_authentication:
            # RSA认证方式
            signature = generate_signature(True, self.api_secret, param_str)
        else:
            # HMAC认证方式
            signature = hmac.new(
                self.api_secret.encode(),
                param_str.encode(),
                digestmod="sha256"
            ).hexdigest()
        # 构造认证消息
        auth_message = json.dumps({"op": "auth", "args": [self.api_key, expires, signature]})
        await self.ws.send(auth_message)

    async def _on_message(self, message):
        try:
            message_data = json.loads(message)
            # 处理 ping/pong
            if self._is_custom_pong(message_data):
                return
            if self._is_custom_ping(message_data):
                print(f"[Ping请求] 收到ping消息: {message_data}，准备发送pong")
                await self._send_custom_pong()
                print("[Pong发送] pong响应已发送")
                return
            # 处理认证响应
            if message_data.get("op") == "auth":
                success = message_data.get("success", False)
                if success:
                    self.auth = True
                    self._connected_event.set()
                else:
                    print(f"[认证失败] 原因: {message_data.get('ret_msg')}")
                    await self.exit()
                    raise Exception(f"认证失败: {message_data.get('ret_msg')}")
                return
            # 处理订阅响应
            if message_data.get("op") == "subscribe":
                req_id = message_data.get("req_id", "")
                success = message_data.get("success", False)
                if not success:
                    print(f"[订阅失败] 原因: {message_data.get('ret_msg')}")
                return
            # 有 topic 的情况（标准数据）
            topic = message_data.get("topic")
            if topic:
                self._process_normal_message(message_data)
                # 查找是否有 callback 注册
                callback = self.callback_directory.get(topic)
                if callback and callable(callback):
                    await callback(message_data)
                else:
                    print(f"[警告] 未找到有效回调或回调不可调用: topic={topic}, callback={callback}")
                return
        except json.JSONDecodeError as e:
            print(f"[JSON解析错误] 消息不是有效的JSON格式: {e}")
        except Exception as e:
            print(f"[消息处理异常] 处理消息时出错: {e}")
            import traceback
            traceback.print_exc()

    async def subscribe(self, topic: str, callback, symbol: (str, list) = False):
        print(f"[订阅开始] 尝试订阅主题: {topic}, 符号: {symbol}, 对象ID: {id(self)}")
        # 确保WebSocket已连接
        if not self.is_connected():
            print("[连接检查] WebSocket未连接，尝试建立连接...")

            if self.attempting_connection:
                print("[连接等待] 连接正在进行中，等待完成...")
                try:
                    await asyncio.wait_for(self._connected_event.wait(), timeout=20)
                except asyncio.TimeoutError:
                    print("[连接超时] 等待连接超时")
                    return False
            else:
                connection_success = await self.connect()
                if not connection_success:
                    print("[连接失败] 无法建立WebSocket连接")
                    return False
        if self.api_key:
            await self._auth()
        # 准备订阅参数
        subscription_args = self._prepare_subscription_args(topic, symbol)
        if not subscription_args:
            print("[订阅错误] 无有效的订阅参数")
            return False
        # 检查回调是否已注册
        self._check_callback_directory(subscription_args)
        # 生成唯一请求ID
        req_id = str(uuid4())
        # 构造订阅消息
        subscription_message = json.dumps({"op": "subscribe", "req_id": req_id, "args": subscription_args})
        # 检查连接状态
        if not self.is_connected():
            print("[连接检查] WebSocket未连接或已关闭，无法发送订阅请求")
            return False
        # 确保已认证(私有主题)
        if topic in self.standard_private_topics and not self._connected_event.is_set():
            print("[认证检查] 等待认证完成...")
            try:
                await asyncio.wait_for(self._connected_event.wait(), timeout=20)
                print("[认证完成] 认证已成功完成")
            except asyncio.TimeoutError:
                print("[认证超时] 等待认证超时，订阅失败")
                return False
        # 发送订阅请求
        try:
            await self.ws.send(subscription_message)
        except Exception as e:
            print(f"[订阅错误] WebSocket 发送失败: {e}")
            import traceback
            traceback.print_exc()
            return False
        # 存储订阅信息
        self.subscriptions[req_id] = subscription_message
        # 注册回调前，确保 callback 是可调用对象
        if not callable(callback):
            raise TypeError(f"[订阅错误] 提供的 callback 不是可调用对象，而是 {type(callback)}")
        # 注册回调
        for formatted_topic in subscription_args:
            self._set_callback(formatted_topic, callback)
        return True

    async def unsubscribe(self, topic: str, symbol: (str, list) = False):
        """
        取消订阅WebSocket主题

        Args:
            topic: 主题名称
            symbol: 交易对名称，可以是字符串或字符串列表，对于私有主题可以为False

        Returns:
            bool: 取消订阅是否成功
        """
        # 准备取消订阅的参数
        unsubscription_args = self._prepare_subscription_args(topic, symbol)

        if not unsubscription_args:
            print("[取消订阅错误] 无有效的取消订阅参数")
            return False

        # 查找要取消的订阅
        req_id_to_remove = None
        for req_id, message in list(self.subscriptions.items()):
            message_data = json.loads(message)
            if any(arg in message_data.get("args", []) for arg in unsubscription_args):
                req_id_to_remove = req_id
                break

        if not req_id_to_remove:
            print(f"[取消订阅警告] 未找到订阅: {unsubscription_args}")
            return False

        # 构造取消订阅消息
        unsubscribe_message = json.dumps({
            "op": "unsubscribe",
            "req_id": req_id_to_remove,
            "args": unsubscription_args
        })

        # 发送取消订阅请求
        try:
            if self.is_connected():
                await self.ws.send(unsubscribe_message)
                print(f"[取消订阅] 成功发送: {unsubscription_args}")

                # 从订阅列表中移除
                self.subscriptions.pop(req_id_to_remove, None)

                # 移除回调
                for topic in unsubscription_args:
                    self.callback_directory.pop(topic, None)

                return True
            else:
                print("[取消订阅错误] WebSocket未连接，无法发送请求")
                return False
        except Exception as e:
            print(f"[取消订阅错误] WebSocket 发送失败: {e}")
            import traceback
            traceback.print_exc()
            return False

    def _prepare_subscription_args(self, topic, symbol):
        # 处理私有主题
        if topic in self.standard_private_topics or topic in self.other_private_topics:
            return [topic]
        # 检查symbol参数
        if not symbol:
            print(f"[订阅错误] 非私有主题需要symbol参数")
            return []

        # 将symbol转换为列表
        if isinstance(symbol, str):
            symbols = [symbol]
        else:
            symbols = symbol

        # 格式化主题
        formatted_topics = []
        for single_symbol in symbols:
            # 假设主题格式是 '{symbol}' 或其他需要替换的格式
            formatted_topic = topic.format(symbol=single_symbol)
            formatted_topics.append(formatted_topic)

        return formatted_topics

    async def _send_heartbeat(self):
        try:
            while not self.exited:
                try:
                    if self.is_connected():
                        await self._send_custom_ping()
                    else:
                        print("[心跳] WebSocket未连接，等待重新连接")
                    await asyncio.sleep(self.ping_interval)
                except Exception as e:
                    print(f"[心跳错误] 发送Ping失败: {e}")
                    if not self.exited:
                        print("[心跳] 尝试重新连接...")
                        await self.connect()
        except asyncio.CancelledError:
            print("[心跳] 心跳任务已取消")

    async def _send_initial_ping(self):
        """
        发送初始心跳包
        """
        await self._send_custom_ping()

    async def _send_custom_ping(self):
        """
        发送自定义ping消息
        """
        if self.is_connected():
            await self.ws.send(self.custom_ping_message)

    async def _send_custom_pong(self):
        """
        发送自定义pong消息
        """
        if self.is_connected():
            await self.ws.send(self.custom_pong_message)

    @staticmethod
    def _is_custom_ping(message):
        """
        检查消息是否为ping消息
        """
        return message.get("ret_msg") == "ping" or message.get("op") == "ping"

    @staticmethod
    def _is_custom_pong(message):
        """
        检查消息是否为pong消息
        """
        return message.get("ret_msg") == "pong" or message.get("op") == "pong"

    def is_connected(self):
        """
        检查WebSocket是否已连接
        """
        return (
                hasattr(self, 'ws') and
                self.ws is not None and
                not getattr(self.ws, 'closed', True)
        )

    async def exit(self):
        """
        关闭WebSocket连接并退出
        """
        print(f"[退出] 正在关闭WebSocket连接...")
        self.exited = True
        if self.is_connected():
            try:
                await self.ws.close()
            except Exception as e:
                print(f"[退出错误] 关闭WebSocket时出错: {e}")
        self._connected_event.clear()
        print(f"[退出完成] WebSocket连接已关闭")

    def _check_callback_directory(self, topics):
        """
        检查回调目录，防止重复订阅
        """
        for topic in topics:
            if topic in self.callback_directory:
                print(f"[订阅警告] 已存在主题订阅: {topic}，将覆盖现有回调")

    def _set_callback(self, topic, callback_function):
        """
        设置主题的回调函数
        """
        self.callback_directory[topic] = callback_function

    def _get_callback(self, topic):
        """
        获取主题的回调函数
        """
        return self.callback_directory.get(topic)

    def _initialise_local_data(self, topic):
        """
        初始化本地数据结构
        """
        if topic not in self.data:
            self.data[topic] = []

    def _process_delta_orderbook(self, message, topic):
        """
        处理订单簿增量更新
        """
        self._initialise_local_data(topic)

        # 记录初始快照
        if "snapshot" in message.get("type", ""):
            self.data[topic] = message["data"]
            return

        # 根据增量响应进行更新
        if "data" in message:
            book_data = message["data"]
            if "b" in book_data and "a" in book_data:
                book_sides = {"b": book_data["b"], "a": book_data["a"]}

                # 更新序列号
                if "u" in book_data:
                    self.data[topic]["u"] = book_data["u"]
                if "seq" in book_data:
                    self.data[topic]["seq"] = book_data["seq"]

                # 更新买卖盘
                for side, entries in book_sides.items():
                    for entry in entries:
                        if side in self.data[topic]:
                            # 删除价格水平
                            if len(entry) > 1 and float(entry[1]) == 0:
                                index = self._find_index(self.data[topic][side], entry, 0)
                                if index is not None:
                                    self.data[topic][side].pop(index)
                                continue

                            # 检查价格水平是否存在
                            price_levels = [level[0] for level in self.data[topic][side]]
                            price_level_exists = entry[0] in price_levels

                            # 插入新价格水平
                            if not price_level_exists:
                                self.data[topic][side].append(entry)
                                continue

                            # 更新现有价格水平
                            if price_level_exists and len(entry) > 1:
                                index = self._find_index(self.data[topic][side], entry, 0)
                                if index is not None:
                                    self.data[topic][side][index] = entry

    def _process_delta_ticker(self, message, topic):
        """
        处理行情增量更新
        """
        self._initialise_local_data(topic)

        # 记录初始快照
        if "snapshot" in message.get("type", ""):
            self.data[topic] = message["data"]
            return

        # 根据增量响应进行更新
        if "data" in message and "type" in message:
            if "delta" in message["type"]:
                for key, value in message["data"].items():
                    self.data[topic][key] = value

    def _process_normal_message(self, message):
        """
        处理正常的数据消息
        """
        # 确保消息有topic字段
        if "topic" not in message:
            print(f"[消息错误] 消息缺少topic字段: {message}")
            return

        topic = message["topic"]

        # 根据主题类型处理消息
        if "orderbook" in topic:
            # 处理订单簿数据
            self._process_delta_orderbook(message, topic)
            # 创建回调数据
            callback_data = copy.deepcopy(message)
            callback_data["type"] = "snapshot"
            callback_data["data"] = self.data[topic]
        elif "tickers" in topic:
            # 处理行情数据
            self._process_delta_ticker(message, topic)
            # 创建回调数据
            callback_data = copy.deepcopy(message)
            callback_data["type"] = "snapshot"
            callback_data["data"] = self.data[topic]
        else:
            # 其他类型的数据，直接传递
            callback_data = message

        # 获取并调用回调函数
        callback_function = self._get_callback(topic)
        if callback_function:
            try:
                if asyncio.iscoroutinefunction(callback_function):
                    # 如果是协程函数，创建任务执行
                    asyncio.create_task(callback_function(callback_data))
                else:
                    # 否则直接调用
                    callback_function(callback_data)
            except Exception as e:
                print(f"[回调错误] 执行回调函数出错: {e}")
                import traceback
                traceback.print_exc()
        else:
            print(f"[回调缺失] 主题 {topic} 没有注册回调函数")

    def _handle_incoming_message(self, message):
        # 检查是否为认证消息
        if message.get("op") == "auth" or message.get("type") == "AUTH_RESP":
            self._process_auth_message(message)
            return
        # 检查是否为订阅确认消息
        if message.get("op") == "subscribe" or message.get("type") == "COMMAND_RESP":
            self._process_subscription_message(message)
            return
        # 处理普通数据消息
        try:
            self._process_normal_message(message)
        except Exception as e:
            print(f"[处理错误] 处理普通消息时出错: {e}")
            import traceback
            traceback.print_exc()

    def _process_auth_message(self, message):
        """
        处理认证消息
        """
        if message.get("success") is True:
            print(f"[认证] {self.ws_name} 认证成功")
            self.auth = True
        elif message.get("success") is False or message.get("type") == "error":
            error_msg = f"[认证失败] {self.ws_name} 认证失败，请检查API密钥和系统时间: {message}"
            print(error_msg)
            raise Exception(error_msg)

    def _process_subscription_message(self, message):
        """
        处理订阅确认消息
        """
        req_id = message.get("req_id", "")
        if req_id and req_id in self.subscriptions:
            sub_message = self.subscriptions[req_id]
            sub_data = json.loads(sub_message)
            sub_topics = sub_data.get("args", [])

            if message.get("success") is True:
                print(f"[订阅成功] 主题: {sub_topics}")
            elif message.get("success") is False:
                error_msg = message.get("ret_msg", "未知错误")
                print(f"[订阅失败] 主题: {sub_topics}, 错误: {error_msg}")

                # 移除失败的订阅回调
                for topic in sub_topics:
                    self.callback_directory.pop(topic, None)

    @staticmethod
    def _find_index(array, item, key_index):
        """
        在数组中查找项目的索引
        """
        for i, entry in enumerate(array):
            if entry[key_index] == item[key_index]:
                return i
        return None
