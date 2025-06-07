import json
import logging
from collections import defaultdict
import asyncio
import websockets
from apis.hyperliquid.hyperliquid_utils.constants import MAINNET_API_URL
from apis.hyperliquid.hyperliquid_utils.types import Any, Callable, NamedTuple, Optional, Subscription


class ActiveSubscription(NamedTuple):
    callback: Callable[[Any], None]
    subscription_id: int


class HyperliquidWebsocketManager:
    def __init__(self):
        self.ws_url = "wss://" + MAINNET_API_URL.removeprefix("https://").removeprefix("http://") + "/ws"
        self.subscription_id_counter = 0
        self.ws_ready = asyncio.Event()
        self.queued_subscriptions = []
        self.active_subscriptions = defaultdict(list)
        self.stop_event = asyncio.Event()
        self.ws = None  # type: Optional[websockets.WebSocketClientProtocol]

    async def connect(self):
        while not self.stop_event.is_set():
            self.ws_ready.clear()  # 连接断开或未建立时，标志未就绪
            self.ws = None
            try:
                async with websockets.connect(self.ws_url) as websocket:
                    self.ws = websocket
                    self.ws_ready.set()  # 连接并认证成功后设置就绪标志
                    if self.queued_subscriptions:
                        print(
                            f"[{asyncio.current_task().get_name()}] 处理 {len(self.queued_subscriptions)} 个排队订阅...")
                        queued_subs = self.queued_subscriptions  # 复制一份，处理时清空原列表
                        self.queued_subscriptions = []
                        for subscription, active_subscription in queued_subs:
                            # 重新订阅，需要确保 subscribe 方法能处理重连后的情况
                            await self.subscribe(subscription, active_subscription.callback,
                                                 active_subscription.subscription_id)
                        print(f"[{asyncio.current_task().get_name()}] 排队订阅处理完毕。")
                    ping_task = asyncio.create_task(self.send_ping())

                    # --- 核心消息接收循环 ---
                    try:
                        async for message in websocket:
                            await self.on_message(message)
                    except websockets.exceptions.ConnectionClosed as e:
                        logging.warning(f"[{asyncio.current_task().get_name()}] WebSocket connection closed: {e}")
                    except Exception as e:
                        logging.error(f"[{asyncio.current_task().get_name()}] WebSocket 消息处理异常: {e}",
                                      exc_info=True)

                    finally:
                        # 连接断开或异常，进行清理
                        print(f"[{asyncio.current_task().get_name()}] Hyperliquid WebSocket 连接关闭，进行清理...")
                        if ping_task and not ping_task.done():
                            ping_task.cancel()
                        await ping_task  # 等待 ping 任务取消/结束
                        self.ws_ready.clear()  # 连接不再就绪
                        self.ws = None
                        # 清理 active_subscriptions 状态 (如果需要)
                        # active_subscriptions 可能需要持久化并在重连后恢复

            except (websockets.exceptions.ConnectionClosed, websockets.exceptions.InvalidStatusCode,
                    ConnectionRefusedError) as e:
                logging.error(f"[{asyncio.current_task().get_name()}] Hyperliquid 连接被拒绝: {e}")
            except Exception as e:
                logging.error(f"[{asyncio.current_task().get_name()}] Hyperliquid 连接或处理异常: {e}", exc_info=True)

            # --- 重连延迟 ---
            if not self.stop_event.is_set():
                print(f"[{asyncio.current_task().get_name()}] 等待 5 秒后尝试重连...")
                await asyncio.sleep(5)  # 等待一段时间再重连

        print(f"[{asyncio.current_task().get_name()}] Hyperliquid 连接管理协程停止。")

    async def send_ping(self):
        try:
            while not self.stop_event.is_set():
                await self.ws_ready.wait()  # 等待连接建立
                await asyncio.sleep(50)
                if self.ws is None:
                    continue  # double check，仅防御性
                logging.debug("Websocket sending ping")
                await self.ws.send(json.dumps({"method": "ping"}))
        except asyncio.CancelledError:
            logging.debug("Ping task cancelled")
        finally:
            logging.debug("Websocket ping sender stopped")

    async def stop(self):
        self.stop_event.set()
        if self.ws:
            await self.ws.close()

    async def on_message(self, message: str):
        if message == "Websocket connection established.":
            logging.debug(message)
            return
        logging.debug(f"on_message {message}")
        ws_msg = json.loads(message)
        identifier = ws_msg_to_identifier(ws_msg)
        if identifier == "pong":
            logging.debug("Websocket received pong")
            return
        if identifier is None:
            logging.debug("Websocket not handling empty message")
            return
        for active_subscription in self.active_subscriptions[identifier]:
            cb = active_subscription.callback
            if asyncio.iscoroutinefunction(cb):
                await cb(ws_msg)
            else:
                cb(ws_msg)

    def on_open(self, _ws):
        logging.debug("on_open")
        self.ws_ready = True
        for subscription, active_subscription in self.queued_subscriptions:
            self.subscribe(subscription, active_subscription.callback, active_subscription.subscription_id)

    async def subscribe(self, subscription, callback, subscription_id=None) -> int:
        if subscription_id is None:
            self.subscription_id_counter += 1
            subscription_id = self.subscription_id_counter
        await self.ws_ready.wait()
        identifier = subscription_to_identifier(subscription)
        if identifier in {"userEvents", "orderUpdates"} and self.active_subscriptions[identifier]:
            raise NotImplementedError(f"Cannot subscribe to {identifier} multiple times")

        self.active_subscriptions[identifier].append(ActiveSubscription(callback, subscription_id))
        await self.ws.send(json.dumps({
            "method": "subscribe",
            "subscription": subscription
        }))
        return subscription_id

    async def unsubscribe(self, subscription: Subscription, subscription_id: int) -> bool:
        identifier = subscription_to_identifier(subscription)
        subs = self.active_subscriptions[identifier]
        new_subs = [s for s in subs if s.subscription_id != subscription_id]
        self.active_subscriptions[identifier] = new_subs
        if not new_subs:
            await self.ws.send(json.dumps({"method": "unsubscribe", "subscription": subscription}))
        return len(subs) != len(new_subs)


def subscription_to_identifier(subscription: Subscription) -> str:
    if subscription["type"] == "allMids":
        return "allMids"
    elif subscription["type"] == "l2Book":
        return f'l2Book:{subscription["coin"].lower()}'
    elif subscription["type"] == "trades":
        return f'trades:{subscription["coin"].lower()}'
    elif subscription["type"] == "userEvents":
        return "userEvents"
    elif subscription["type"] == "userFills":
        return f'userFills:{subscription["user"].lower()}'
    elif subscription["type"] == "candle":
        return f'candle:{subscription["coin"].lower()},{subscription["interval"]}'
    elif subscription["type"] == "orderUpdates":
        return "orderUpdates"
    elif subscription["type"] == "userFundings":
        return f'userFundings:{subscription["user"].lower()}'
    elif subscription["type"] == "activeAssetCtx":
        return f'activeAssetCtx:{subscription["coin"].lower()}'
    elif subscription["type"] == "userNonFundingLedgerUpdates":
        return f'userNonFundingLedgerUpdates:{subscription["user"].lower()}'
    elif subscription["type"] == "webData2":
        return f'webData2:{subscription["user"].lower()}'


def ws_msg_to_identifier(ws_msg: dict) -> str:
    ch = ws_msg.get("channel")
    if ch == "pong":
        return "pong"
    elif ch == "allMids":
        return "allMids"
    elif ch == "l2Book":
        return f'l2Book:{ws_msg["data"]["coin"].lower()}'
    elif ch == "trades":
        trades = ws_msg["data"]
        if not trades:
            raise ValueError("Trades message is empty")
        return f'trades:{trades[0]["coin"].lower()}'
    elif ch == "user":
        return "userEvents"
    elif ch == "userFills":
        return f'userFills:{ws_msg["data"]["user"].lower()}'
    elif ch == "candle":
        return f'candle:{ws_msg["data"]["s"].lower()},{ws_msg["data"]["i"]}'
    elif ch == "orderUpdates":
        return "orderUpdates"
    elif ch == "userFundings":
        return f'userFundings:{ws_msg["data"]["user"].lower()}'
    elif ch == "userNonFundingLedgerUpdates":
        return f'userNonFundingLedgerUpdates:{ws_msg["data"]["user"].lower()}'
    elif ch == "webData2":
        return f'webData2:{ws_msg["data"]["user"].lower()}'
    elif ch == "activeAssetCtx":
        return f'activeAssetCtx:{ws_msg["data"]["coin"].lower()}'
    elif ch == "subscriptionResponse":
        # 处理订阅确认消息
        subscription_type = ws_msg.get("data", {}).get("subscription", {}).get("type", "unknown")
        coin = ws_msg.get("data", {}).get("subscription", {}).get("coin", "unknown")
        return f'subscriptionResponse:{subscription_type}:{coin}'
    # 如果没有匹配，抛出 ValueError
    raise ValueError(f"Unknown WebSocket message type: {ch} - Message: {ws_msg}")
