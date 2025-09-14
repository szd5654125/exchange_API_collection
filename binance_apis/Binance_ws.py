# binance_ws.py
# Python 3.10+
import asyncio
import json
import logging
import time
import uuid
from typing import Callable, Dict, List, Optional, Set, Any

import requests
import websockets
from websockets import WebSocketClientProtocol

logger = logging.getLogger("binance.ws")
logger.setLevel(logging.INFO)


class BinanceWS:
    """
    通用 Binance WebSocket 客户端（市场流）
    - 支持：connect / disconnect / subscribe / unsubscribe / 自动重连 / 自动重订阅 / 心跳 / 限速保护（轻量）
    - 适配：Spot:    wss://stream.binance.com:9443/ws
           UM:      wss://fstream.binance.com/ws
           COIN-M:  wss://dstream.binancefuture.com/ws  （改 base_url 即可）
    - 使用在线 SUBSCRIBE/UNSUBSCRIBE（更灵活，便于动态增删流）
    """

    def __init__(
            self,
            base_url: str = "wss://stream.binance.com:9443/ws",
            ping_interval: float = 15.0,  # 建议 Spot 15s，UM 可 180s
            ping_timeout: float = 20.0,
            reconnect: bool = True,
            max_reconnect_delay: float = 30.0,
            identify: Optional[str] = None,
    ) -> None:
        self.base_url = base_url
        self.ping_interval = ping_interval
        self.ping_timeout = ping_timeout
        self.reconnect = reconnect
        self.max_reconnect_delay = max_reconnect_delay
        self.identify = identify or f"binance-ws-{uuid.uuid4().hex[:6]}"

        self._ws: Optional[WebSocketClientProtocol] = None
        self._reader_task: Optional[asyncio.Task] = None
        self._ping_task: Optional[asyncio.Task] = None
        self._reconnect_lock = asyncio.Lock()

        # 已订阅的流集合（用于断线自动重订阅）
        self._subscriptions: Set[str] = set()
        # 入站控制消息的简单限速（binance: spot 5/s、um 10/s；这里只做最小保护）
        self._last_ctrl_ts: List[float] = []

        # 回调：on_message 全量；或按流名注册回调（如 "btcusdt@bookTicker"）
        self._on_message: Optional[Callable[[dict], Any]] = None
        self._stream_handlers: Dict[str, Callable[[dict], Any]] = {}

        # 自增 id（配合 SUBSCRIBE/UNSUBSCRIBE）
        self._id_counter = 0
        self._running = False

    # ======== 公共 API ========
    def set_default_handler(self, handler: Callable[[dict], Any]) -> None:
        """设置默认消息处理回调（未匹配到特定流的消息将走这里）"""
        self._on_message = handler

    def set_stream_handler(self, stream: str, handler: Callable[[dict], Any]) -> None:
        """为特定流名设置消息回调（例：'btcusdt@bookTicker'）"""
        self._stream_handlers[stream] = handler

    def current_subscriptions(self) -> Set[str]:
        return set(self._subscriptions)

    async def connect(self) -> None:
        """建立连接并启动读循环 + 心跳"""
        if self._ws and not self._ws.closed:
            return
        self._ws = await websockets.connect(
            self.base_url,
            ping_interval=None,  # 我们自己管理 ping/pong
            max_size=8 * 1024 * 1024,
        )
        self._running = True
        # 读循环
        self._reader_task = asyncio.create_task(self._read_loop())
        # 心跳
        self._ping_task = asyncio.create_task(self._ping_loop())

        # 断线重连后自动重订阅
        if self._subscriptions:
            await self._resubscribe_all()

    async def disconnect(self) -> None:
        self._running = False
        # 先收拢要取消的任务
        tasks = []
        if getattr(self, "_reader_task", None):
            self._reader_task.cancel()
            tasks.append(self._reader_task)
            self._reader_task = None
        if getattr(self, "_ping_task", None):
            self._ping_task.cancel()
            tasks.append(self._ping_task)
            self._ping_task = None
        # ✅ 等待这些任务真正退出，吃掉 CancelledError
        for t in tasks:
            try:
                await t
            except asyncio.CancelledError:
                pass
        # ✅ 关闭 WS，并等待完全关闭
        if getattr(self, "_ws", None):
            try:
                if not self._ws.closed:
                    await self._ws.close(code=1000)
                # 有的版本需要显式等待
                if hasattr(self._ws, "wait_closed"):
                    await self._ws.wait_closed()
            except Exception:
                pass
            finally:
                self._ws = None

    async def subscribe(self, params: List[str]) -> dict:
        """在线订阅一组流（如 ['btcusdt@bookTicker','btcusdt@trade']）"""
        await self._ensure_connected()
        # 控制消息限速（非常简化；生产可换成令牌桶）
        await self._throttle_control_msg()
        self._id_counter += 1
        req = {"method": "SUBSCRIBE", "params": params, "id": self._id_counter}
        await self._ws.send(json.dumps(req))
        # 记录订阅
        self._subscriptions |= set(params)
        # 等待一次确认（可选：这里直接返回，不等待服务器回执）
        return {"sent": req}

    async def unsubscribe(self, params: List[str]) -> dict:
        """在线取消订阅"""
        await self._ensure_connected()
        await self._throttle_control_msg()
        self._id_counter += 1
        req = {"method": "UNSUBSCRIBE", "params": params, "id": self._id_counter}
        await self._ws.send(json.dumps(req))
        # 本地状态移除
        for p in params:
            self._subscriptions.discard(p)
            self._stream_handlers.pop(p, None)
        return {"sent": req}

    # ======== 内部：读循环 & 心跳 & 自动重连 ========
    async def _read_loop(self) -> None:
        try:
            assert self._ws is not None
            ws = self._ws
            async for raw in ws:
                # 诊断：原始帧
                logger.debug(f"[{self.identify}] raw frame: {raw}")
                try:
                    msg = json.loads(raw)
                except Exception:
                    logger.debug(f"[{self.identify}] non-json message: {raw!r}")
                    continue
                await self._dispatch(msg)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.warning(f"[{self.identify}] read loop error: {e}")
        finally:
            if self._running and self.reconnect:
                await self._reconnect()

    async def _ping_loop(self) -> None:
        try:
            while self._running:
                await asyncio.sleep(self.ping_interval)
                if not self._ws or self._ws.closed or getattr(self._ws, "close_code", None) is not None:
                    continue
                try:
                    # Binance 接受文本 ping；多数实现直接使用 ws.ping() 也行
                    ping_payload = str(int(time.time() * 1000))
                    await self._ws.ping(ping_payload.encode())
                    # 可选：等待 pong（websockets 会自动处理）
                except Exception as e:
                    logger.warning(f"[{self.identify}] ping error: {e}")
        except asyncio.CancelledError:
            pass

    async def _reconnect(self) -> None:
        async with self._reconnect_lock:
            if not self._running:
                return
            delay = 1.0
            while self._running:
                try:
                    await self.connect()
                    logger.info(f"[{self.identify}] reconnected")
                    return
                except Exception as e:
                    logger.warning(f"[{self.identify}] reconnect fail: {e}, retry in {delay:.1f}s")
                    await asyncio.sleep(delay)
                    delay = min(self.max_reconnect_delay, delay * 1.7)

    async def _resubscribe_all(self) -> None:
        subs = sorted(self._subscriptions)
        if not subs:
            return
        # 分批避免一次性 params 太多
        batch = 200
        for i in range(0, len(subs), batch):
            chunk = subs[i:i + batch]
            await self.subscribe(chunk)
            await asyncio.sleep(0.2)

    async def _dispatch(self, msg: dict) -> None:
        # 服务器的确认/回执（如 {"result":null,"id":1}）直接忽略或打印
        if "result" in msg and "id" in msg:
            logger.debug(f"[{self.identify}] ctrl ack: {msg}")
            return

        # 组合流消息：{"stream":"btcusdt@bookTicker","data":{...}}
        if "stream" in msg and "data" in msg:
            stream = msg.get("stream")
            data = msg.get("data")
            handler = self._stream_handlers.get(stream)
            if handler:
                await _maybe_await(handler, data)
                return
            if self._on_message:
                await _maybe_await(self._on_message, msg)
            return

        # 单流消息（/ws + SUBSCRIBE 时通常如此）：payload 本身即数据
        # 这种情况下，没有 stream 字段，无法用 _stream_handlers 精确路由
        # 直接走默认回调
        if self._on_message:
            await _maybe_await(self._on_message, msg)

    async def _ensure_connected(self) -> None:
        if not self._ws or self._ws.closed:
            await self.connect()

    async def _throttle_control_msg(self) -> None:
        # 简易：确保 200ms 间隔；你可根据 spot(5/s) 或 um(10/s) 做更严谨的窗口统计
        now = time.time()
        self._last_ctrl_ts = [t for t in self._last_ctrl_ts if now - t <= 1.0]
        limit_per_sec = 5 if "stream.binance.com" in self.base_url else 10
        if len(self._last_ctrl_ts) >= limit_per_sec:
            await asyncio.sleep(1.0 - (now - self._last_ctrl_ts[0]))
        self._last_ctrl_ts.append(time.time())


async def _maybe_await(fn: Callable, *args, **kwargs):
    res = fn(*args, **kwargs)
    if asyncio.iscoroutine(res):
        await res


# ======== 用户数据流（listenKey）模板：Futures/Delivery 等 ========
class BinanceUserDataWS(BinanceWS):
    """
    用户数据流（如 UM: wss://fstream.binance.com/ws/<listenKey>）
    - 注意：这是只读流，不支持在线 SUBSCRIBE；订阅集合忽略。
    - 你需要外部定时 keepalive listenKey（REST: PUT /fapi/v1/listenKey）。
    """

    def __init__(
            self,
            listen_key: str,
            base_url_prefix: str = "wss://fstream.binance.com/ws",
            ping_interval: float = 180.0,
            ping_timeout: float = 600.0,
            reconnect: bool = True,
            identify: Optional[str] = None,
    ) -> None:
        super().__init__(
            base_url=base_url_prefix,
            ping_interval=ping_interval,
            ping_timeout=ping_timeout,
            reconnect=reconnect,
            identify=identify or f"user-ws-{uuid.uuid4().hex[:6]}",
        )

    async def subscribe(self, params: List[str]) -> dict:
        raise NotImplementedError("UserData stream via listenKey does not support SUBSCRIBE.")

    async def unsubscribe(self, params: List[str]) -> dict:
        raise NotImplementedError("UserData stream via listenKey does not support UNSUBSCRIBE.")


import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


class ManagedUserDataWS(BinanceUserDataWS):
    """
    托管版用户数据流：
    - 自己创建/保活/关闭 listenKey
    - 自动在断线后重连 & 复用/重建 listenKey
    - 一行参数即可切 UM/COIN-M/统一账户（Portfolio Margin）
    """
    LINE_CFG = {
        "um": {  # USDT 本位合约
            "rest_base": "https://fapi.binance.com",
            "listen_ep": "/fapi/v1/listenKey",
            "ws_prefix": "wss://fstream.binance.com/ws",
            "keepalive_sec": 25 * 60,
        },
        "cm": {  # 币本位交割合约
            "rest_base": "https://dapi.binance.com",
            "listen_ep": "/dapi/v1/listenKey",
            "ws_prefix": "wss://dstream.binancefuture.com/ws",
            "keepalive_sec": 25 * 60,
        },
        "pm": {
            "rest_base": "https://papi.binance.com",
            "listen_ep": "/papi/v1/listenKey",
            "ws_prefix": "wss://fstream.binance.com/pm/ws",
            "keepalive_sec": 25 * 60,
            "builder": "path"
        },
        "pmpro": {
               "rest_base": "https://papi.binance.com",
               "listen_ep": "/papi/v1/listenKey",
               "ws_prefix": "wss://fstream.binance.com/pm-classic/ws",
               "keepalive_sec": 25*60,
               "builder": "path"
             }
    }

    def __init__(
            self,
            api_key: str,
            line: str = "um",  # "um" / "cm" / "pm"
            ping_interval: float = 180.0,
            ping_timeout: float = 600.0,
            reconnect: bool = True,
            identify: Optional[str] = None,
    ):
        cfg = self.LINE_CFG[line]
        self.api_key = api_key
        self.rest_base = cfg["rest_base"]
        self.listen_ep = cfg["listen_ep"]
        self.ws_prefix = cfg["ws_prefix"]
        self.keepalive_sec = cfg["keepalive_sec"]

        self._keep_task: Optional[asyncio.Task] = None
        self._listen_key: Optional[str] = None
        self._line = line

        # 先占位，稍后 create_listen_key 后再调用父类 __init__
        self._initialized = False
        super().__init__(
            listen_key="placeholder",
            base_url_prefix=self.ws_prefix,
            ping_interval=ping_interval,
            ping_timeout=ping_timeout,
            reconnect=reconnect,
            identify=identify or f"user-ws-{uuid.uuid4().hex[:6]}",
        )
        self._initialized = True

    # -------- listenKey 生命周期 --------
    def _headers(self) -> Dict[str, str]:
        return {"X-MBX-APIKEY": self.api_key}

    def _build_ws_url(self, lk: str) -> str:
        btype = self.LINE_CFG[self._line].get("builder", "path")
        if btype == "stream":
            # pstream 走流式
            return f"{self.ws_prefix}={lk}"
        else:
            # 常规 /ws/<lk>
            return f"{self.ws_prefix}/{lk}"

    def create_listen_key(self) -> str:
        url = f"{self.rest_base}{self.listen_ep}"
        r = requests.post(url, headers=self._headers(), timeout=10)
        if r.status_code >= 400:
            raise RuntimeError(f"listenKey create failed: {r.status_code} {r.text}")
        r.raise_for_status()
        lk = r.json()["listenKey"]
        self._listen_key = lk
        logger.info(f"[{self.identify}] listenKey created: {lk}")
        return lk

    def keepalive_listen_key(self) -> None:
        if not self._listen_key:
            return
        url = f"{self.rest_base}{self.listen_ep}"
        r = requests.put(url, headers=self._headers(), timeout=10)
        # 失效会报错（如 -1125），外层捕获并重建
        r.raise_for_status()

    def close_listen_key(self) -> None:
        if not self._listen_key:
            return
        url = f"{self.rest_base}{self.listen_ep}"
        r = requests.delete(url, headers=self._headers(), timeout=10)
        r.raise_for_status()
        self._listen_key = None

    # -------- 覆盖连接流程：先拿 lk，再连接 --------
    async def connect(self) -> None:
        if not self._listen_key:
            lk = self.create_listen_key()
            self.base_url = self._build_ws_url(lk)
        await super().connect()
        # 启动保活
        if self._keep_task is None or self._keep_task.done():
            self._keep_task = asyncio.create_task(self._keepalive_loop())

    async def disconnect(self) -> None:
        # 先停 keepalive 任务，并等待退出
        if getattr(self, "_keep_task", None):
            self._keep_task.cancel()
            try:
                await self._keep_task
            except asyncio.CancelledError:
                pass
            finally:
                self._keep_task = None

        # 再让父类去关 reader/ping/ws
        await super().disconnect()

        # （可选）关闭 listenKey（若你希望下次复用则不关）
        try:
            self.close_listen_key()
        except Exception:
            pass

    async def _reconnect(self) -> None:
        """
        断线重连时，如果 listenKey 失效则自动重建并更新 ws URL。
        """
        # 尝试保活一次；失败则重建
        try:
            self.keepalive_listen_key()
        except Exception:
            try:
                self.close_listen_key()
            except Exception:
                pass
            lk = self.create_listen_key()
            self.base_url = self._build_ws_url(lk)
        await super()._reconnect()

    async def _keepalive_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(self.keepalive_sec)
                try:
                    self.keepalive_listen_key()
                except Exception as e:
                    logger.warning(f"[{self.identify}] keepalive failed: {e}; rotating listenKey...")
                    # 直接轮换 listenKey，平滑切换
                    try:
                        self.close_listen_key()
                    except Exception:
                        pass
                    lk = self.create_listen_key()
                    self.base_url = self._build_ws_url(lk)
                    if self._ws and not self._ws.closed:
                        await self._ws.close()
        except asyncio.CancelledError:
            pass
