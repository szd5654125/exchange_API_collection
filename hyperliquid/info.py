from apis.hyperliquid.api import API
from apis.hyperliquid.hyperliquid_utils.types import (
    Any,
    Cloid,
    Meta,
    Optional,
    SpotMeta,
    SpotMetaAndAssetCtxs,
    cast,
)


class Info(API):
    def __init__(self, spot_meta: Optional[SpotMeta] = None, meta: Optional[Meta] = None,
                 perp_dexs: Optional[list[str]] = None,):
        super().__init__()
        if spot_meta is None:
            spot_meta = self.spot_meta()

        self.coin_to_asset = {}
        self.spot_name_to_asset = {}  # BTC/USDC -> @1
        self.asset_to_spot_name = {}  # @1 -> BTC/USDC
        self.asset_to_sz_decimals = {}

        # spot assets start at 10000
        for spot_info in spot_meta["universe"]:
            asset_id = spot_info["index"] + 10000
            at_name = f"@{spot_info['index']}"
            base, quote = spot_info["tokens"]
            base_info = spot_meta["tokens"][base]
            quote_info = spot_meta["tokens"][quote]
            pair_name = f"{base_info['name']}/{quote_info['name']}"
            # 建立三重映射
            self.coin_to_asset[pair_name] = asset_id
            self.coin_to_asset[at_name] = asset_id  # 兼容@xxx
            self.spot_name_to_asset[pair_name] = at_name
            self.asset_to_spot_name[at_name] = pair_name

            self.asset_to_sz_decimals[asset_id] = base_info["szDecimals"]

        perp_dex_to_offset = {"": 0}
        if perp_dexs is None:
            perp_dexs = [""]
        else:
            for i, perp_dex in enumerate(self.perp_dexs()[1:]):
                # builder-deployed perp dexs start at 110000
                perp_dex_to_offset[perp_dex["name"]] = 110000 + i * 10000

        for perp_dex in perp_dexs:
            offset = perp_dex_to_offset[perp_dex]
            if perp_dex == "" and meta is not None:
                self.set_perp_meta(meta, 0)
            else:
                fresh_meta = self.meta(dex=perp_dex)
                self.set_perp_meta(fresh_meta, offset)

    def set_perp_meta(self, meta: Meta, offset: int) -> Any:
        for asset, asset_info in enumerate(meta["universe"]):
            asset += offset
            self.coin_to_asset[asset_info["name"]] = asset
            self.asset_to_sz_decimals[asset] = asset_info["szDecimals"]

    # info.user_state("0xC5e6657C5252E7dD1DAa301bd79D9069227a45dc")
    def user_state(self, address: str) -> Any:
        """Retrieve trading details about a user.
        POST /info
        Args:
            address (str): Onchain address in 42-character hexadecimal format;
                            e.g. 0x0000000000000000000000000000000000000000.
        Returns:
            {
                assetPositions: [
                    {
                        position: {
                            coin: str,
                            entryPx: Optional[float string]
                            leverage: {
                                type: "cross" | "isolated",
                                value: int,
                                rawUsd: float string  # only if type is "isolated"
                            },
                            liquidationPx: Optional[float string]
                            marginUsed: float string,
                            positionValue: float string,
                            returnOnEquity: float string,
                            szi: float string,
                            unrealizedPnl: float string
                        },
                        type: "oneWay"
                    }
                ],
                crossMarginSummary: MarginSummary,
                marginSummary: MarginSummary,
                withdrawable: float string,
            }

            where MarginSummary is {
                    accountValue: float string,
                    totalMarginUsed: float string,
                    totalNtlPos: float string,
                    totalRawUsd: float string,
                }
        """
        return self.post("/info", {"type": "clearinghouseState", "user": address})

    def get_withdrawable_usdc(self, address) -> float:
        """
        获取当前账户的可提取 USDC 余额（withdrawable 字段）。

        :return: 可提取的 USDC 数量（float）
        """
        state = self.user_state(address)
        return float(state.get("withdrawable", 0.0))

    def get_liquidation_price(self, address: str, symbol: str) -> float | None:

        state = self.user_state(address)
        positions = state.get("assetPositions", [])
        for pos in positions:
            position = pos.get("position", {})
            if position.get("coin") == symbol:
                liquidation_px = position.get("liquidationPx")
                return float(liquidation_px) if liquidation_px else None
        return None  # 没有该币种的持仓

    def spot_user_state(self, address: str) -> Any:
        return self.post("/info", {"type": "spotClearinghouseState", "user": address})

    # check current limit order
    def open_orders(self, address: str) -> Any:
        """Retrieve a user's open orders.

        POST /info

        Args:
            address (str): Onchain address in 42-character hexadecimal format;
                            e.g. 0x0000000000000000000000000000000000000000.
        Returns: [
            {
                coin: str,
                limitPx: float string,
                oid: int,
                side: "A" | "B",
                sz: float string,
                timestamp: int
            }
        ]
        """
        return self.post("/info", {"type": "openOrders", "user": address})

    # info.frontend_open_orders('0xC5e6657C5252E7dD1DAa301bd79D9069227a45dc')
    def frontend_open_orders(self, address: str) -> Any:
        """Retrieve a user's open orders with additional frontend info.

        POST /info

        Args:
            address (str): Onchain address in 42-character hexadecimal format;
                            e.g. 0x0000000000000000000000000000000000000000.
        Returns: [
            {
                children:
                    [
                        dict of frontend orders
                    ]
                coin: str,
                isPositionTpsl: bool,
                isTrigger: bool,
                limitPx: float string,
                oid: int,
                orderType: str,
                origSz: float string,
                reduceOnly: bool,
                side: "A" | "B",
                sz: float string,
                tif: str,
                timestamp: int,
                triggerCondition: str,
                triggerPx: float str
            }
        ]
        """
        return self.post("/info", {"type": "frontendOpenOrders", "user": address})

    def all_mids(self) -> Any:
        """Retrieve all mids for all actively traded coins.

        POST /info

        Returns:
            {
              ATOM: float string,
              BTC: float string,
              any other coins which are trading: float string
            }
        """
        return self.post("/info", {"type": "allMids"})

    def get_price(self, symbol: str, all_mids: dict) -> float:
        """
        symbol: 合约如 'BTC'，现货如 'BTC/USDC'。只要初始化时区分好即可自动识别。
        """
        asset_id = self.coin_to_asset[symbol]

        # 现货的asset_id >= 10000
        if asset_id >= 10000:
            # 假设all_mids用的是@index编号
            at_name = f"@{asset_id - 10000}"  # 因为初始化时现货asset_id = index+10000，@index正好是@{asset_id-10000}
            if at_name in all_mids:
                return float(all_mids[at_name])
            raise KeyError(f"现货 {symbol} (all_mids: {at_name}) 不在 all_mids！")

        # 合约asset_id < 10000
        else:
            # 先试symbol，再试@asset_id
            if symbol in all_mids:
                return float(all_mids[symbol])
            key2 = f"@{asset_id}"
            if key2 in all_mids:
                return float(all_mids[key2])
            raise KeyError(f"合约 {symbol} (all_mids: {symbol}/{key2}) 不在 all_mids！")

    # get history filled oeder
    def user_fills(self, address: str) -> Any:
        """Retrieve a given user's fills.

        POST /info

        Args:
            address (str): Onchain address in 42-character hexadecimal format;
                            e.g. 0x0000000000000000000000000000000000000000.

        Returns:
            [
              {
                closedPnl: float string,
                coin: str,
                crossed: bool,
                dir: str,
                hash: str,
                oid: int,
                px: float string,
                side: str,
                startPosition: float string,
                sz: float string,
                time: int
              },
              ...
            ]
        """
        return self.post("/info", {"type": "userFills", "user": address})

    def user_fills_by_time(self, address: str, start_time: int, end_time: Optional[int] = None) -> Any:
        """Retrieve a given user's fills by time.

        POST /info

        Args:
            address (str): Onchain address in 42-character hexadecimal format;
                            e.g. 0x0000000000000000000000000000000000000000.
            start_time (int): Unix timestamp in milliseconds
            end_time (Optional[int]): Unix timestamp in milliseconds

        Returns:
            [
              {
                closedPnl: float string,
                coin: str,
                crossed: bool,
                dir: str,
                hash: str,
                oid: int,
                px: float string,
                side: str,
                startPosition: float string,
                sz: float string,
                time: int
              },
              ...
            ]
        """
        return self.post(
            "/info", {"type": "userFillsByTime", "user": address, "startTime": start_time, "endTime": end_time}
        )

    # 获取合约元数据
    def meta(self, dex: str = "") -> Meta:
        """Retrieve exchange perp metadata

        POST /info

        Returns:
            {
                universe: [
                    {
                        name: str,
                        szDecimals: int
                    },
                    ...
                ]
            }
        """
        return cast(Meta, self.post("/info", {"type": "meta", "dex": dex}))

    # 获取永续合约资产的相关信息（包括标记价格、当前资金费率、未平仓量等）
    def meta_and_asset_ctxs(self) -> Any:
        """Retrieve exchange MetaAndAssetCtxs

        POST /info

        Returns:
            [
                {
                    universe: [
                        {
                            'name': str,
                            'szDecimals': int
                            'maxLeverage': int,
                            'onlyIsolated': bool,
                        },
                        ...
                    ]
                },
            [
                {
                    "dayNtlVlm": float string,
                    "funding": float string,
                    "impactPxs": Optional([float string, float string]),
                    "markPx": Optional(float string),
                    "midPx": Optional(float string),
                    "openInterest": float string,
                    "oraclePx": float string,
                    "premium": Optional(float string),
                    "prevDayPx": float string
                },
                ...
            ]
        """
        return self.post("/info", {"type": "metaAndAssetCtxs"})

    def perp_dexs(self) -> Any:
        return self.post("/info", {"type": "perpDexs"})

    def spot_meta(self) -> SpotMeta:
        """Retrieve exchange spot metadata

        POST /info

        Returns:
            {
                universe: [
                    {
                        tokens: [int, int],
                        name: str,
                        index: int,
                        isCanonical: bool
                    },
                    ...
                ],
                tokens: [
                    {
                        name: str,
                        szDecimals: int,
                        weiDecimals: int,
                        index: int,
                        tokenId: str,
                        isCanonical: bool
                    },
                    ...
                ]
            }
        """
        return cast(SpotMeta, self.post("/info", {"type": "spotMeta"}))

    def spot_meta_and_asset_ctxs(self) -> SpotMetaAndAssetCtxs:
        """Retrieve exchange spot asset contexts
        POST /info
        Returns:
            [
                {
                    universe: [
                        {
                            tokens: [int, int],
                            name: str,
                            index: int,
                            isCanonical: bool
                        },
                        ...
                    ],
                    tokens: [
                        {
                            name: str,
                            szDecimals: int,
                            weiDecimals: int,
                            index: int,
                            tokenId: str,
                            isCanonical: bool
                        },
                        ...
                    ]
                },
                [
                    {
                        dayNtlVlm: float string,
                        markPx: float string,
                        midPx: Optional(float string),
                        prevDayPx: float string,
                        circulatingSupply: float string,
                        coin: str
                    }
                    ...
                ]
            ]
        """
        return cast(SpotMetaAndAssetCtxs, self.post("/info", {"type": "spotMetaAndAssetCtxs"}))

    # info.funding_history('BTC', 1742481620000)
    def funding_history(self, name: str, startTime: int, endTime: Optional[int] = None) -> Any:
        """Retrieve funding history for a given coin

        POST /info

        Args:
            name (str): Coin to retrieve funding history for.
            startTime (int): Unix timestamp in milliseconds.
            endTime (int): Unix timestamp in milliseconds.

        Returns:
            [
                {
                    coin: str,
                    fundingRate: float string,
                    premium: float string,
                    time: int
                },
                ...
            ]
        """
        if endTime is not None:
            return self.post(
                "/info", {"type": "fundingHistory", "coin": name, "startTime": startTime, "endTime": endTime}
            )
        return self.post("/info", {"type": "fundingHistory", "coin": name, "startTime": startTime})

    # 获取不同交易场所的预测资金费率（hyperliquid, binance, bybit）
    def predicted_fundings(self):
        """
        获取不同交易场所的预测资金费率
        """
        return self.post("/info", {"type": "predictedFundings"})

    # Retrieve funding payment records for a specific user within a time range.
    def user_funding_history(self, user: str, startTime: int, endTime: Optional[int] = None) -> Any:
        """Retrieve a user's funding history
        POST /info
        Args:
            user (str): Address of the user in 42-character hexadecimal format.
            startTime (int): Start time in milliseconds, inclusive.
            endTime (int, optional): End time in milliseconds, inclusive. Defaults to current time.
        Returns:
            List[Dict]: A list of funding history records, where each record contains:
                - user (str): User address.
                - type (str): Type of the record, e.g., "userFunding".
                - startTime (int): Unix timestamp of the start time in milliseconds.
                - endTime (int): Unix timestamp of the end time in milliseconds.
        """
        if endTime is not None:
            return self.post("/info", {"type": "userFunding", "user": user, "startTime": startTime, "endTime": endTime})
        return self.post("/info", {"type": "userFunding", "user": user, "startTime": startTime})

    # Return the full level 2 order book snapshot for a given coin (bids and asks).
    def l2_snapshot(self, name: str) -> Any:
        """Retrieve L2 snapshot for a given coin
        POST /info
        Args:
            name (str): Coin to retrieve L2 snapshot for.
        Returns:
            {
                coin: str,
                levels: [
                    [
                        {
                            n: int,
                            px: float string,
                            sz: float string
                        },
                        ...
                    ],
                    ...
                ],
                time: int
            }
        """
        return self.post("/info", {"type": "l2Book", "coin": name})

    # info.parse_l2_snapshot('PNUT')
    def parse_l2_snapshot(self, symbol):
        levels = self.l2_snapshot(symbol).get("levels", [[], []])
        # 买一价格 (Bid)
        bid_list = levels[0]
        best_bid = float(bid_list[0]['px']) if bid_list else None
        best_bid_size = float(bid_list[0]['sz']) if bid_list else None
        # 卖一价格 (Ask)
        ask_list = levels[1]
        best_ask = float(ask_list[0]['px']) if ask_list else None
        best_ask_size = float(ask_list[0]['sz']) if ask_list else None

        return best_bid, best_ask

    # info.candles_snapshot('BTC', '15m', 1742660973000, 1742660982000)
    def candles_snapshot(self, name: str, interval: str, startTime: int, endTime: int) -> Any:
        """Retrieve candles snapshot for a given coin

        POST /info

        Args:
            name (str): Coin to retrieve candles snapshot for.
            interval (str): Candlestick interval.
            startTime (int): Unix timestamp in milliseconds.
            endTime (int): Unix timestamp in milliseconds.

        Returns:
            [
                {
                    T: int,
                    c: float string,
                    h: float string,
                    i: str,
                    l: float string,
                    n: int,
                    o: float string,
                    s: string,
                    t: int,
                    v: float string
                },
                ...
            ]
        """
        req = {"coin": name, "interval": interval, "startTime": startTime, "endTime": endTime}
        return self.post("/info", {"type": "candleSnapshot", "req": req})

    # get current fee level
    def user_fees(self, address: str) -> Any:
        """Retrieve the volume of trading activity associated with a user.
        POST /info
        Args:
            address (str): Onchain address in 42-character hexadecimal format;
                            e.g. 0x0000000000000000000000000000000000000000.
        Returns:
            {
                activeReferralDiscount: float string,
                dailyUserVlm: [
                    {
                        date: str,
                        exchange: str,
                        userAdd: float string,
                        userCross: float string
                    },
                ],
                feeSchedule: {
                    add: float string,
                    cross: float string,
                    referralDiscount: float string,
                    tiers: {
                        mm: [
                            {
                                add: float string,
                                makerFractionCutoff: float string
                            },
                        ],
                        vip: [
                            {
                                add: float string,
                                cross: float string,
                                ntlCutoff: float string
                            },
                        ]
                    }
                },
                userAddRate: float string,
                userCrossRate: float string
            }
        """
        return self.post("/info", {"type": "userFees", "user": address})

    def user_staking_summary(self, address: str) -> Any:
        """Retrieve the staking summary associated with a user.
        POST /info
        Args:
            address (str): Onchain address in 42-character hexadecimal format;
                            e.g. 0x0000000000000000000000000000000000000000.
        Returns:
            {
                delegated: float string,
                undelegated: float string,
                totalPendingWithdrawal: float string,
                nPendingWithdrawals: int
            }
        """
        return self.post("/info", {"type": "delegatorSummary", "user": address})

    def user_staking_delegations(self, address: str) -> Any:
        """Retrieve the user's staking delegations.
        POST /info
        Args:
            address (str): Onchain address in 42-character hexadecimal format;
                            e.g. 0x0000000000000000000000000000000000000000.
        Returns:
            [
                {
                    validator: string,
                    amount: float string,
                    lockedUntilTimestamp: int
                },
            ]
        """
        return self.post("/info", {"type": "delegations", "user": address})

    def user_staking_rewards(self, address: str) -> Any:
        """Retrieve the historic staking rewards associated with a user.
        POST /info
        Args:
            address (str): Onchain address in 42-character hexadecimal format;
                            e.g. 0x0000000000000000000000000000000000000000.
        Returns:
            [
                {
                    time: int,
                    source: string,
                    totalAmount: float string
                },
            ]
        """
        return self.post("/info", {"type": "delegatorRewards", "user": address})

    # check order status with oid
    # info.query_order_by_oid('0xC5e6657C5252E7dD1DAa301bd79D9069227a45dc',81277564134)
    def query_order_by_oid(self, user: str, oid: int) -> Any:
        return self.post("/info", {"type": "orderStatus", "user": user, "oid": oid})

    # check order status with Cloid
    def query_order_by_cloid(self, user: str, cloid: Cloid) -> Any:
        return self.post("/info", {"type": "orderStatus", "user": user, "oid": cloid.to_raw()})

    def query_referral_state(self, user: str) -> Any:
        return self.post("/info", {"type": "referral", "user": user})

    def query_sub_accounts(self, user: str) -> Any:
        return self.post("/info", {"type": "subAccounts", "user": user})

    def query_user_to_multi_sig_signers(self, multi_sig_user: str) -> Any:
        return self.post("/info", {"type": "userToMultiSigSigners", "user": multi_sig_user})

    def name_to_asset(self, name: str) -> int:
        return self.coin_to_asset[name]
