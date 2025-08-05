
from web3 import Web3
import asyncio


class OnchainUtils:
    def __init__(self, config: dict, wallet_name: str = "wallet_1"):
        self.config = config
        self.wallet_config = config['on_chain']['wallets'][wallet_name]
        self.contracts_config = config['on_chain']['contracts']['arbitrum']

        self.wallet_address = Web3.to_checksum_address(self.wallet_config['address'])

        self.w3 = Web3(Web3.HTTPProvider(self.contracts_config['rpc_url']))

        self.usdc_address = Web3.to_checksum_address(self.contracts_config['usdc']['address'])

        # USDC ABI，仅包含 balanceOf 和 transfer
        self.usdc_abi = [
            {
                "constant": True,
                "inputs": [{"name": "_owner", "type": "address"}],
                "name": "balanceOf",
                "outputs": [{"name": "balance", "type": "uint256"}],
                "type": "function",
            },
            {
                "constant": False,
                "inputs": [
                    {"name": "recipient", "type": "address"},
                    {"name": "amount", "type": "uint256"},
                ],
                "name": "transfer",
                "outputs": [{"name": "", "type": "bool"}],
                "type": "function",
            }
        ]
        self.usdc_contract = self.w3.eth.contract(address=self.usdc_address, abi=self.usdc_abi)

    async def poll_transaction_confirmed(self, tx_hash: str, max_attempts: int = 10, delay_sec: int = 10) -> bool:
        """轮询等待交易上链确认"""
        for attempt in range(1, max_attempts + 1):
            try:
                receipt = self.w3.eth.get_transaction_receipt(tx_hash)
                if receipt and receipt.status == 1:
                    print(f"✅ 交易 {tx_hash} 已确认。")
                    return True
            except Exception:
                pass  # 交易还未打包
            print(f"⏳ 等待交易确认中...（第 {attempt}/{max_attempts} 次）")
            await asyncio.sleep(delay_sec)
        print(f"❌ 超时，交易 {tx_hash} 未确认。")
        return False

    async def get_usdc_balance(self) -> float:
        """查询钱包的 USDC 余额，单位 USDC"""
        balance_wei = self.usdc_contract.functions.balanceOf(self.wallet_address).call()
        balance = balance_wei / 1e6  # USDC 6位小数
        print(f"📊 当前 USDC 余额：{balance} USDC")
        return balance

    async def poll_usdc_balance_change(
        self,
        except_usdc_amount: float,
        max_attempts: int = 30,
        delay_sec: int = 10
    ) -> bool:
        """轮询检测余额是否达到预期"""
        for attempt in range(1, max_attempts + 1):
            await asyncio.sleep(delay_sec)
            current_balance_wei = self.usdc_contract.functions.balanceOf(self.wallet_address).call()
            current_balance = current_balance_wei / 1e6
            if current_balance >= except_usdc_amount:
                print(f"✅ 检测到余额为 {current_balance} USDC，符合预期！")
                return True
        print("❌ 超时，余额未按预期增加。")
        return False

