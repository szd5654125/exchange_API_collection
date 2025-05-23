from web3 import Web3
from utils import config
from web3.contract import Contract
import base58
from nacl.signing import SigningKey


class HyperliquidDepositor:
    def __init__(self, config: dict, wallet_name: str = "wallet_1"):
        self.config = config
        self.wallet_config = config['on_chain']['wallets'][wallet_name]
        self.contracts_config = config['on_chain']['contracts']['arbitrum']

        self.private_key = self.wallet_config['private_key']
        self.wallet_address = self.wallet_config['address']

        self.w3 = Web3(Web3.HTTPProvider(self.contracts_config['rpc_url']))

        self.usdc_address = Web3.to_checksum_address(self.contracts_config['usdc']['address'])
        self.bridge_address = Web3.to_checksum_address(self.contracts_config['bridge2']['address'])
        self.chain_id = self.contracts_config.get('chainId', 42161)  # 默认 42161 Arbitrum

        # 简化版 USDC ABI，仅包含 transfer
        self.usdc_abi = [
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

    def build_transfer_tx(self, amount_usdc: float, gas_limit: int = 150000, priority_fee_gwei: int = 2) -> dict:
        if amount_usdc < 5:
            raise ValueError("⚠️ 充值金额不能小于 5 USDC。Hyperliquid 最低限制。")

        amount_wei = self.w3.to_wei(amount_usdc, 'mwei')
        nonce = self.w3.eth.get_transaction_count(self.wallet_address)

        latest_block = self.w3.eth.get_block('latest')
        base_fee = latest_block.get('baseFeePerGas', self.w3.to_wei(2, 'gwei'))
        priority_fee = self.w3.to_wei(priority_fee_gwei, 'gwei')
        max_fee = base_fee + priority_fee

        tx = self.usdc_contract.functions.transfer(
            self.bridge_address,
            amount_wei
        ).build_transaction({
            "from": self.wallet_address,
            "gas": gas_limit,
            "maxFeePerGas": max_fee,
            "maxPriorityFeePerGas": priority_fee,
            "nonce": nonce,
            "chainId": self.chain_id,
        })
        return tx

    def sign_and_send(self, tx: dict) -> tuple:
        signed_tx = self.w3.eth.account.sign_transaction(tx, self.private_key)
        tx_hash = self.w3.eth.send_raw_transaction(signed_tx.raw_transaction)
        receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash)
        return tx_hash.hex(), receipt.transactionHash.hex()

    def deposit(self, amount: float) -> str:
        tx = self.build_transfer_tx(amount_usdc=amount)
        tx_hash, _ = self.sign_and_send(tx)
        print(f"✅ 成功发送充值交易，TxHash: {tx_hash}")
        return tx_hash


# depositor = HyperliquidDepositor(config)
# depositor.deposit(amount=5)
