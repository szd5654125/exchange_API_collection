from web3 import Web3
from apis.onchain.onchain_utils import OnchainUtils
from utils import config

erc20_abi = [
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

onchain_utils = OnchainUtils(config)


class USDCTransferOnArbitrum:
    def __init__(self, config: dict, wallet_name: str = "wallet_1"):
        # 读取配置
        self.config = config
        self.wallet_config = config['on_chain']['wallets'][wallet_name]
        self.contracts_config = config['on_chain']['contracts']['arbitrum']

        # 钱包相关
        self.private_key = self.wallet_config['private_key']
        self.from_address = Web3.to_checksum_address(self.wallet_config['address'])

        # 节点和链信息
        self.rpc_url = self.contracts_config['rpc_url']
        self.chain_id = self.contracts_config.get('chainId', 42161)  # 默认 42161 = Arbitrum One

        # USDC合约地址
        self.usdc_address = Web3.to_checksum_address(self.contracts_config['usdc']['address'])

        # 初始化web3对象和USDC合约对象
        self.w3 = Web3(Web3.HTTPProvider(self.rpc_url))
        self.usdc_contract = self.w3.eth.contract(
            address=self.usdc_address,
            abi=erc20_abi
        )

        # 初始化动态小费
        self.base_fee = 0
        self.priority_fee = 0
        self.max_fee = 0

    def refresh_priority_fee(self):
        """刷新当前 gas 基础费和小费。"""
        latest_block = self.w3.eth.get_block("latest")
        self.base_fee = latest_block.get("baseFeePerGas", self.w3.to_wei(2, 'gwei'))
        self.priority_fee = int(self.base_fee * 2 + 1)  # 在基础费率上再加1gwei避免gas过低使得交易失败
        self.priority_fee = min(self.priority_fee, self.w3.to_wei(500, 'gwei'))  # 但最多不超过5 Gwei
        self.max_fee = self.base_fee + self.priority_fee
        print(f"📡 当前 BaseFee: {self.base_fee}, PriorityFee: {self.priority_fee}")

    async def transfer(self, to_address: str, amount_usdc: float):
        # 获取 nonce
        nonce = self.w3.eth.get_transaction_count(self.from_address)

        # 转账金额，单位转为mwei（USDC是6位小数）
        amount_wei = self.w3.to_wei(amount_usdc, "mwei")

        # 获取动态 gas 费用
        base_fee = self.w3.eth.get_block("latest").get("baseFeePerGas", self.w3.to_wei(2, 'gwei'))
        priority_fee = int(base_fee * 0.2)  # priority_fee = 20% of base_fee
        priority_fee = min(priority_fee, self.w3.to_wei(500, 'gwei'))  # 但最多不超过5 Gwei
        max_fee = base_fee + priority_fee

        # 构造交易
        tx = self.usdc_contract.functions.transfer(
            Web3.to_checksum_address(to_address),
            amount_wei
        ).build_transaction({
            "chainId": self.chain_id,
            "from": self.from_address,
            "nonce": nonce,
            "gas": 100000,
            "maxFeePerGas": max_fee,
            "maxPriorityFeePerGas": priority_fee,
        })

        # 签名交易
        signed_tx = self.w3.eth.account.sign_transaction(tx, private_key=self.private_key)

        # 发送交易
        tx_hash = self.w3.eth.send_raw_transaction(signed_tx.raw_transaction)
        print(f"✅ USDC 转账已发送，交易哈希: {tx_hash.hex()}")
        return tx_hash.hex()

    async def safe_transfer(self, to_address: str, amount_usdc: float, max_retries=5):
        to_address = Web3.to_checksum_address(to_address)
        amount_wei = self.w3.to_wei(amount_usdc, "mwei")
        gas_limit = 100000
        retries = 0

        # 发送前刷新一次最新gas
        self.refresh_priority_fee()
        while retries <= max_retries:
            nonce = self.w3.eth.get_transaction_count(self.from_address)

            tx = self.usdc_contract.functions.transfer(
                to_address,
                amount_wei
            ).build_transaction({
                "chainId": self.chain_id,
                "from": self.from_address,
                "nonce": nonce,
                "gas": gas_limit,
                "maxFeePerGas": self.max_fee,
                "maxPriorityFeePerGas": self.priority_fee,
            })

            signed_tx = self.w3.eth.account.sign_transaction(tx, private_key=self.private_key)
            tx_hash = self.w3.eth.send_raw_transaction(signed_tx.raw_transaction)
            print(f"TxHash: {tx_hash.hex()} (第{retries+1}次尝试)")
            print(self.max_fee)
            # 轮询一分40秒一共10次
            success = await onchain_utils.poll_transaction_confirmed(tx_hash.hex())
            if success:
                print(f"TxHash: {tx_hash.hex()}")
                return 'finished'

            # 如果失败，增加20%的priority_fee
            self.priority_fee = int(self.priority_fee * 1.4)
            self.max_fee = self.base_fee + self.priority_fee
            print(f"⚡ 提高PriorityFee到 {self.w3.from_wei(self.priority_fee, 'gwei')} Gwei，重新发起交易...")
            print(self.max_fee)
            retries += 1

        return 'Transfer failed'
