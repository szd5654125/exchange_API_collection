from collections import defaultdict
from dataclasses import dataclass, field
import time
import hmac
import hashlib

import asyncio
import aiohttp
from Crypto.Hash import SHA256
from Crypto.PublicKey import RSA
from Crypto.Signature import PKCS1_v1_5
import base64
import json
import logging
import requests

from datetime import datetime as dt, timezone

from exceptions import FailedRequestError, InvalidRequestError
import _helpers

# Requests will use simplejson if available.
try:
    from simplejson.errors import JSONDecodeError
except ImportError:
    from json.decoder import JSONDecodeError

HTTP_URL = "https://{SUBDOMAIN}.{DOMAIN}.{TLD}"
SUBDOMAIN_TESTNET = "api-testnet"
SUBDOMAIN_MAINNET = "api"
DEMO_SUBDOMAIN_TESTNET = "api-demo-testnet"
DEMO_SUBDOMAIN_MAINNET = "api-demo"
DOMAIN_MAIN = "bybit"
DOMAIN_ALT = "bytick"
TLD_MAIN = "com"
TLD_NL = "nl"
TLD_HK = "com.hk"


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


@dataclass
class _V5HTTPManager:
    testnet: bool = field(default=False)
    domain: str = field(default=DOMAIN_MAIN)
    tld: str = field(default=TLD_MAIN)
    demo: bool = field(default=False)
    rsa_authentication: str = field(default=False)
    api_key: str = field(default=None)
    api_secret: str = field(default=None)
    logging_level: logging = field(default=logging.INFO)
    log_requests: bool = field(default=False)
    timeout: int = field(default=10)
    recv_window: bool = field(default=5000)
    force_retry: bool = field(default=False)
    retry_codes: defaultdict[dict] = field(default_factory=dict)
    ignore_codes: dict = field(default_factory=dict)
    max_retries: bool = field(default=3)
    retry_delay: bool = field(default=3)
    referral_id: bool = field(default=None)
    record_request_time: bool = field(default=False)
    return_response_headers: bool = field(default=False)

    async def __post_init__(self):
        """
        Asynchronous initialization of the HTTP client.
        """
        subdomain = SUBDOMAIN_TESTNET if self.testnet else SUBDOMAIN_MAINNET
        domain = DOMAIN_MAIN if not self.domain else self.domain
        if self.demo:
            subdomain = DEMO_SUBDOMAIN_TESTNET if self.testnet else DEMO_SUBDOMAIN_MAINNET

        url = HTTP_URL.format(SUBDOMAIN=subdomain, DOMAIN=domain, TLD=self.tld)
        self.endpoint = url

        if not self.ignore_codes:
            self.ignore_codes = set()
        if not self.retry_codes:
            self.retry_codes = {10002, 10006, 30034, 30035, 130035, 130150}
        self.logger = logging.getLogger(__name__)

        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter(fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
        )
        handler.setLevel(self.logging_level)
        self.logger.addHandler(handler)

        self.client = aiohttp.ClientSession(
            headers={"Content-Type": "application/json", "Accept": "application/json"}
        )
        self.logger.debug("Initialized async HTTP session.")

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
            ]
            integer_params = ["positionIdx"]
            for key, value in parameters.items():
                if key in string_params:
                    if type(value) != str:
                        parameters[key] = str(value)
                elif key in integer_params:
                    if type(value) != int:
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

    def _auth(self, payload, recv_window, timestamp):
        """
        Prepares authentication signature per Bybit API specifications.
        """

        if self.api_key is None or self.api_secret is None:
            raise PermissionError("Authenticated endpoints require keys.")

        param_str = str(timestamp) + self.api_key + str(recv_window) + payload

        return generate_signature(
            self.rsa_authentication, self.api_secret, param_str
        )

    @staticmethod
    def _verify_string(params, key):
        if key in params:
            if not isinstance(params[key], str):
                return False
            else:
                return True
        return True

    async def _submit_request(self, method=None, path=None, query=None, auth=False):
        if query is None:
            query = {}

        recv_window = self.recv_window
        retries_attempted = self.max_retries
        req_params = None

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

            if auth:
                timestamp = _helpers.generate_timestamp()
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
            else:
                headers = {}

            async with aiohttp.ClientSession() as session:
                try:
                    if method == "GET":
                        url = f"{path}?{req_params}" if req_params else path
                        async with session.get(url, headers=headers, timeout=self.timeout) as response:
                            response_text = await response.text()
                            if response.status != 200:
                                raise Exception(f"HTTP error: {response.status} - {response_text}")
                            s_json = await response.json()

                    elif method == "POST":
                        async with session.post(path, headers=headers, data=req_params,
                                                timeout=self.timeout) as response:
                            response_text = await response.text()
                            if response.status != 200:
                                raise Exception(f"HTTP error: {response.status} - {response_text}")
                            s_json = await response.json()

                    ret_code = "retCode"
                    ret_msg = "retMsg"

                    if s_json.get(ret_code):
                        error_msg = f"{s_json[ret_msg]} (ErrCode: {s_json[ret_code]})"
                        delay_time = self.retry_delay

                        if s_json[ret_code] in self.retry_codes:
                            if s_json[ret_code] == 10002:
                                error_msg += ". Added 2.5 seconds to recv_window"
                                recv_window += 2500
                            elif s_json[ret_code] == 10006:
                                limit_reset_time = int(response.headers.get("X-Bapi-Limit-Reset-Timestamp", 0))
                                delay_time = (limit_reset_time - _helpers.generate_timestamp()) / 1000
                                error_msg += f". Rate limit will reset in {delay_time} seconds"

                            self.logger.error(f"{error_msg}. {retries_remaining}")
                            await asyncio.sleep(delay_time)
                            continue

                        elif s_json[ret_code] not in self.ignore_codes:
                            raise InvalidRequestError(
                                request=f"{method} {path}: {req_params}",
                                message=s_json[ret_msg],
                                status_code=s_json[ret_code],
                                time=dt.now(timezone.utc).strftime("%H:%M:%S"),
                                resp_headers=response.headers,
                            )

                    if self.log_requests:
                        self.logger.debug(f"Response headers: {response.headers}")

                    if self.return_response_headers:
                        return s_json, response.elapsed, response.headers
                    elif self.record_request_time:
                        return s_json, response.elapsed
                    else:
                        return s_json

                except aiohttp.ClientError as e:
                    if self.force_retry:
                        self.logger.error(f"{e}. {retries_remaining}")
                        await asyncio.sleep(self.retry_delay)
                        continue
                    else:
                        raise e
