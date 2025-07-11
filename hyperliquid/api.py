import json
import logging
from json import JSONDecodeError

import requests

from apis.hyperliquid.hyperliquid_utils.constants import MAINNET_API_URL
from apis.hyperliquid.hyperliquid_utils.error import ClientError, ServerError
from apis.hyperliquid.hyperliquid_utils.types import Any


class API:
    def __init__(self):
        self.base_url = MAINNET_API_URL
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        self._logger = logging.getLogger(__name__)

    def post(self, url_path: str, payload: Any = None) -> Any:
        payload = payload or {}
        url = self.base_url + url_path
        response = self.session.post(url, json=payload)
        self._handle_exception(response)
        try:
            return response.json()
        except ValueError:
            return {"error": f"Could not parse JSON: {response.text}"}

    def _handle_exception(self, response):
        status_code = response.status_code
        if status_code < 400:
            return
        if 400 <= status_code < 500:
            try:
                err = json.loads(response.text)
            except JSONDecodeError:
                raise ClientError(status_code, None, response.text, None, response.headers)
            if err is None:
                raise ClientError(status_code, None, response.text, None, response.headers)
            error_data = err.get("data")
            raise ClientError(status_code, err["code"], err["msg"], response.headers, error_data)
        raise ServerError(status_code, response.text)
