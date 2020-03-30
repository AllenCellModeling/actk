#!/usr/bin/env python
# -*- coding: utf-8 -*-

from concurrent.futures import Future as ThreadFuture
from concurrent.futures import ThreadPoolExecutor as ThreadClient
from typing import Any, Iterable, Optional, Union

from distributed import Client as DaskClient
from distributed import Future as DaskFuture

#######################################################################################


class DistributedHandler:
    def __init__(self, address: Optional[str] = None):
        # Create client based off address existance
        if address is None:
            self._client = ThreadClient()
        else:
            self._client = DaskClient(address)

    @property
    def client(self):
        return self._client

    def gather(
        self, futures: Iterable[Union[ThreadFuture, DaskFuture]]
    ) -> Iterable[Any]:
        if isinstance(self.client, ThreadClient):
            return list(futures)
        else:
            return self.client.gather(futures)

    def close(self):
        if isinstance(self.client, ThreadClient):
            self.client.shutdown()
        else:
            self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
