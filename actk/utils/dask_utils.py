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

    def batched_map(
        self, func, *iterables, batch_size: int = 10, **kwargs,
    ) -> Iterable[Any]:
        """
        Map a function across iterables in a batched fashion.

        If the iterables are of length 1000, but the batch size is 10, it will
        process and _complete_ 10 at a time. Other batch implementations relate
        to how the tasks are submitted to the scheduler itself. See `batch_size`
        parameter on the `distributed.Client.map`:
        https://distributed.dask.org/en/latest/api.html#distributed.Client.map

        Parameters
        ----------
        func: Callable
            A serializable callable function to run across each iterable set.
        iterables: Iterables
            List-like objects to map over. They should have the same length.
        batch_size: int
            Number of items to process and _complete_ in a single batch.
        **kwargs: dict
            Other keyword arguments to pass down to this handler's client.

        Returns
        -------
        results: Iterable[Any]
            The complete results of all items after they have been fully processed
            and gathered.
        """
        results = []
        for i in range(0, len(iterables[0]), batch_size):
            this_batch_iterables = []
            for iterable in iterables:
                this_batch_iterables.append(iterable[i:i+batch_size])

            futures = self.client.map(
                func,
                *this_batch_iterables,
                **kwargs,
            )

            results += self.gather(futures)

        return results

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
