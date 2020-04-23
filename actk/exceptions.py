#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typing import List, Union

import dask.dataframe as dd
import pandas as pd

###############################################################################


class MissingDataError(Exception):
    def __init__(
        self, dataset: Union[pd.DataFrame, dd.DataFrame], required_fields: List[str]
    ):
        # Run base exception init
        super().__init__()

        # Store params for display
        self.dataset = dataset
        self.required_fields = required_fields

    def __str__(self):
        return (
            f"Dataset provided does not have the required columns for this operation. "
            f"Dataset columns: {self.dataset.columns} "
            f"Required fields: {self.required_fields} "
            f"Dataset head: {self.dataset.head()}"
        )
