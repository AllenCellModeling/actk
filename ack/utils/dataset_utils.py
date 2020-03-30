#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pathlib import Path
from typing import List, Optional, Union

import dask.dataframe as dd
import pandas as pd

from .. import exceptions

#######################################################################################


def check_required_fields(
    dataset: Union[str, Path, pd.DataFrame, dd.DataFrame], required_fields: List[str],
) -> Optional[exceptions.MissingDataError]:
    # Handle dataset provided as string or path
    if isinstance(dataset, (str, Path)):
        dataset = Path(dataset).expanduser().resolve(strict=True)

        # Read dataset
        dataset = dd.read_csv(dataset)

    # Check that all columns provided as required are in the dataset
    if not all(required_col in dataset.columns for required_col in required_fields):
        raise exceptions.MissingDataError(dataset, required_fields)
