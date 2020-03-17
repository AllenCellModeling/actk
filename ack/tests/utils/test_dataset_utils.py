#!/usr/bin/env python
# -*- coding: utf-8 -*-

import dask.dataframe as dd
import pandas as pd
import pytest

from ack import exceptions
from ack.utils import dataset_utils

#######################################################################################

EXAMPLE_PD_DATAFRAME = pd.DataFrame(
    [
        {"name": "jackson", "job": "engineer"},
        {"name": "rory", "job": "scientist"},
        {"name": "julie", "job": "scientist"},
    ]
)

EXAMPLE_DD_DATAFRAME = dd.from_pandas(EXAMPLE_PD_DATAFRAME, npartitions=1)

#######################################################################################


@pytest.mark.parametrize(
    "dataset, required_columns",
    [
        (EXAMPLE_PD_DATAFRAME, ["name", "job"]),
        (EXAMPLE_DD_DATAFRAME, ["name", "job"]),
        pytest.param(
            EXAMPLE_PD_DATAFRAME,
            ["hello"],
            marks=pytest.mark.raises(exception=exceptions.MissingDataError),
        ),
        pytest.param(
            EXAMPLE_DD_DATAFRAME,
            ["hello"],
            marks=pytest.mark.raises(exception=exceptions.MissingDataError),
        ),
        pytest.param(
            EXAMPLE_PD_DATAFRAME,
            ["name", "job", "hello"],
            marks=pytest.mark.raises(exception=exceptions.MissingDataError),
        ),
        pytest.param(
            EXAMPLE_DD_DATAFRAME,
            ["name", "job", "hello"],
            marks=pytest.mark.raises(exception=exceptions.MissingDataError),
        ),
    ],
)
def test_check_required_columns(dataset, required_columns):
    # Run check
    dataset_utils.check_required_columns(dataset, required_columns)
