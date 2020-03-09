#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from pathlib import Path
from typing import Optional, Tuple, Union

import pandas as pd
from datastep import Step, log_run_params

from ... import exceptions

###############################################################################

log = logging.getLogger(__name__)

###############################################################################


class CellFeatures(Step):
    @log_run_params
    def run(
        self,
        dataset: Union[str, Path, pd.DataFrame],
        desired_pixel_sizes: Tuple[float] = (0.29, 0.29, 0.29),
        distributed_executor_address: Optional[str] = None,
        clean: bool = False,
        debug: bool = False,
        **kwargs
    ):
        """
        Convert raw FOV images into single cell images using provided segmentations.

        Parameters
        ----------
        dataset: Union[str, Path, pd.DataFrame]
            The dataset to use for generating single cell features.
        desired_pixel_sizes: Tuple[float]
            The desired pixel size for to resize each image to in XYZ order.
            Default: (0.29, 0.29, 0.29)
        distributed_executor_address: Optional[str]
            An optional executor address to pass to some computation engine.
            Default: None
        clean: bool
            Should the local staging directory be cleaned prior to this run.
            Default: False (Do not clean)
        debug: bool
            A debug flag for the developer to use to manipulate how much data runs,
            how it is processed, etc.
            Default: False (Do not debug)

        Returns
        -------
        save_dir: Path
            Path to the directory where all the feature JSON files were stored.
        """
        # Handle dataset provided as string or path
        if isinstance(dataset, (str, Path)):
            dataset = Path(dataset).expanduser().resolve(strict=True)

            # Read dataset
            dataset = pd.read_csv(dataset)

        # Ensure that we now have a dataframe dataset
        if not isinstance(dataset, pd.DataFrame):
            raise exceptions.ParameterTypeError("dataset", [str, Path, pd.DataFrame])

        return self.step_local_staging_dir
