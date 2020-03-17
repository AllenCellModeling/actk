#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from pathlib import Path
from typing import Optional, Tuple, Union

import pandas as pd
from datastep import Step, log_run_params

from ...utils import image_utils
from ...utils.dask_utils import DistributedHandler

###############################################################################

log = logging.getLogger(__name__)

###############################################################################


class SingleCellFeatures(Step):
    def __init__(self, filepath_columns=["features"]):
        super().__init__(filepath_columns=filepath_columns)

    @staticmethod
    def _process_cell(
        row_index: int,
        row: pd.Series,
        desired_pixel_sizes: Tuple[float],
        save_dir: Path,
    ) -> Path:
        pass

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

        # Create features directory
        features_dir = self.step_local_staging_dir / "features"
        features_dir.mkdir(exist_ok=True)

        # Process each row
        with DistributedHandler(distributed_executor_address) as handler:
            # Start processing
            futures = handler.client.map(
                self._process_cell(
                    # Convert dataframe iterrows into two lists of items to iterate over
                    # One list will be row index
                    # One list will be the pandas series of every row
                    *zip(*list(dataset.iterrows())),
                    # Pass the other parameters as list of the same thing for each
                    # mapped function call
                    [desired_pixel_sizes for i in range(len(dataset))],
                    [features_dir for i in range(len(dataset))]
                )
            )

            # Block until all complete
            results = handler.gather(futures)

        # Set the manifest
        self.manifest = pd.DataFrame({"features": results})

        return features_dir
