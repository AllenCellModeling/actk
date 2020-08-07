#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This script will run all tasks in a prefect Flow.

When you add steps to you step workflow be sure to add them to the step list
and configure their IO in the `run` function.
"""

import logging
from datetime import datetime
from pathlib import Path

import psutil
from dask_jobqueue import SLURMCluster
from distributed import LocalCluster
from prefect import Flow
from prefect.engine.executors import DaskExecutor, LocalExecutor

from actk import steps

###############################################################################

log = logging.getLogger(__name__)

###############################################################################


class All:
    def __init__(self):
        """
        Set all of your available steps here.
        This is only used for data logging operations, not computation purposes.
        """
        self.step_list = [
            steps.StandardizeFOVArray(),
            steps.SingleCellFeatures(),
            steps.SingleCellImages(),
        ]

    def run(
        self,
        dataset: str,
        distributed: bool = False,
        n_workers: int = 10,
        worker_cpu: int = 8,
        worker_mem: str = "120GB",
        overwrite: bool = False,
        debug: bool = False,
        **kwargs,
    ):
        """
        Run a flow with your steps.

        Parameters
        ----------
        dataset: str
            The dataset to use for the pipeline.

        distributed: bool
            A boolean option to determine if the jobs should be distributed to a SLURM
            cluster when possible.
            Default: False (Do not distribute)

        n_workers: int
            Number of workers to request (when distributed is enabled).
            Default: 10

        worker_cpu: int
            Number of cores to provide per worker (when distributed is enabled).
            Default: 8

        worker_mem: str
            Amount of memory to provide per worker (when distributed is enabled).
            Default: 120GB

        overwrite: bool
            If this pipeline has already partially or completely run, should it
            overwrite the previous files or not.
            Default: False (Do not overwrite or regenerate files)

        debug: bool
            A debug flag for the developer to use to manipulate how much data runs,
            how it is processed, etc. Additionally, if debug is True, any mapped
            operation will run on threads instead of processes.
            Default: False (Do not debug)
        """
        # Initalize steps
        standardize_fov_array = steps.StandardizeFOVArray()
        single_cell_features = steps.SingleCellFeatures()
        single_cell_images = steps.SingleCellImages()

        # Choose executor
        if debug:
            exe = LocalExecutor()
            distributed_executor_address = None
            batch_size = None
            log.info("Debug flagged. Will use threads instead of Dask.")
        else:
            if distributed:
                # Create or get log dir
                # Do not include ms
                log_dir_name = datetime.now().isoformat().split(".")[0]
                log_dir = Path(f".dask_logs/{log_dir_name}").expanduser()
                # Log dir settings
                log_dir.mkdir(parents=True, exist_ok=True)

                # Create cluster
                log.info("Creating SLURMCluster")
                cluster = SLURMCluster(
                    cores=worker_cpu,
                    memory=worker_mem,
                    queue="aics_cpu_general",
                    walltime="10:00:00",
                    local_directory=str(log_dir),
                    log_directory=str(log_dir),
                )

                # Spawn workers
                cluster.scale(n_workers)
                log.info("Created SLURMCluster")

                # Use the port from the created connector to set executor address
                distributed_executor_address = cluster.scheduler_address

                # Batch size is n_workers * worker_cpu * 0.75
                # We could just do n_workers * worker_cpu but 3/4 of that is safer
                batch_size = int(n_workers * worker_cpu * 0.75)

                # Log dashboard URI
                log.info(f"Dask dashboard available at: {cluster.dashboard_link}")
            else:
                # Create local cluster
                log.info("Creating LocalCluster")
                current_mem_gb = psutil.virtual_memory().available / 2 ** 30
                n_workers = int(current_mem_gb // 4)
                cluster = LocalCluster(n_workers=n_workers)
                log.info("Created LocalCluster")

                # Set distributed_executor_address
                distributed_executor_address = cluster.scheduler_address

                # Batch size on local cluster
                batch_size = int(psutil.cpu_count() // n_workers)

                # Log dashboard URI
                log.info(f"Dask dashboard available at: {cluster.dashboard_link}")

            # Use dask cluster
            exe = DaskExecutor(distributed_executor_address)

        # Configure your flow
        with Flow("actk") as flow:
            standardized_fov_paths_dataset = standardize_fov_array(
                dataset=dataset,
                distributed_executor_address=distributed_executor_address,
                batch_size=batch_size,
                overwrite=overwrite,
                debug=debug,
                # Allows us to pass `--desired_pixel_sizes [{float},{float},{float}]`
                **kwargs,
            )

            single_cell_features_dataset = single_cell_features(
                dataset=standardized_fov_paths_dataset,
                distributed_executor_address=distributed_executor_address,
                batch_size=batch_size,
                overwrite=overwrite,
                debug=debug,
                # Allows us to pass `--cell_ceiling_adjustment {int}`
                **kwargs,
            )

            single_cell_images_dataset = single_cell_images(
                dataset=single_cell_features_dataset,
                distributed_executor_address=distributed_executor_address,
                batch_size=batch_size,
                overwrite=overwrite,
                debug=debug,
                # Allows us to pass `--cell_ceiling_adjustment {int}`
                **kwargs,
            )

        # Run flow and get ending state, log duration
        start = datetime.now()
        state = flow.run(executor=exe)
        duration = datetime.now() - start
        log.info(
            f"Total duration of pipeline: "
            f"{duration.seconds // 60 // 60}:"
            f"{duration.seconds // 60}:"
            f"{duration.seconds % 60}"
        )

        # Get and display any outputs you want to see on your local terminal
        log.info(single_cell_images_dataset.get_result(state, flow))

    def pull(self):
        """
        Pull all steps.
        """
        for step in self.step_list:
            step.pull()

    def checkout(self):
        """
        Checkout all steps.
        """
        for step in self.step_list:
            step.checkout()

    def push(self):
        """
        Push all steps.
        """
        for step in self.step_list:
            step.push()

    def clean(self):
        """
        Clean all steps.
        """
        for step in self.step_list:
            step.clean()
