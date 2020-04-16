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

import dask.config
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
        ]

    def run(
        self,
        dataset: str,
        distributed: bool = False,
        clean: bool = False,
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

        clean: bool
            Should the local staging directory be cleaned prior to this run.
            Default: False (Do not clean)

        debug: bool
            A debug flag for the developer to use to manipulate how much data runs,
            how it is processed, etc. Additionally, if debug is True, any mapped
            operation will run on threads instead of processes.
            Default: False (Do not debug)
        """
        # Initalize steps
        standardize_fov_array = steps.StandardizeFOVArray()
        single_cell_features = steps.SingleCellFeatures()

        # Choose executor
        if debug:
            exe = LocalExecutor()
            distributed_executor_address = None
            log.info(f"Debug flagged. Will use threads instead of Dask.")
        else:
            if distributed:
                # Create or get log dir
                # Do not include ms
                log_dir_name = datetime.now().isoformat().split(".")[0]
                log_dir = Path(f"~/.dask_logs/actk/{log_dir_name}").expanduser()
                # Log dir settings
                log_dir.mkdir(parents=True, exist_ok=True)

                # Configure dask config
                dask.config.set({
                    "scheduler.work-stealing": False,
                })

                # Create cluster
                log.info("Creating SLURMCluster")
                cluster = SLURMCluster(
                    cores=1,
                    memory="4GB",
                    processes=1,
                    queue="aics_cpu_general",
                    walltime="10:00:00",
                    death_timeout=180,
                    nanny=False,
                    extra=["--resources nprocs=1,nthreads=1"],
                    local_directory=str(log_dir),
                    log_directory=str(log_dir),
                )
                log.info("Created SLURMCluster")

                # Scale up cluster
                cluster.scale_up(64)

                # Use the port from the created connector to set executor address
                distributed_executor_address = cluster.scheduler_address

                # Log dashboard URI
                log.info(f"Dask dashboard available at: {cluster.dashboard_link}")
            else:
                # Create local cluster
                log.info("Creating LocalCluster")
                cluster = LocalCluster()
                log.info("Created LocalCluster")

                # Set distributed_executor_address
                distributed_executor_address = cluster.scheduler_address

                # Log dashboard URI
                log.info(f"Dask dashboard available at: {cluster.dashboard_link}")

            # Use dask cluster
            exe = DaskExecutor(distributed_executor_address)

        # Configure your flow
        with Flow("actk") as flow:
            standardized_fov_paths_dataset = standardize_fov_array(
                dataset=dataset,
                distributed_executor_address=distributed_executor_address,
                clean=clean,
                debug=debug,
                # Allows us to pass `--desired_pixel_sizes [{float},{float},{float}]`
                **kwargs,
            )

            _ = single_cell_features(
                dataset=standardized_fov_paths_dataset,
                distributed_executor_address=distributed_executor_address,
                clean=clean,
                debug=debug,
                # Allows us to pass `--cell_ceiling_adjustment {int}`
                **kwargs,
            )

        # Run flow and get ending state
        state = flow.run(executor=exe)

        # Get and display any outputs you want to see on your local terminal
        log.info(single_cell_features.get_result(state, flow))

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
