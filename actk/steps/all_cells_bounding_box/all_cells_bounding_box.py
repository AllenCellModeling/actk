#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from pathlib import Path
from typing import Dict, List, Optional, Union

from datastep import Step, log_run_params

###############################################################################

log = logging.getLogger(__name__)

###############################################################################


class AllCellsBoundingBox(Step):
    def __init__(
        self,
        direct_upstream_tasks: List["Step"] = [],
        config: Optional[Union[str, Path, Dict[str, str]]] = None,
    ):
        super().__init__(direct_upstream_tasks=direct_upstream_tasks, config=config)

    @log_run_params
    def run(self, **kwargs):
        """
        Run a pure function.

        Protected Parameters
        --------------------
        distributed_executor_address: Optional[str]
            An optional executor address to pass to some computation engine.
        clean: bool
            Should the local staging directory be cleaned prior to this run.
            Default: False (Do not clean)
        debug: bool
            A debug flag for the developer to use to manipulate how much data runs,
            how it is processed, etc.
            Default: False (Do not debug)

        Parameters
        ----------

        Returns
        -------
        result: Any
            A pickable object or value that is the result of any processing you do.
        """
        # Your code here
        #
        # The `self.step_local_staging_dir` is exposed to save files in
        #
        # The user should set `self.manifest` to a dataframe of absolute paths that
        # point to the created files and each files metadata
        #
        # By default, `self.filepath_columns` is ["filepath"], but should be edited
        # if there are more than a single column of filepaths
        #
        # By default, `self.metadata_columns` is [], but should be edited to include
        # any columns that should be parsed for metadata and attached to objects
        #
        # The user should not rely on object state to retrieve results from prior steps.
        # I.E. do not call use the attribute self.upstream_tasks to retrieve data.
        # Pass the required path to a directory of files, the path to a prior manifest,
        # or in general, the exact parameters required for this function to run.
        return
