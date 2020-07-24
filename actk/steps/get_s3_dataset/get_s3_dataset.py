#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from pathlib import Path
from typing import Dict, List, Optional, Union

from datastep import Step, log_run_params
from quilt3 import Package

from ...constants import DatasetFields

###############################################################################

log = logging.getLogger(__name__)

###############################################################################


class GetS3Dataset(Step):
    @log_run_params
    def run(self):
        p = Package.browse("aics/pipeline_integrated_single_cell", "s3://allencell")
        manifest = p["metadata.csv"]().sample(10, random_state=12)
        manifest = manifest.rename(columns={
            "ChannelNumber405": DatasetFields.ChannelIndexDNA,
            "ChannelNumber638": DatasetFields.ChannelIndexMembrane,
            "ChannelNumberStruct": DatasetFields.ChannelIndexStructure,
            "ChannelNumberBrightfield": DatasetFields.ChannelIndexBrightfield,
        })

        return manifest
