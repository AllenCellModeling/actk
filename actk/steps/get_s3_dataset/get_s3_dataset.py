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
        cells = Package.browse("aics/pipeline_integrated_single_cell", "s3://allencell")
        cell_manifest = cells["metadata.csv"]().sample(10, random_state=12)
        cell_manifest = cell_manifest.rename(columns={
            "ChannelNumber405": DatasetFields.ChannelIndexDNA,
            "ChannelNumber638": DatasetFields.ChannelIndexMembrane,
            "ChannelNumberStruct": DatasetFields.ChannelIndexStructure,
            "ChannelNumberBrightfield": DatasetFields.ChannelIndexBrightfield,
        })

        fovs = Package.browse("aics/pipeline_integrated_cell", "s3://allencell")
        fov_manifest = fovs["metadata.csv"]()
        fov_manifest = fov_manifest[[
            DatasetFields.FOVId, DatasetFields.SourceReadPath,
            DatasetFields.NucleusSegmentationReadPath,
            DatasetFields.MembraneSegmentationReadPath,
        ]]
        cell_manifest = cell_manifest.merge(fov_manifest, on="FOVId", how="left")
        cell_manifest = cell_manifest.drop_duplicates("CellId")

        return cell_manifest
