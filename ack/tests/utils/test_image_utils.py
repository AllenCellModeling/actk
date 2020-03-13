#!/usr/bin/env python
# -*- coding: utf-8 -*-

import numpy as np
import pytest
from aicsimageio import AICSImage

from ack.utils import image_utils

#######################################################################################


@pytest.mark.parametrize(
    "raw_image, "
    "nuc_seg_image, "
    "cell_seg_image, "
    "dna_channel_index, "
    "membrane_channel_index, "
    "structure_channel_index, "
    "transmitted_light_channel_index, "
    "current_pixel_sizes, "
    "desired_pixel_sizes, "
    "expected_image",
    [
        (
            "example_raw_0.ome.tiff",
            "example_nuc_seg_0.tiff",
            "example_cell_seg_0.tiff",
            3,
            2,
            1,
            0,
            None,
            None,
            "example_normed_image_array_0.ome.tiff",
        ),
        (
            "example_raw_1.ome.tiff",
            "example_nuc_seg_1.tiff",
            "example_cell_seg_1.tiff",
            2,
            0,
            1,
            3,
            None,
            None,
            "example_normed_image_array_1.ome.tiff",
        ),
    ],
)
def test_get_normed_image_array(
    data_dir,
    raw_image,
    nuc_seg_image,
    cell_seg_image,
    dna_channel_index,
    membrane_channel_index,
    structure_channel_index,
    transmitted_light_channel_index,
    current_pixel_sizes,
    desired_pixel_sizes,
    expected_image,
):
    # Get actual
    actual_image, actual_channels, actual_px_sizes = image_utils.get_normed_image_array(
        data_dir / raw_image,
        data_dir / nuc_seg_image,
        data_dir / cell_seg_image,
        dna_channel_index,
        membrane_channel_index,
        structure_channel_index,
        transmitted_light_channel_index,
        current_pixel_sizes,
        desired_pixel_sizes,
    )

    # Read expected
    expected_image = AICSImage(data_dir / expected_image)

    # Assert actual equals expected
    assert np.array_equiv(actual_image, expected_image.get_image_data("CZYX", S=0, T=0))
    assert actual_channels == expected_image.get_channel_names()
    assert tuple(actual_px_sizes) == expected_image.get_physical_pixel_size()
