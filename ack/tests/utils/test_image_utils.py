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
    "memb_seg_image, "
    "dna_channel_index, "
    "membrane_channel_index, "
    "structure_channel_index, "
    "brightfield_channel_index, "
    "current_pixel_sizes, "
    "desired_pixel_sizes, "
    "expected_image",
    [
        (
            "example_raw_0.ome.tiff",
            "example_nuc_seg_0.tiff",
            "example_memb_seg_0.tiff",
            3,
            2,
            1,
            0,
            None,
            (0.29, 0.29, 0.29),
            "example_normed_image_array_0.ome.tiff",
        ),
        (
            "example_raw_1.ome.tiff",
            "example_nuc_seg_1.tiff",
            "example_memb_seg_1.tiff",
            2,
            0,
            1,
            3,
            None,
            (0.29, 0.29, 0.29),
            "example_normed_image_array_1.ome.tiff",
        ),
    ],
)
def test_get_normed_image_array(
    data_dir,
    raw_image,
    nuc_seg_image,
    memb_seg_image,
    dna_channel_index,
    membrane_channel_index,
    structure_channel_index,
    brightfield_channel_index,
    current_pixel_sizes,
    desired_pixel_sizes,
    expected_image,
):
    """
    The example data used to test this function was generated with the original function
    and then stored with `aicsimageio.writers.OmeTiffWriter` after doing an
    `aicsimageio.transforms.transpose_to_dims` to transpose to "CZYX" as `OmeTiffWriter`
    requires data have the "YX" dimensions last.
    """
    # Get actual
    actual_image, actual_channels, actual_px_sizes = image_utils.get_normed_image_array(
        data_dir / raw_image,
        data_dir / nuc_seg_image,
        data_dir / memb_seg_image,
        dna_channel_index,
        membrane_channel_index,
        structure_channel_index,
        brightfield_channel_index,
        current_pixel_sizes,
        desired_pixel_sizes,
    )

    # Read expected
    expected_image = AICSImage(data_dir / expected_image)

    # Assert actual equals expected
    assert np.array_equiv(actual_image, expected_image.get_image_data("CYXZ", S=0, T=0))
    assert actual_channels == expected_image.get_channel_names()
    assert tuple(actual_px_sizes) == expected_image.get_physical_pixel_size()


@pytest.mark.parametrize(
    "image, cell_index, expected_image",
    [
        (
            "example_normed_image_array_0.ome.tiff",
            1,
            "example_selected_and_adjusted_array_0_1.ome.tiff",
        ),
        (
            "example_normed_image_array_0.ome.tiff",
            2,
            "example_selected_and_adjusted_array_0_2.ome.tiff",
        ),
        (
            "example_normed_image_array_0.ome.tiff",
            3,
            "example_selected_and_adjusted_array_0_3.ome.tiff",
        ),
    ],
)
def test_select_and_adjust_segmentation_ceiling(
    data_dir, image, cell_index, expected_image,
):
    # Get actual
    image = AICSImage(data_dir / image).get_image_data("CZYX", S=0, T=0)
    actual_image = image_utils.select_and_adjust_segmentation_ceiling(image, cell_index)

    # Read expected
    expected_image = AICSImage(data_dir / expected_image)

    # Assert actual equals expected
    assert np.array_equiv(actual_image, expected_image.get_image_data("CZYX", S=0, T=0))


@pytest.mark.parametrize(
    "image, expected_image",
    [
        (
            "example_selected_and_adjusted_array_0_1.ome.tiff",
            "example_cropped_with_segs_array_0_1.ome.tiff",
        ),
        (
            "example_selected_and_adjusted_array_0_2.ome.tiff",
            "example_cropped_with_segs_array_0_2.ome.tiff",
        ),
        (
            "example_selected_and_adjusted_array_0_3.ome.tiff",
            "example_cropped_with_segs_array_0_3.ome.tiff",
        ),
    ],
)
def test_crop_raw_channels_with_segmentation(data_dir, image, expected_image):
    # Get actual
    image = AICSImage(data_dir / image)
    data = image.get_image_data("CZYX", S=0, T=0)
    channels = image.get_channel_names()
    actual_image = image_utils.crop_raw_channels_with_segmentation(data, channels)

    # Read expected
    expected_image = AICSImage(data_dir / expected_image)

    # Assert actual equals expected
    assert np.array_equiv(actual_image, expected_image.get_image_data("CZYX", S=0, T=0))
