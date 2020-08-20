#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json

import numpy as np
import pytest
from aicsimageio import AICSImage
from numpy import testing as nptest

from actk.utils import image_utils

#######################################################################################


@pytest.mark.parametrize(
    "raw_image, "
    "nuc_seg_image, "
    "memb_seg_image, "
    "dna_channel_index, "
    "membrane_channel_index, "
    "structure_channel_index, "
    "brightfield_channel_index, "
    "nucleus_seg_channel_index, "
    "membrane_seg_channel_index, "
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
            0,
            0,
            None,
            # The most recently used desired pixel size from original repo
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
            0,
            0,
            None,
            # The most recently used desired pixel size from original repo
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
    nucleus_seg_channel_index,
    membrane_seg_channel_index,
    current_pixel_sizes,
    desired_pixel_sizes,
    expected_image,
):
    """
    The example data used to test this function was generated with the original function
    and then stored with `aicsimageio.writers.OmeTiffWriter` after doing an
    `aicsimageio.transforms.transpose_to_dims` to transpose to "CZYX" as `OmeTiffWriter`
    requires data have the "YX" dimensions last. Additionally, metadata has been updated
    to the Channel name standards in the constants.py file.
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
        nucleus_seg_channel_index,
        membrane_seg_channel_index,
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
    """
    The example data used to test this function was generated with the original function
    and then stored with `aicsimageio.writers.OmeTiffWriter` after doing an
    `aicsimageio.transforms.transpose_to_dims` to transpose to "CZYX" as `OmeTiffWriter`
    requires data have the "YX" dimensions last. Additionally, metadata has been updated
    to the Channel name standards in the constants.py file.
    """
    # Get actual
    image = AICSImage(data_dir / image).get_image_data("CYXZ", S=0, T=0)
    actual_image = image_utils.select_and_adjust_segmentation_ceiling(image, cell_index)

    # Read expected
    expected_image = AICSImage(data_dir / expected_image)

    # Assert actual equals expected
    assert np.array_equiv(actual_image, expected_image.get_image_data("CYXZ", S=0, T=0))


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
    """
    The example data used to test this function was generated with the original function
    and then stored with `aicsimageio.writers.OmeTiffWriter` after doing an
    `aicsimageio.transforms.transpose_to_dims` to transpose to "CZYX" as `OmeTiffWriter`
    requires data have the "YX" dimensions last. Additionally, metadata has been updated
    to the Channel name standards in the constants.py file.
    """
    # Get actual
    image = AICSImage(data_dir / image)
    data = image.get_image_data("CYXZ", S=0, T=0)
    channels = image.get_channel_names()
    actual_image = image_utils.crop_raw_channels_with_segmentation(data, channels)

    # Read expected
    expected_image = AICSImage(data_dir / expected_image)

    # Assert actual equals expected
    assert np.array_equiv(actual_image, expected_image.get_image_data("CYXZ", S=0, T=0))


@pytest.mark.parametrize(
    "image, expected_image, expected_params",
    [
        (
            "example_cropped_with_segs_array_0_1.ome.tiff",
            "example_prepared_image_for_feature_extraction_0_1.ome.tiff",
            "example_prepared_params_for_feature_extraction_0_1.json",
        ),
        (
            "example_cropped_with_segs_array_0_2.ome.tiff",
            "example_prepared_image_for_feature_extraction_0_2.ome.tiff",
            "example_prepared_params_for_feature_extraction_0_2.json",
        ),
        (
            "example_cropped_with_segs_array_0_3.ome.tiff",
            "example_prepared_image_for_feature_extraction_0_3.ome.tiff",
            "example_prepared_params_for_feature_extraction_0_3.json",
        ),
    ],
)
def test_prepare_image_for_feature_extraction(
    data_dir, image, expected_image, expected_params
):
    """
    The example image data used to test this function was generated with the original
    function and then stored with `aicsimageio.writers.OmeTiffWriter` after doing an
    `aicsimageio.transforms.transpose_to_dims` to transpose to "CZYX" as `OmeTiffWriter`
    requires data have the "YX" dimensions last. Additionally, metadata has been updated
    to the Channel name standards in the constants.py file. Example parameter data was
    stored in JSON after converting numpy arrays to lists.
    """
    # Get actual
    image = AICSImage(data_dir / image).get_image_data("CYXZ", S=0, T=0)
    (
        actual_image,
        actual_memb_com,
        actual_angle,
        actual_flipdim,
    ) = image_utils.prepare_image_for_feature_extraction(image)

    # Read expected image
    expected_image = AICSImage(data_dir / expected_image).get_image_data(
        "CYXZ", S=0, T=0
    )

    # Read expected params
    with open(data_dir / expected_params, "r") as read_params:
        expected_params = json.load(read_params)

    # Unpack expected params and reformat
    expected_memb_com = np.array(expected_params["memb_com"])
    expected_angle = expected_params["angle"]
    expected_flipdim = np.array(expected_params["flipdim"])

    # Assert actual equals expected
    nptest.assert_almost_equal(actual_image, expected_image)
    nptest.assert_almost_equal(actual_memb_com, expected_memb_com)
    nptest.assert_almost_equal(actual_angle, expected_angle)
    nptest.assert_almost_equal(actual_flipdim, expected_flipdim)


@pytest.mark.parametrize(
    "image, expected_features",
    [
        (
            "example_cropped_with_segs_array_0_1.ome.tiff",
            "example_generated_features_0_1.json",
        ),
        (
            "example_cropped_with_segs_array_0_2.ome.tiff",
            "example_generated_features_0_2.json",
        ),
        (
            "example_cropped_with_segs_array_0_3.ome.tiff",
            "example_generated_features_0_3.json",
        ),
    ],
)
def test_get_features_from_image(
    data_dir, image, expected_features,
):
    """
    The example data used to test this function was generated with the original function
    and then stored with JSON.
    """
    # Get actual
    image = AICSImage(data_dir / image).get_image_data("CYXZ", S=0, T=0)
    actual_features = image_utils.get_features_from_image(image)

    # Serialize and deserialize the actual features
    # Things like tuples become lists during serialization
    # which technically assert False, even when the contents are equal
    actual_features = json.dumps(actual_features)
    actual_features = json.loads(actual_features)

    # Read expected
    with open(data_dir / expected_features, "r") as read_feats:
        expected_features = json.load(read_feats)

    # Assert each key value pair
    assert all(feat in actual_features for feat in expected_features)
    # These values may be a tiny bit different depending on
    # machine, environment, randomness, who knows. :shrug:
    # We will test a few specific representative features instead
    for feat in [
        "imsize_orig",
        "com",
        "angle",
        "flipdim",
        "imsize_registered",
        "dna_intensity_mean",
        "cell_intensity_mean",
        "dna_cell_struct_cyto_distal_ratio",
        "dna_shape_sphericity",
        "cell_shape_sphericity",
    ]:
        if isinstance(actual_features[feat], int):
            assert np.isclose(actual_features[feat], expected_features[feat])
        else:
            nptest.assert_almost_equal(
                actual_features[feat], expected_features[feat], decimal=2
            )
