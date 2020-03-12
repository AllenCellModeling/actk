#!/usr/bin/env python
# -*- coding: utf-8 -*-

from ack.utils import image_utils

import numpy as np
import pytest

#######################################################################################

# Represents (6, 10, 10) ZYX
ONES = np.ones((6, 10, 10))
TWOS = ONES * 2
THREES = ONES * 3
FOURS = ONES * 4

# Represents (4, 6, 10, 10) CZYX
FAKE_RAW = np.stack([ONES, TWOS, THREES, FOURS])

# Fake Nucleus Segmentation YX
# [[0., 0., 0., 0., 0., 0., 0., 0., 0., 0.],
#  [0., 0., 0., 0., 0., 0., 0., 0., 0., 0.],
#  [0., 0., 0., 0., 0., 0., 0., 0., 0., 0.],
#  [0., 0., 0., 0., 0., 0., 0., 0., 0., 0.],
#  [0., 1., 1., 0., 0., 0., 0., 2., 2., 0.],
#  [0., 1., 1., 0., 0., 0., 0., 2., 2., 0.],
#  [0., 0., 0., 0., 0., 0., 0., 0., 0., 0.],
#  [0., 0., 0., 0., 0., 0., 0., 0., 0., 0.],
#  [0., 0., 0., 0., 0., 0., 0., 0., 0., 0.],
#  [0., 0., 0., 0., 0., 0., 0., 0., 0., 0.]]
SINGLE_NUC = np.ones((2, 2))
LEFT_NUC_SEG = np.pad(SINGLE_NUC, ((4, 4), (1, 2)))
RIGHT_NUC_SEG = np.pad(SINGLE_NUC * 2, ((4, 4), (2, 1)))
FAKE_NUC_SEG_YX = np.concatenate([LEFT_NUC_SEG, RIGHT_NUC_SEG], axis=1)
FAKE_NUC_SEG = np.tile(FAKE_NUC_SEG_YX, (6, 1, 1))

# Fake Membrane Segmentation YX
# [[0., 0., 0., 0., 0., 0., 0., 0., 0., 0.],
#  [0., 0., 0., 0., 0., 0., 0., 0., 0., 0.],
#  [0., 0., 0., 0., 0., 0., 0., 0., 0., 0.],
#  [1., 1., 1., 1., 0., 0., 2., 2., 2., 2.],
#  [1., 1., 1., 1., 0., 0., 2., 2., 2., 2.],
#  [1., 1., 1., 1., 0., 0., 2., 2., 2., 2.],
#  [1., 1., 1., 1., 0., 0., 2., 2., 2., 2.],
#  [0., 0., 0., 0., 0., 0., 0., 0., 0., 0.],
#  [0., 0., 0., 0., 0., 0., 0., 0., 0., 0.],
#  [0., 0., 0., 0., 0., 0., 0., 0., 0., 0.]]
SINGLE_MEMB = np.ones((4, 4))
LEFT_MEMB_SEG = np.pad(SINGLE_MEMB, ((3, 3), (0, 1)))
RIGHT_MEMB_SEG = np.pad(SINGLE_MEMB * 2, ((3, 3), (1, 0)))
FAKE_MEMB_SEG_YX = np.concatenate([LEFT_MEMB_SEG, RIGHT_MEMB_SEG], axis=1)
FAKE_MEMB_SEG = np.tile(FAKE_MEMB_SEG_YX, (6, 1, 1))


#######################################################################################


@pytest.mark.parametrize(
    "dna_channel_index, "
    "membrane_channel_index, "
    "structure_channel_index, "
    "transmitted_light_channel_index, "
    "current_pixel_sizes, "
    "desired_pixel_sizes, "
    "expected_img, "
    "expected_channels, "
    "expected_px_sizes",
    [
        (
            0,
            1,
            2,
            3,
            None,
            None,
            np.stack([FAKE_MEMB_SEG, FAKE_NUC_SEG, ONES, TWOS, THREES, FOURS]),
            [1, 2, 3, 4],
            (1.0, 1.0, 1.0),
        )
    ]
)
def test_get_normed_image_array(
    dna_channel_index,
    membrane_channel_index,
    structure_channel_index,
    transmitted_light_channel_index,
    current_pixel_sizes,
    desired_pixel_sizes,
    expected_img,
    expected_channels,
    expected_px_sizes,
):
    # Get actual
    actual_img, actual_channels, actual_px_sizes = image_utils.get_normed_image_array(
        FAKE_RAW,
        FAKE_NUC_SEG,
        FAKE_MEMB_SEG,
        dna_channel_index,
        membrane_channel_index,
        structure_channel_index,
        transmitted_light_channel_index,
        current_pixel_sizes,
        desired_pixel_sizes,
    )

    # Assert actual equals expected
    assert np.array_equal(actual_img, expected_img)
    assert actual_channels == expected_channels
    assert actual_px_sizes == expected_px_sizes
