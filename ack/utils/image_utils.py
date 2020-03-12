#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typing import List, Optional, Tuple

import aicsimageprocessing as proc
import dask.array as da
import numpy as np
from aicsimageio import AICSImage, types

#######################################################################################


def get_normed_image_array(
    raw_image: types.ImageLike,
    nucleus_seg_image: types.ImageLike,
    membrane_seg_image: types.ImageLike,
    dna_channel_index: int,
    membrane_channel_index: int,
    structure_channel_index: int,
    transmitted_light_channel_index: int,
    current_pixel_sizes: Optional[Tuple[float]] = None,
    desired_pixel_sizes: Optional[Tuple[float]] = None,
) -> Tuple[np.ndarray, List[str], Tuple[float]]:
    """
    Take a pandas row (dictionary), and preferences file (dictionary) from pipeline
    spreadsheet and returns image as a CYXZ numpy array channel ordering of [seg_cell,
    seg_nuc, nuc, mem, struct, transmitted].
    """

    # Read images
    raw = AICSImage(raw_image)
    nuc_seg = AICSImage(nucleus_seg_image)
    memb_seg = AICSImage(membrane_seg_image)

    # Get default current and desired pixel sizes
    if current_pixel_sizes is None:
        current_pixel_sizes = raw.get_physical_pixel_size()

    # Default desired to be the same pixel size
    if desired_pixel_sizes is None:
        desired_pixel_sizes = current_pixel_sizes

    # Select the channels
    channel_indices = [
        dna_channel_index,
        membrane_channel_index,
        structure_channel_index,
        transmitted_light_channel_index,
    ]
    selected_channels = [
        raw.get_image_dask_data("ZYX", S=0, T=0, C=index) for index in channel_indices
    ]

    # Combine selections and get numpy array
    raw = da.stack(selected_channels).compute()

    # Convert pixel sizes to numpy arrays
    current_pixel_sizes = np.array(current_pixel_sizes)
    desired_pixel_sizes = np.array(desired_pixel_sizes)

    # Only resize image if desired is different from current
    if not np.array_equal(current_pixel_sizes, desired_pixel_sizes):
        # Resize images
        scale = current_pixel_sizes / desired_pixel_sizes
        raw = np.stack(
            [proc.resize(channel, scale, "bilinear") for channel in raw]
        )

    # Prep segmentations
    nuc_seg = nuc_seg.get_image_data("ZYX", S=0, T=0, C=0)
    memb_seg = memb_seg.get_image_data("ZYX", S=0, T=0, C=0)

    # We do not assume that the segementations are the same size as the raw
    # Resize the segmentations to match the raw
    # Drop the first dimension of the raw image as it is the channel dimension
    raw_size = np.array(raw.shape[1:]).astype(float)
    nuc_size = np.array(nuc_seg.shape).astype(float)
    memb_size = np.array(memb_seg.shape).astype(float)
    scale_nuc = raw_size / nuc_size
    scale_memb = raw_size / memb_size

    # Actual resize
    nuc_seg = proc.resize(nuc_seg, scale_nuc, method="nearest")
    memb_seg = proc.resize(memb_seg, scale_memb, method="nearest")

    # Normalize images
    normalized_images = []
    for i, index in enumerate(channel_indices):
        if index == transmitted_light_channel_index:
            norm_method = "trans"
        else:
            norm_method = "img_bg_sub"

        # Normalize and append
        normalized_images.append(proc.normalize_img(raw[i], method=norm_method))

    # Stack all together
    img = np.stack([nuc_seg, memb_seg, *normalized_images])
    channel_names = ["nuc_seg", "cell_seg", "dna", "memb", "struct", "trans"]

    return img, channel_names, tuple(desired_pixel_sizes)
