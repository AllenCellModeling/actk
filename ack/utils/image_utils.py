#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typing import List, Optional, Tuple

import aicsimageprocessing as proc
import dask.array as da
import numpy as np
from aicsimageio import AICSImage, types
from scipy.signal import fftconvolve as convolve

from ..constants import Channels

#######################################################################################


def get_normed_image_array(
    raw_image: types.ImageLike,
    nucleus_seg_image: types.ImageLike,
    cell_seg_image: types.ImageLike,
    dna_channel_index: int,
    membrane_channel_index: int,
    structure_channel_index: int,
    transmitted_light_channel_index: int,
    current_pixel_sizes: Optional[Tuple[float]] = None,
    desired_pixel_sizes: Optional[Tuple[float]] = None,
) -> Tuple[np.ndarray, List[str], Tuple[float]]:
    """
    Generate a single numpy array of related images.

    Parameters
    ----------
    raw_image: types.ImageLike
        A filepath to the raw imaging data. The image should be 4D and include
        channels for DNA, Membrane, Structure, and Transmitted Light.

    nucleus_seg_image: types.ImageLike
        A filepath to the nucleus segmentation for the provided raw image.

    cell_seg_image: types.ImageLike
        A filepath to the cell segmentation for the provided raw image.

    dna_channel_index: int
        The index in channel dimension in the raw image that stores DNA data.

    membrane_channel_index: int
        The index in the channel dimension in the raw image that stores membrane data.

    structure_channel_index: int
        The index in the channel dimension in the raw image that stores structure data.

    transmitted_light_channel_index: int
        The index in the channel dimension in the raw image that stores the transmitted
        light data.

    current_pixel_sizes: Optioal[Tuple[float]]
        The current physical pixel sizes as a tuple of the raw image.
        Default: None (`aicsimageio.AICSImage.get_physical_pixel_size` on the raw image)

    desired_pixel_sizes: Optional[Tuple[float]]
        The desired physical pixel sizes as a tuple to scale all images to.
        Default: None (scale all images to current_pixel_sizes if different)

    Returns
    -------
    normed: np.ndarray
        The normalized images stacked into a single CZYX numpy ndarray.

    channels: List[str]
        The standardized channel names for the returned array.
        In order: ["nuc_seg", "cell_seg", "dna", "memb", "struct", "trans"]

    pixel_sizes: Tuple[float]
        The physical pixel sizes of the returned image in XYZ order.
    """

    # Read images
    raw = AICSImage(raw_image)
    nuc_seg = AICSImage(nucleus_seg_image)
    cell_seg = AICSImage(cell_seg_image)

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
        raw = np.stack([proc.resize(channel, scale, "bilinear") for channel in raw])

    # Prep segmentations
    nuc_seg = nuc_seg.get_image_data("ZYX", S=0, T=0, C=0)
    cell_seg = cell_seg.get_image_data("ZYX", S=0, T=0, C=0)

    # We do not assume that the segementations are the same size as the raw
    # Resize the segmentations to match the raw
    # Drop the first dimension of the raw image as it is the channel dimension
    raw_size = np.array(raw.shape[1:]).astype(float)
    nuc_size = np.array(nuc_seg.shape).astype(float)
    cell_size = np.array(cell_seg.shape).astype(float)
    scale_nuc = raw_size / nuc_size
    scale_cell = raw_size / cell_size

    # Actual resize
    nuc_seg = proc.resize(nuc_seg, scale_nuc, method="nearest")
    cell_seg = proc.resize(cell_seg, scale_cell, method="nearest")

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
    img = np.stack([nuc_seg, cell_seg, *normalized_images])
    channel_names = Channels.DefaultOrderList

    return img, channel_names, tuple(desired_pixel_sizes)


def select_and_adjust_segmentation_ceiling(
    image: np.ndarray, cell_id: int, image_ceiling_adjustment: int = 7
) -> np.ndarray:
    """
    Select and adjust the cell shape "ceiling" for a specific cell in the provided
    image.

    Parameters
    ----------
    image: np.ndarray
        The image numpy ndarray output from `get_normed_image_array`.

    cell_id: int
        The integer index for the target cell.

    image_ceiling_adjustment: int
        The adjust to use for raising the cell shape ceiling. If <= 0, this will be
        ignored and cell data will be selected but not adjusted.
        Default: 7

    Returns
    -------
    adjusted: np.ndarray
        The image with the cell segmentation adjusted for ceiling shape correction.
    """
    # Select only the data in the first two channels (the segmentation channels)
    # where the data matches the provided cell id
    image[0:2] = image[0:2] == cell_id

    # Adjust image ceiling if adjustment is greater than zero
    if image_ceiling_adjustment > 0:
        # Because they are conservatively segmented,
        # we raise the "ceiling" of the cell shape
        # Get the center of mass of the nucleus
        nuc_com = proc.get_center_of_mass(image[0])[-1]

        # Get the top of the cell
        cell_top = np.where(np.sum(np.sum(image[1], axis=0), axis=0))[0][-1]

        # Get the halfway point between the two
        start = int(np.floor((nuc_com + cell_top) / 2))

        # Get the shape of the cell from the cell segmentation
        cell_shape = image[1, :, :, start:]

        # Adjust cell shape "ceiling" using the adjustment integer provided
        start_ind = int(np.floor(image_ceiling_adjustment)) - 1
        imf = np.zeros([1, 1, image_ceiling_adjustment * 2 - 1])
        imf[:, :, start_ind:] = 1
        cell_shape = convolve(cell_shape, imf, mode="same") > 1e-8

        # Set the image data with the new cell shape data
        image[1, :, :, start:] = cell_shape

    return image


def crop_raw_channels_with_segmentation(
    image: np.ndarray, channels: List[str]
) -> np.ndarray:
    """
    Crop imaging data in raw channels using a provided selected full field of with a
    target cell in the segmentation channels.

    Parameters
    ----------
    image: np.ndarray
        The image numpy ndarray output from `select_and_adjust_segmentation_ceiling`.

    channels: List[str]
        The channel names for the provided image.
        The channels output from `get_normed_image_array`.

    Returns
    -------
    cropped: np.ndarray
        A 4D numpy ndarray with CZYX dimensions in the same order as provided.
        The raw DNA channel has been cropped using the nucleus segmentation.
        All other raw channels have been cropped using the cell segmentation.
    """
    # Select segementation indicies
    nuc_ind = np.array(channels) == Channels.NucleusSegmentation
    cell_ind = np.array(channels) == Channels.CellSegmentation

    # Select DNA and all other indicies
    dna_ind = np.array(channels) == Channels.DNA
    other_channel_inds = np.ones(len(channels))
    other_channel_inds[nuc_ind | cell_ind | dna_ind] = 0

    # Crop DNA channel with the nucleus segmentation
    image[dna_ind] = image[dna_ind] * image[nuc_ind]

    # All other channels are cropped using cell segmentation
    for i in np.where(other_channel_inds)[0]:
        image[i] = image[i] * image[cell_ind]

    return image
