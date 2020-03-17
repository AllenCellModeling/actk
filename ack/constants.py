#!/usr/bin/env python
# -*- coding: utf-8 -*-


class Channels:
    NucleusSegmentation = "nucleus_segmentation"
    MembraneSegmentation = "membrane_segmentation"
    DNA = "dna"
    Membrane = "membrane"
    Structure = "structure"
    Brightfield = "brightfield"
    DefaultOrderList = [
        NucleusSegmentation,
        MembraneSegmentation,
        DNA,
        Membrane,
        Structure,
        Brightfield,
    ]


class DatasetFields:
    FOVId = "FOVId"
    SourceReadPath = "SourceReadPath"
    NucleusSegmentationReadPath = "NucleusSegmentationReadPath"
    MembraneSegmentationReadPath = "MembraneSegmentationReadPath"
    ChannelIndexDNA = "ChannelIndexDNA"
    ChannelIndexMembrane = "ChannelIndexMembrane"
    ChannelIndexStructure = "ChannelIndexStructure"
    ChannelIndexBrightfield = "ChannelIndexBrightfield"
    All = [
        FOVId,
        SourceReadPath,
        NucleusSegmentationReadPath,
        MembraneSegmentationReadPath,
        ChannelIndexDNA,
        ChannelIndexMembrane,
        ChannelIndexStructure,
        ChannelIndexBrightfield,
    ]
