#!/usr/bin/env python
# -*- coding: utf-8 -*-


class Channels:
    NucleusSegmentation = "nucleus_segmentation"
    CellSegmentation = "cell_segmentation"
    DNA = "dna"
    Membrane = "membrane"
    Structure = "structure"
    TransmittedLight = "transmitted_light"
    DefaultOrderList = [
        NucleusSegmentation,
        CellSegmentation,
        DNA,
        Membrane,
        Structure,
        TransmittedLight,
    ]
