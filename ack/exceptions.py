#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typing import List


class ParameterTypeError(Exception):
    def __init__(self, parameter_name: str, allowed_types: List[type]):
        # Run base exception init
        super().__init__()

        # Store parameter name and allowed types
        self.parameter_name = parameter_name
        self.allowed_types = allowed_types

    def __str__(self):
        return (
            f"Parameter: `{self.parameter_name}` must be provided as one of the "
            f"following types: {self.allowed_types}"
        )
