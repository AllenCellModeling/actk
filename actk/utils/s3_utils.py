#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typing import NamedTuple

from boto3.session import Session
from quilt3 import Bucket

###############################################################################

def s3_dir_exists(b: Bucket, key: str) -> bool:
    # Remove trailing "/"
    if key[-1] == "/":
        key = key[:-1]

    # Get parent of key by removing the last bit (the direct file or dir name)
    parent = "/".join(key.split("/")[:-1])

    # ls parent for contents
    contents = b.ls(parent)[0]

    # Add trailing "/"
    if key[-1] != "/":
        key += "/"

    # Check for dir in contents
    return any(f["Prefix"] == key for f in contents)

def s3_file_exists(b: Bucket, key: str) -> bool:
    # Get parent of key by removing the last bit (the direct file or dir name)
    parent = "/".join(key.split("/")[:-1])

    # ls parent for contents
    contents = b.ls(parent)[1]

    # Check for file in contents
    return any(f["Key"] == key for f in contents)


class AWSCreds(NamedTuple):
    access_key: str
    secret_key: str


def get_aws_creds() -> AWSCreds:
    s = Session()
    c = s.get_credentials()
    return AWSCreds(c.access_key, c.secret_key)
