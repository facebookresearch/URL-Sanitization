#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved
"""
Demo Run
It loads data from url_training_data_path and trains a rule for each full
domain that determines which query parameters need to be removed. Then,
it applies the rule to the data in url_full_data_path and removes URLs'
parameters accordingly. Finally, it loads the cleaned URLs to the
output_data_path. Data in url_training_data_path can be either the same
as or a subset of that in url_full_data_path.
"""

from ..modules.process_urls import process_urls

url_training_data_path = './demo_input.tsv'  # path for training data
url_full_data_path = './demo_input.tsv'  # path for all to-be-cleaned URL data
output_data_path = './demo_output.tsv'  # path for cleaned URL data
process_urls(url_training_data_path, url_full_data_path, output_data_path)
