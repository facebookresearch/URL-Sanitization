#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved
import pandas as pd
import json
import csv

from url_comparison import URLComparison
from url_parameters_removal import URLParametersRemoval


def process_urls(
        url_training_data_path,
        url_full_data_path,
        output_data_path,
        proxies=None):
    """
    :param url_training_data_path: STRING. Path for a tsv file. Expect the file
        to have a header line with three columns (canonical_url, url_id,
        full_domain). Avoid csv type in purpose, since URL strings may contain
        comma.
    :param url_full_data_path: STRING. Path for a tsv file. Expect the file
        to have a header line with three columns (canonical_url, url_id,
        full_domain). Avoid csv type in purpose, since URL strings may contain
        comma.
    :param output_data_path: STRING. Path for the output file.
    :param proxies: Optional. This argument can be used to configure proxy
        settings for HTTP and/or HTTPS. See requests documentation for
        additional information.
    """
    input_data = pd.read_csv(
        url_training_data_path,
        dtype={'canonical_url': str, 'url_id': str, 'full_domain': str},
        sep='\t',
        header=0)
    url_list = input_data['canonical_url'].values

    run_batch = URLComparison(
        proxies=proxies)
    url_info = run_batch.process_multiple_urls(url_list)

    url_data = pd.read_csv(
        url_full_data_path,
        dtype={'canonical_url': str, 'url_id': str, 'full_domain': str},
        sep='\t',
        header=0)

    removal = URLParametersRemoval(url_data)
    url_data_with_params = removal.parse_urls_for_param()
    url_data_with_similarity = removal.append_url_similarity(
        url_info)
    param_domain = URLParametersRemoval.build_param_data(
        url_data_with_similarity)
    url_data_with_params = url_data.merge(
        url_data_with_params, how='inner')
    urls = URLParametersRemoval.drop_params_via_similarity(
        url_data_with_params, param_domain)

    clean_urls = URLParametersRemoval.remove_pii_params(urls)
    clean_urls['params_dropped'] = [
        json.dumps(x) for x in clean_urls['params_dropped']]
    clean_urls['params_kept'] = [
        json.dumps(x) for x in clean_urls['params_kept']]
    clean_urls.to_csv(
        output_data_path, index=False, header=True, sep='\t',
        quoting=csv.QUOTE_NONE)
