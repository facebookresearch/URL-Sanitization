#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved

import pandas as pd
import numpy as np
import urllib.parse as urlparse
import phonenumbers
import re
import sys


class URLParametersRemoval(object):
    """
    Build out parameter-level dataframe with param similarity, same_title
    """
    def __init__(
            self,
            url_data,
            timeout=3,
            verbose=False,
            parser='html5lib',
            proxies=None,
            process_timeout=3600,
            chunksize=200,
            max_worker=4):
        self.url_data = url_data
        self.timeout = timeout
        self.verbose = verbose
        self.parser = parser
        self.proxies = proxies
        # set timeout to wait for website to load (seconds)
        self.process_timeout = process_timeout
        self.chunksize = chunksize
        self.max_worker = max_worker

    def append_url_similarity(self, url_info):
        assert isinstance(self.url_data, pd.DataFrame)
        assert isinstance(url_info, pd.DataFrame)

        if 'canonical_url' not in self.url_data:
            raise ValueError('missing column canonical_url')
        if 'full_domain' not in self.url_data:
            self.url_data['full_domain'] = [
                urlparse.urlparse(x).netloc
                for x in self.url_data['canonical_url'].values
            ]
        if 'url_id' not in self.url_data:
            self.url_data['url_id'] = [
                hash(x)
                for x in self.url_data['canonical_url']
            ]

        url_info_2 = url_info[
            url_info['success']][
            ['url', 'key', 'body_length', 'dl_ratio', 'same_title']]
        url_info_2 = url_info_2.rename(columns={
            'url': 'canonical_url',
            'key': 'param',
            'dl_ratio': 'qsim'
        })

        url_data_with_similarity = self.url_data.merge(
            url_info_2, how='inner')

        return url_data_with_similarity

    @staticmethod
    def build_param_data(url_data_with_similarity):
        """
        For each domain, take the average of query similarity, title similarity,
        body length so we can create domain-specific rules
        """
        param_dat_aa = url_data_with_similarity[
            url_data_with_similarity['param'] == ''].copy()
        param_dat_ab = url_data_with_similarity[
            url_data_with_similarity['param'] != ''].copy()

        # fix a few difficult but popular cases:
        youtube_rows = param_dat_ab[
            "full_domain"].str.contains("www.youtube.com")
        param_dat_ab["same_title"] = np.where(
            youtube_rows & (param_dat_ab["param"] == "v"),
            False, param_dat_ab["same_title"])

        google_rows = param_dat_ab[
            "full_domain"].str.contains("www.google.com")
        param_dat_ab["same_title"] = np.where(
            google_rows & (param_dat_ab.param == "url"),
            False, param_dat_ab['same_title'])

        # take the average of query similarity, title similarity, body length
        param_domain = param_dat_ab.groupby(["full_domain", "param"])[
            "qsim", "same_title", "body_length"
        ].mean()
        param_domain = param_domain.reset_index()

        # subtract AA test similarity from difference
        same_url_means = param_dat_aa.groupby(
            ["full_domain"])["qsim", "same_title"].mean()
        same_url_means = same_url_means.reset_index()
        same_url_means.columns = ["full_domain", "gsim_mean", "same_title_mean"]
        same_url_means = same_url_means.astype({
            "same_title_mean": "float64"})

        # Join it back together
        param_domain = pd.merge(param_domain, same_url_means)
        param_domain["diff_gsim"] = \
            param_domain["gsim_mean"] - param_domain["qsim"]
        param_domain["diff_same_title"] = \
            param_domain["same_title_mean"] \
            - param_domain["same_title"].astype('float64')
        return param_domain

    def parse_urls_for_param(self):
        urls_id_list = []
        urls_param_list = []
        for i in range(self.url_data.shape[0]):
            row_i = self.url_data.loc[i]
            parsed = urlparse.urlparse(row_i['canonical_url'])
            query = urlparse.parse_qs(parsed.query)
            params = list(query.keys())
            urls_id_list.extend([row_i['url_id']] * len(params))
            urls_param_list.extend(params)
            if i % 100000 == 0:
                print("progress: {} / {}".format(i, self.url_data.shape[0]),
                      file=sys.stderr)
        urls_with_param = pd.DataFrame({
            'url_id': urls_id_list,
            'param': urls_param_list
        })
        return urls_with_param

    @staticmethod
    def _check_ph_num(text, region=None):
        for _ in phonenumbers.PhoneNumberMatcher(text, region):
            return True

    # function to check values in query parameters
    @staticmethod
    def _qp_no_phone(v, countries=None):
        if countries is None:
            countries = [
                'IN', 'US', 'BR', 'ID', 'MX', 'PI',
                'VN', 'TH', 'TR', 'GB', 'FR', 'DE']
        any_phone = any(
            URLParametersRemoval._check_ph_num(v, region=c)
            for c in countries)
        return not any_phone

    @staticmethod
    def drop_params_via_similarity(
            urls_with_param, param_domain,
            same_title_upper_bound=0.95,
            mean_diff_gsim_lower_bound=0.02,
            mean_diff_gsim_upper_bound=0.98,
            body_length_lower_bound=100):
        urls = urls_with_param[
            ['url_id', 'full_domain', 'canonical_url', 'param']]
        urls = pd.merge(
            urls, param_domain, how="left", on=["full_domain", "param"])
        urls['url'] = urls['canonical_url']

        # keep list of parameters to remove for each URL, defaults to False
        if 'keep' not in urls:
            urls['keep'] = False

        # THIS IS WHERE THE RUBBER MEETS THE ROAD.
        # Keep params that when removed result in a webpage with a different title or
        # a page whose content is very different. Note that urls['diff'] is
        # the difference above and beyond the change when the URL is refreshed.

        # convert same_tile from bool to float before going further
        urls = urls.astype({'same_title': 'float'})
        keep_idx = (
            (((urls['same_title'] < same_title_upper_bound)
                & (abs(urls['diff_gsim']) > mean_diff_gsim_lower_bound))
                | (urls['diff_gsim'] > mean_diff_gsim_upper_bound))
            & (urls['body_length'] > body_length_lower_bound))

        # keep_idx.mean() # 0.21376656596720206, old result
        urls['keep'] = np.where(keep_idx, True, urls['keep'])

        # ^^ THIS SHOULD EVENTUALLY BE DONE VIA ML
        # - Training outcome, something like:
        # (urls['same_title'] < .1 & (urls['diff'] > .40) | (
        #    urls['diff'] > .98) ) & (urls['body_length'] > 100)

        # Add these parameters + others that we obviously need to remove to
        # our drop list:
        drop = [
            # pii related parameters:
            'pw', 'pass', 'password', 'key', 'username', 'name', 'email',
            'address', 'account', 'password', 'ssn', 'dob', 'zipcode',
            'user_id', 'userid', 'accountid', 'account_id',
            # commonly occuring tracking/token-related parameters
            'utm_source', 'utm_medium', 'utm_campaign', 'utm_content', 'source',
            'utm_term', 'usp', 'edit_requested', 'ogsrc', 'fbclid',
            'entrypoint', 'redirect', 'platform', 'widgetTypeCall', 'logType',
            'uuid', 'app_id', 'campaign', 'src', 'caption', 'fbrefresh',
            'user', 'cp', 'desc', 'c_id', 'geo', 'cmpid', 'cHash', '_reff',
            'pk_campaign', 'ctype', 's_src', 'referrer',
            'channel', 'userid', 'uc_param_str', 'fb-share-results', 'cpidfb',
            'content_type', 'tag', 'campaign_id', 'cID', 'channel_id',
            'NONCE_TOKEN', 'reco_id', 'promo_id',
        ]

        # Add params to a drop index if they match:
        drop_str = '|'.join(drop)
        drop_idx = urls['param'].str.contains(drop_str, regex=True)

        # NOTE: if drop_idx is null, it means that we couldn't reach the website to
        # check query params, so add all params to the drop list.
        drop_idx[pd.isnull(drop_idx)] = True

        # Note that our parameter keep-list should shrink a touch in response to
        # this filtering.
        urls['keep'] = np.where(drop_idx, False, urls['keep'])

        return urls

    # function to iterate over query params and drop if they don't meet condition
    # 'keep'. Eventually could set keep based on ML.
    # Function will be applied to a dataframe grouped by URL.
    @staticmethod
    def drop_query_params(url_group):
        params_dropped = []
        params_kept = []
        url = url_group['url'].values[0]
        url_id = url_group['url_id'].values[0]
        parsed = urlparse.urlparse(url)
        query = urlparse.parse_qs(parsed.query)
        query_dict = dict(query)
        query_dict_output = dict(query)
        # Delete query param unless it meets 'keep' criteria
        for qp, v in query_dict.items():
            if qp in set(url_group['param']):
                if url_group['keep'][
                    url_group['param'] == qp].values[0] \
                        & URLParametersRemoval._qp_no_phone(str(v)):
                    params_kept.append(qp)
                else:
                    params_dropped.append(qp)
                    del query_dict_output[qp]
            else:
                params_dropped.append(qp)
                del query_dict_output[qp]
        # re-parse query
        query_parsed = parsed._replace(
            query=urlparse.urlencode(query_dict_output, True)
        )
        url = urlparse.urlunparse(query_parsed)
        return (
            url_id,
            url_group['url'].values[0],
            url,
            params_dropped,
            params_kept)

    @staticmethod
    def remove_pii_params(urls, lower=None, upper=None):
        # Now group by URL and check each parameter against the list and against
        # common phone number patterns for countries in the list above. Save output
        # as we go in case the process dies.
        urls_grouped = urls.groupby('canonical_url')
        results = []
        i = 0
        for _idx, url_group in urls_grouped:
            if i % 10000 == 0:
                print("progress: {} / {}".format(i, len(urls_grouped)),
                      file=sys.stderr)
            if lower is not None and i < lower:
                i = i + 1
                continue
            if upper is not None and i >= upper:
                break
            results.append(URLParametersRemoval.drop_query_params(url_group))
            i = i + 1
        urls_params_dropped = pd.Series(results)

        # parse out url and params dropped from output
        clean_urls = urls_params_dropped.apply(pd.Series)
        clean_urls.reset_index(level=0, inplace=True)
        clean_urls.columns = ['index', 'urlid', 'canonical_url', 'clean_url',
                              'params_dropped', 'params_kept']

        # Remove email addresses
        email_pattern = re.compile(
            (r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+'))

        clean_urls['clean_url'] = \
            clean_urls['clean_url'].replace(
                email_pattern, '<EMAIL>', regex=True)
        return clean_urls
