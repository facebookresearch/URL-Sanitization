#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved

import time
import logging
import difflib
import pandas as pd
import urllib.parse as urlparse
from pebble import ProcessPool
import sys

from fetch_url_content import URLUtil


class URLComparison(object):
    def __init__(
            self,
            timeout=3,
            verbose=False,
            parser='html5lib',
            proxies=None,
            process_timeout=3600,
            chunksize=200,
            max_worker=4):
        self.timeout = timeout
        self.verbose = verbose
        self.parser = parser
        self.proxies = proxies
        # set timeout to wait for website to load (seconds)
        self.process_timeout = process_timeout
        self.chunksize = chunksize
        self.max_worker = max_worker

    def compare_two_soups(self, soup_1, soup_2):
        """
        Compare content between 2 urls.
        Use difflib.SequenceMatcher to get ratio of same text,
        look for same title. Measure total time to fetch and soup URLs.
        difflib.SequenceMatcher ratio is equal to 2.0 * M/T, where
        M is the number of matches and T is the number of elements in both
        sequences.
        See https://docs.python.org/2/library/difflib.html.
        """

        assert isinstance(soup_1, URLUtil)
        assert isinstance(soup_2, URLUtil)

        try:
            start_time = time.time()
            if self.verbose:
                logging.info("soup1: " + soup_1.get_body())
                logging.info("soup2: " + soup_2.get_body())
            dl_ratio = difflib.SequenceMatcher(
                None,
                soup_1.get_body(),
                soup_2.get_body(),
            ).ratio()
            body_length = len(soup_1.get_body())
            same_title = \
                soup_1.get_title() == soup_1.get_title()
            end_time = time.time()
            running_time = end_time - start_time
            return pd.Series({
                "success": True,
                "message": None,
                "dl_ratio": dl_ratio,
                "running_time": running_time,
                "body_length": body_length,
                "same_title": same_title
            })
        except Exception as e:
            message = str(e) + ", url: {0}".format(soup_1.url)
            logging.error(message)
            return pd.Series({
                "success": False,
                "message": message,
                "dl_ratio": None,
                "running_time": None,
                "body_length": None,
                "same_title": None
            })

    def generate_modified_urls(self, url):
        """
        For the given url, iterate over query parameters, generate a modified
        url by removing a parameter. Also keep a copy of original url for
        AA-test.
        """
        parsed = urlparse.urlparse(url)
        query = urlparse.parse_qs(parsed.query)

        # keep a copy to the original url for the AA test
        modified_urls = [(None, url)]
        for key in query.keys():
            # modify URL by removing query param (key)
            query_mod = query.copy()
            del query_mod[key]
            query_mod_parsed = parsed._replace(
                query=urlparse.urlencode(query_mod, True)
            )
            mod_url = urlparse.urlunparse(query_mod_parsed)
            modified_urls.append((key, mod_url))
        return modified_urls

    def process_one_url(self, url):
        """
        Function to iterate over query params in a particular url,
        parse params, iteratively remove each, store comparison. Also
        performs an 'AA test' comparing the full URL to itself to
        account for dynamic elements in a webpage and minimize false
        positives.

        :param url: STRING
        :return: dict containing each param, the difference ratio
        """
        # first compare url to self ('AA test')
        # Then, removing one query parameter at a time for computation:
        # loop over each parameter (query key) in URL, remove it,
        # then compare fetch of modified url to original
        url_withs_soup = URLUtil(
            url, timeout=self.timeout, parser=self.parser, proxies=self.proxies)
        modified_urls = self.generate_modified_urls(url)
        compare_result = []
        for key, mod_url in modified_urls:
            # Compare urls and save output:
            # how similar would a URL would be to its original form if
            # a particular query string was removed? Use content similarity
            # and whether it has the same title as metrics.
            mod_url_with_soup = URLUtil(
                mod_url,
                timeout=self.timeout, parser=self.parser, proxies=self.proxies)
            comp = self.compare_two_soups(url_withs_soup, mod_url_with_soup)

            current = pd.concat(
                (pd.Series({'url': url, 'key': key, 'mod_url': mod_url}), comp))
            compare_result.append(current)

        compare_result = pd.DataFrame(compare_result)
        return compare_result

    def process_one_url_empty_result(self, url, message):
        """
        Prepare the results with error message, in case it encounters error
        when process the url.
        :param url: STRING
        :param message: STRING
        :return: DataFrame
        """
        return pd.DataFrame({
            'url': [url],
            'key': [None],
            'mod_url': [None],
            "success": [False],
            "message": [message],
            "dl_ratio": [None],
            "running_time": [None],
            "body_length": [None],
            "same_title": [None]
        })

    @staticmethod
    def _chunker(seq, size, start_idx=0):
        return (seq[pos: pos + size] for pos in range(start_idx, len(seq), size))

    def process_multiple_urls(self, url_list):
        """
        Process a list of URLs in parallel.
        :param url_list: list, where each element is an URL in string
        :return: pd.DataFrame, where each row contains the comparison result
            for one pair of URLs.
        """
        if len(url_list) == 0:
            raise ValueError("empty list!")
        start_idx = 0
        i = 0
        results = []

        start = time.time()

        for partial_url_list in self._chunker(
                url_list, self.chunksize, start_idx):
            # parcel out jobs to various pebble multithreading 'workers'
            # We use pebble so we can enforce a timeout--sometimes users share
            # GB-sized files, other times the website just returns nothing
            # for hours.
            print(
                str(float(i) / len(url_list) * 100) + " percent complete",
                file=sys.stderr)
            print(
                "Elapsed Time: " + str(time.time() - start),
                file=sys.stderr)
            with ProcessPool(max_workers=self.max_worker) as pool:
                future = pool.map(
                    self.process_one_url,
                    partial_url_list,
                    timeout=self.process_timeout
                )
                iterator = future.result()
                # iterate over all results, if a computation timed out
                # print it and continue to the next result
                for index in range(len(partial_url_list)):
                    try:
                        result = next(iterator)
                        results.append(result)
                    except StopIteration:
                        break
                    except TimeoutError as error:
                        message = \
                            "Function took longer than %d seconds" \
                            % error.args[1]
                        logging.error(message)
                        results.append(
                            self.process_one_url_empty_result(
                                partial_url_list[index], message))
                    except Exception as e:
                        message = "other error: " + str(e)
                        logging.error(message)
                        results.append(
                            self.process_one_url_empty_result(
                                partial_url_list[index], message))
            i += len(partial_url_list)

        print(
            "Elapsed Time: " + str(time.time() - start),
            file=sys.stderr)
        print(
            "Rate: "
            + str(len(url_list) / (time.time() - start))
            + " urls per second",
            file=sys.stderr
        )

        url_info = pd.concat(results, axis=0).reset_index()
        return url_info
