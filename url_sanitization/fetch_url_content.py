#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved
"""
Given an URL in string, make a requests, fetches its content,
and BeautifulSoup the content.
"""

import time
import requests
import logging
import urllib.parse as urlparse
from bs4 import BeautifulSoup


class URLUtil(object):
    def __init__(self, url, timeout=3, parser='html5lib', proxies=None):
        self.url = url
        self.soup = None
        self.success = None
        self.message = None
        self.timeout = timeout
        self.parser = parser
        self.proxies = proxies
        self.running_time = 0

    def read_and_soup(self):
        """
        Fetch content from a url
        """
        user_agent_list = [
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) \
            AppleWebKit/537.36 (KHTML, like Gecko) \
            Chrome/35.0.1916.47 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) \
            AppleWebKit/537.36 (KHTML, like Gecko) \
            Chrome/60.0.3112.113 Safari/537.36',
            'Mozilla/5.0 (Windows NT 6.1; Win64; x64) \
            AppleWebKit/537.36 (KHTML, like Gecko) \
            Chrome/60.0.3112.90 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) \
            AppleWebKit/537.36 (KHTML, like Gecko) \
            Chrome/44.0.2403.157 Safari/537.36',
            'Mozilla/5.0 (Windows NT 6.3; Win64; x64) \
            AppleWebKit/537.36 (KHTML, like Gecko) \
            Chrome/60.0.3112.113 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) \
            AppleWebKit/537.36 (KHTML, like Gecko) \
            Chrome/57.0.2987.133 Safari/537.36',
        ]

        parsed = urlparse.urlparse(self.url)
        headers = {
            "User-Agent": user_agent_list[
                hash(parsed.netloc + parsed.path) % len(user_agent_list)],
            "X-Requested-With": "XMLHttpRequest",
            "Accept-Encoding": "gzip",
        }
        try:
            start_time = time.time()
            r = requests.get(
                self.url,
                headers=headers,
                timeout=self.timeout,
                stream=True,
                proxies=self.proxies
            )
            url_data = r.content.decode('utf-8', 'ignore')
            soup = BeautifulSoup(url_data, self.parser)
            end_time = time.time()
            self.running_time = end_time - start_time
            self.soup = soup
            self.success = True
        except Exception as e:
            logging.error(repr(e) + ", url: {0}".format(self.url))
            self.success = False
            self.message = "Modified URL error: " + str(e)

    def get_body(self):
        """
        Get the body of a HTML content
        """
        if self.soup is None:
            self.read_and_soup()
        if not self.success or self.soup.body is None:
            return ""
        return self.soup.body.getText()

    def get_title(self):
        """
        Get the title from a HTML content
        """
        if self.soup is None:
            self.read_and_soup()
        if not self.success or self.soup.title is None:
            return ""
        return self.soup.title
