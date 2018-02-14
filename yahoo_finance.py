from __future__ import print_function

import datetime
import requests
import numpy as np
import pandas as pd
import time
import io
import re

from sqlite_cache import SqliteCache

class YahooFinance(object):

    _url = ("https://query1.finance.yahoo.com/v7/finance/download/{ticker}"
            "?period1={start}&period2={end}&interval={interval}&events={action}")
    
    def __init__(self, sqlite_file=None):
        self.yahoo_checked = None
        self.dfs = {}
        if sqlite_file:
            self.cache = SqliteCache(sqlite_file)
        else:
            self.cache = None

    def get_yahoo_crumb(self, force=False, ttl=60*10):
        """
        Regenerate the yahoo cookie
        """
        # use same cookie for 5 min
        if self.yahoo_checked and not force:
            now = datetime.datetime.now()
            delta = (now - self.yahoo_checked).total_seconds()
            if delta < ttl:
                return (self.yahoo_crumb, self.yahoo_cookie)
        res = requests.get('https://finance.yahoo.com/quote/SPY/history')
        self.yahoo_cookie = res.cookies['B']
        pattern = re.compile('.*"CrumbStore":{"crumb":"(?P<crumb>[^"]+)"}')
        for line in res.text.splitlines():
            m = pattern.match(line)
            if m is not None:
                self.yahoo_crumb = m.groupdict()['crumb']
        # Reset timer
        self.yahoo_checked = datetime.datetime.now()
        print("Yahoo crumb: {} Yahoo cookie: {}".format(self.yahoo_crumb, self.yahoo_cookie))
        return (self.yahoo_crumb, self.yahoo_cookie)
    
    def format_date(self, date_str, position):
        """
        Format date from string
        """
        if date_str is None and position == 'start':
            return int(time.mktime(time.strptime('1950-01-01', '%Y-%m-%d')))
        if date_str is None and position == 'end':
            dt = datetime.datetime.now()
            return int(time.mktime(dt.replace(hour=0, minute=0, second=0, microsecond=0).timetuple()))
        if isinstance(date_str, datetime.datetime):
            return int(time.mktime(date_str.timetuple()))
        return int(time.mktime(time.strptime(str(date_str), '%Y-%m-%d')))

    def fetch(self, url):
        """
        Fetch the url results. Use cached results if exist.
        """
        cache_key = url
        if self.cache:
            res = self.cache.get(cache_key)
            if res:
                print("Using cached result...")
                return res
        crumb, cookie = self.get_yahoo_crumb()
        url = url + "&crumb=" + crumb
        results = requests.get(url, cookies={'B': cookie}).text
        if "error" in results:
            raise Exception('"Returned error in results', results)
        # Cache the results
        if self.cache:
            self.cache.update(cache_key, results)
        return results
        
    def download_ticker(self, ticker, start=None, end=None, interval='1d', action='hist'):
        """
        Download ticker results for given action
        :Parameters:
            tickers : str, list
                List of tickers to download
            start: str
                Download start date string (YYYY-MM-DD) or datetime. Default is 1950-01-01
            end: str
                Download end date string (YYYY-MM-DD) or datetime. Default is today
            interval: str
                The time interval for the results. Default: 1d
            action: str
                One of 'hist', 'div', 'split' for price history, dividends, or stock splits
                Default: 'hist'
        """
        assert action in ['hist', 'div', 'split']
        # format dates
        start = self.format_date(start, 'start')
        end = self.format_date(end, 'end')
        url = self._url.format(ticker=ticker, start=start, end=end, interval=interval, action=action)
        res = self.fetch(url)
        df = pd.read_csv(
            io.StringIO(res), index_col=0, error_bad_lines=False,header=0).replace('null', np.nan).dropna()
        df.index = pd.to_datetime(df.index)
        return df
