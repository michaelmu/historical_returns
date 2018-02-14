import datetime
import pandas as pd

from yahoo_finance import YahooFinance

class Ticker(object):
    
    _date_fmt = '%Y-%m-%d'
    
    def __init__(self, symbol, amount=1.0):
        self.symbol = symbol
        self.amount = amount
        self.yf = YahooFinance('ticker_cache.db')
        self.min_ds = None
            
    def _date_range(self, start, end):
        """
        Generate a date range between start and end
        """
        # Expecting datetime format input
        #start = datetime.datetime.strptime(start, self._date_fmt)
        #end = datetime.datetime.strptime(end, self._date_fmt)
        return [start + datetime.timedelta(days=i) for i in range((end-start).days)]
    
    def _generate_zero_test(self, start, end, value=10.0):
        """
        Generate a flat ticker value over time
        """
        df = pd.DataFrame(index=self._date_range(start, end))
        df['Adj Close'] = value
        return df
    
    def _generate_constant_test(self, start, end, const=1.02):
        """
        Generate a constantly increasing ticker value over time
        """
        df = pd.DataFrame(index=self._date_range(start, end))
        df['Adj Close'] = [(i*const) for i in range(df.shape[0])]
        return df
    
    def download(self, start, end, interval, action='hist'):
        if self.symbol == 'zero_test':
            df = self._generate_zero_test(start, end)
        elif self.symbol == 'const_test':
            df = self._generate_constant_test(start, end)
        else: 
            df = self.yf.download_ticker(
                self.symbol, start=start, end=end, 
                interval=interval, action=action)
        self.min_ds = min(df.index)
        return df

class Simulator(object):
    """
    params:
        start (str)    - YYYY-MM-DD
        end (str)      - YYYY-MM-DD
        increment (str)- The amount to increment for each step (1w)
        interval (str) - The full time range to iterate over (2yr) to calculate 
                         the returns
    """
    
    _date_fmt = '%Y-%m-%d'
    
    def __init__(self, start, end, interval='2yr', increment='1wk'):
        self.start = datetime.datetime.strptime(start, self._date_fmt)
        self.end = datetime.datetime.strptime(end, self._date_fmt)
        self.interval = interval
        self.increment = increment
        self.max_start = self.get_max_start()
        self.interval_delta = self.get_interval_delta()
        assert self.start < self.end
        assert self.max_start > self.start
        self.portfolio = []
        
    
    def add_security(self, symbol, amount):
        self.portfolio.append(Ticker(symbol, amount))
    
    def get_interval_delta(self):
        return datetime.timedelta(days=365*int(self.interval.split('yr')[0]))
    
    def get_max_start(self):
        """
        Calculate the maximum of our interval starts so the interval
        doesn't slide over the end of our range
        """
        interval_delta = self.get_interval_delta()
        last_min = self.end - interval_delta
        return last_min

    def nearest(self, items, pivot):
        """
        This function will return the datetime in items which is 
        the closest to the date pivot
        """
        if pivot in items:
            return pivot
        else:
            return min(items, key=lambda x: abs(x - pivot))
    
    def get_aligned_tickers(self):
        """
        Pull the ticker histories then make sure the start date is set to
        the max of all of them.
        """
        full_hist = {}    
        start_adj = self.start
        # Pull the full history for the stock
        for t in self.portfolio:
            df = t.download(start=start_adj, end=self.end, interval=self.increment, action='hist')
            full_hist[t] = df
            # Update the adjusted start date in case this one is after
            start_adj = max(start_adj, t.min_ds)
        # Make sure the histories start at the same point
        for t, d in full_hist.items():
            d.drop(d[d.index < start_adj].index)
        return full_hist
    
    def get_return_distributions(self):
        """
        Calculate the returns over our date range.
        params:
            normalize (bool) - Whether or not to normalize the ticker amounts to 1
        """
        hist = {}
        # Pull the start-aligned ticker history data
        full_hist = self.get_aligned_tickers()
        # Pull the index from some ticker as our range to iterate over
        sampledf = next(iter(full_hist.values()))
        daterange = sampledf[sampledf.index < self.max_start].index
        for t, df in full_hist.items():
            return_hist = []
            for d in daterange:
                start_dt = d
                end_dt = self.nearest(sampledf.index, d + self.interval_delta)
                start_val = df[df.index==start_dt]['Adj Close'].values[0]
                end_val = df[df.index==end_dt]['Adj Close'].values[0]
                return_hist.append((d, (end_val - start_val)/start_val*t.amount))
            hist[t.symbol] = return_hist
        return hist
                
            
        
       
    
    