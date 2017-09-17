from __future__ import absolute_import

from pandas_datareader import data as pdr
from pandas import DataFrame
from datetime import date
from collections import namedtuple
import logging
import dateutil.parser as date_parser

EOD = namedtuple('EOD', ['ticker', 'date', 'open', 'close'])

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class EODReader:
    YF_OPEN = 'Open'
    YF_CLOSE = 'Close'

    def __init__(self, tickers, start_date, end_date, repo):
        self.tickers = tickers
        self.repo = repo
        self.start_date = date_parser.parse(start_date).date()
        self.end_date = date_parser.parse(end_date).date()
        self.data = self._get_data()

    def read(self):
        """
        Return all data within this reader

        :return: [EOD]
        """
        return self.data

    def _get_data(self):
        """
        Generate a list of EOD data from DB. If the data is not complete from start_date to end_date,
        fill the gap with data from Yahoo finance

        :return: [EOD]
        """
        data = []
        for ticker in self.tickers:
            gap_start = self.start_date
            for d in self._dates_between(self.start_date, self.end_date):
                # Try to find EOD for this 'd' at DB at first
                results = self.repo.read([ticker, d.isoformat()])
                # If EOD found in DB
                if results and len(results) == len(self.repo.read_cols):
                    if gap_start < d:
                        data += self._fill_gap(ticker, gap_start, self._yesterday(d))
                    data.append(EOD(ticker=ticker, date=d.isoformat(), open=results[0], close=results[1]))
                    gap_start = self._tomorrow(d)
            data += self._fill_gap(ticker, gap_start, self.end_date)
        return data

    @staticmethod
    def _fetch_eod_data(ticker, start_date, end_date):
        """
        Fetch data from Yahoo finance with the given ticker, start date & end date
        
        :param ticker: string
        :param start_date: date
        :param end_date: date
        :return: DataFrame
        """
        try:
            result = pdr.get_data_yahoo(ticker, start=start_date, end=end_date)
            logging.info('Remote fetching finance data...')
            if not isinstance(result, DataFrame):
                raise Exception('Incorrect return type from data source')
        except Exception as e:
            raise e

        return result

    def _fill_gap(self, ticker, start_date, end_date):
        """
        Return the eods from start_date to end_date by querying Yahoo finance

        :param ticker: string
        :param start_date: date
        :param end_date: date
        :return: [EOD]
        """
        if start_date > end_date:
            return []

        fetched_eods = self._fetch_eod_data(ticker, start_date, end_date)

        if fetched_eods.empty:
            return []

        logger.info('Get data from %s to %s' % (start_date.isoformat(), end_date.isoformat()))

        gap_eods = []
        for i in range(len(fetched_eods)):
            d = fetched_eods.index[i].date()
            # remove EOD that's outside of date range
            if d < start_date or d > end_date:
                continue
            e = EOD(
                ticker=ticker,
                date=str(d.isoformat()),
                open=fetched_eods.iloc[i][self.YF_OPEN],
                close=fetched_eods.iloc[i][self.YF_CLOSE],
            )
            gap_eods.append(e)
            # write down to DB for future uses
            self.repo.write([e.ticker, e.date, e.open, e.close])

        # sort the eods by date
        return sorted(gap_eods, key=lambda eod: date_parser.parse(eod.date))

    @staticmethod
    def _tomorrow(d):
        """
        Return tomorrow's date

        :param d: date
        :return: date
        """
        d = d + date.resolution
        return d

    @staticmethod
    def _yesterday(d):
        """
        Return yesterday's date

        :param d: date
        :return: date
        """
        d = d - date.resolution
        return d

    @staticmethod
    def _dates_between(start_date, end_date):
        """
        Return the list of dates that between start_date and end_date, inclusively

        :param start_date: date
        :param end_date: date
        :return: [date]
        """
        dates = []
        while start_date <= end_date:
            dates.append(start_date)
            # date.resolution == 1 day
            start_date = start_date + date.resolution
        return dates
