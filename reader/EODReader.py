from __future__ import  absolute_import

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
        return self.data

    def _get_data(self):
        data = []
        for ticker in self.tickers:
            gap_start = self.start_date
            for d in self._dates_between(self.start_date, self.end_date):
                results = self.repo.read([ticker, d.isoformat()])
                if results and len(results) == len(self.repo.read_cols):
                    if gap_start == d:
                        data.append(EOD(ticker=ticker, date=d.isoformat(), open=results[0], close=results[1]))
                        gap_start = self._tomorrow(d)
                        continue
                    else:
                        data += self._fill_gap(ticker, gap_start, self._yesterday(d))
                        data.append(EOD(ticker=ticker, date=d.isoformat(), open=results[0], close=results[1]))
                        gap_start = self._tomorrow(d)

            data += self._fill_gap(ticker, gap_start, self.end_date)
        return data

    @staticmethod
    def _fetch_eod_data(ticker, start_date, end_date):
        """
        
        :param ticker: string
        :param start_date: date
        :param end_date: date
        :return: DataFrame
        """
        try:
            result = pdr.get_data_yahoo(ticker, start=start_date, end=end_date)
            if not isinstance(result, DataFrame):
                raise Exception('Incorrect return type from data source')
        except Exception as e:
            raise e

        return result

    def _fill_gap(self, ticker, start_date, end_date):
        """

        :param ticker:
        :param start_date: date
        :param end_date: date
        :return: [EOD]
        """
        if start_date > end_date:
            return []

        eods = self._fetch_eod_data(ticker, start_date, end_date)

        if eods.empty:
            return []

        logger.info('Get data from %s to %s' % (start_date.isoformat(), end_date.isoformat()))

        eods = [
            EOD(
                ticker=ticker,
                date=str(eods.index[i].date().isoformat()),
                open=eods.iloc[i][self.YF_OPEN],
                close=eods.iloc[i][self.YF_CLOSE],
            )
            for i in range(len(eods))
        ]

        for eod in eods:
            self.repo.write([eod.ticker, eod.date, eod.open, eod.close])

        return eods

    @staticmethod
    def _tomorrow(d):
        """

        :param d: date
        :return: date
        """
        d = d + date.resolution
        return d

    @staticmethod
    def _yesterday(d):
        """

        :param d: date
        :return: date
        """
        d = d - date.resolution
        return d

    @staticmethod
    def _dates_between(start_date, end_date):
        """

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
