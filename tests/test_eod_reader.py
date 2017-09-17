from __future__ import absolute_import

import unittest
from datetime import date
import dateutil.parser as date_parser
from storage.cassandra import CassandraRepo
from reader.eod_reader import EODReader
from reader import get_trading_close_holidays


test_table_info = {
    'name': 'junior_test',
    'cols': ['ticker', 'date', 'open', 'close'],
    'pk_cols': ['ticker', 'date'],
    'read_cols': ['open', 'close'],
}

test_year = 2017
test_start_date = '2017-07-01'
test_end_date = '2017-07-31'
num_trading_days = 20


class TestEODReader(unittest.TestCase):

    def setUp(self):
        self.ticker = 'MSFT'
        self.start_date = date_parser.parse(test_start_date).date()
        self.end_date = date_parser.parse(test_end_date).date()
        self.holidays = get_trading_close_holidays(test_year)
        self.repo = CassandraRepo(test_table_info)

    def test_eod_reader_empty_repo(self):
        try:
            r = EODReader([self.ticker], self.start_date.isoformat(), self.end_date.isoformat(), self.repo)
            read_data = r.read()
            self.check_read_data(read_data)
        finally:
            self.clean_up_repo()

    def test_eod_reader_first_half_filled_repo(self):
        try:
            today = self.start_date
            while today <= self.end_date:
                if today.day <= 16 and self.is_trading_day(today):
                    self.repo.write([self.ticker, today.isoformat(), .0, .0])
                today += date.resolution

            r = EODReader([self.ticker], self.start_date.isoformat(), self.end_date.isoformat(), self.repo)
            read_data = r.read()
            self.check_read_data(read_data)
        finally:
            self.clean_up_repo()

    def test_eod_reader_second_half_filled_repo(self):
        try:
            today = self.start_date
            while today <= self.end_date:
                if today.day > 16 and self.is_trading_day(today):
                    self.repo.write([self.ticker, today.isoformat(), .0, .0])
                today += date.resolution

            r = EODReader([self.ticker], self.start_date.isoformat(), self.end_date.isoformat(), self.repo)
            read_data = r.read()
            self.check_read_data(read_data)
        finally:
            self.clean_up_repo()

    def test_eod_reader_second_fully_filled_repo(self):
        try:
            today = self.start_date
            while today <= self.end_date:
                if self.is_trading_day(today):
                    self.repo.write([self.ticker, today.isoformat(), .0, .0])
                today += date.resolution

            r = EODReader([self.ticker], self.start_date.isoformat(), self.end_date.isoformat(), self.repo)
            read_data = r.read()
            self.check_read_data(read_data)
        finally:
            self.clean_up_repo()

    def check_read_data(self, read_data):
        self.assertEqual(num_trading_days, len(read_data))
        today = self.start_date
        i = 0
        while today <= self.end_date:
            if self.is_trading_day(today):
                self.assertEqual(today.isoformat(), read_data[i].date)
                i += 1
            today += date.resolution

    def clean_up_repo(self):
        today = self.start_date
        while today <= self.end_date:
            self.repo.delete([self.ticker, today.isoformat()])
            today += date.resolution

    def is_trading_day(self, today):
        return today.weekday() <= 4 and today not in self.holidays
