from __future__ import absolute_import

from tickers.ftp_client import FTPClient
import unittest


class TestFTPClient(unittest.TestCase):

    def test_get_tickers(self):
        ftp_client = FTPClient()
        all_tickes = ftp_client.get_tickers()
        # should return more than 3000 tickers
        self.assertTrue(len(all_tickes) > 3000)
        self.assertEqual(['Symbol', 'Security Name', 'Market Category', 'Test Issue', 'Financial Status',
                          'Round Lot Size', 'ETF', 'NextShares'], ftp_client.get_titles())
