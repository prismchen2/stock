from __future__ import absolute_import

import unittest
from storage.cassandra import CassandraRepo


class TestCassandraRepo(unittest.TestCase):

    table_info = {
        'name': 'stock',
        'cols': ['ticker', 'date', 'open', 'close'],
        'pk_cols': ['ticker', 'date'],
        'read_cols': ['open', 'close'],
    }

    repo = CassandraRepo(table_info)

    def test_write(self):
        pass

    def test_read(self):
        pass
