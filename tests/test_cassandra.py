import unittest
from storage.cassandra import CassandraRepo


class TestCassandraRepo(unittest.TestCase):

    table_info = {
        'name': 'stock',
        'cols': ['ticker', 'date', 'open', 'close'],
        'pk_cols': ['ticker', 'date'],
    }

    repo = CassandraRepo(table_info)

    def test_write(self):
        pass
