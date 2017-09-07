from __future__ import  absolute_import

import fix_yahoo_finance as yf
from storage.cassandra import CassandraRepo
from reader.EODReader import EODReader

yf.pdr_override()

data_path = 'data/'
tickers = ['MSFT']


table_info = {
        'name': 'junior',
        'cols': ['ticker', 'date', 'open', 'close'],
        'pk_cols': ['ticker', 'date'],
        'read_cols': ['open', 'close'],
    }


repo = CassandraRepo(table_info)


def main():
    reader = EODReader(tickers, '2017-07-01', '2017-07-31', repo)

if __name__ == '__main__':
    main()
