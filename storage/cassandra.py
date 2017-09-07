from __future__ import absolute_import
from storage.cassandra_client import CassandraClient


class CassandraRepo(object):

    _keyspace = 'stock'
    _default_ttl = 3600

    def __init__(self, table_info):
        self.table_name = table_info['name']
        self.cols = table_info['cols']
        self.pk_cols = table_info['pk_cols']
        self.read_cols = table_info['read_cols']

    def write(self, values, ttl=_default_ttl):
        CassandraClient.write(self._keyspace, self.table_name, self.cols, values, ttl)

    def read(self, pk_values):
        result = CassandraClient.read(self._keyspace, self.table_name, self.read_cols, self.pk_cols, pk_values)
        if not result:
            return []
        return [item for item in result]

    def delete(self, pk_values):
        return CassandraClient.delete(self._keyspace, self.table_name, self.pk_cols, pk_values)

    def exists(self, pk_values):
        return CassandraClient.exists(self._keyspace, self.table_name, self.pk_cols, pk_values)
