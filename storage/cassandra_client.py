from __future__ import absolute_import

from cassandra.cluster import Cluster
from cassandra.policies import RetryPolicy


class CassandraClient(object):

    default_contact_points = ['127.0.0.1']
    default_port = 9042

    _session = None

    @classmethod
    def _get_session(cls):
        if cls._session:
            return cls._session

        cluster = Cluster(
            contact_points=cls.default_contact_points,
            port=cls.default_port,
            default_retry_policy=RetryPolicy(),
        )

        cls._session = cluster.connect()
        return cls._session

    @classmethod
    def write(cls, keyspace, table_name, cols, values, ttl):
        session = cls._get_session()
        prepared_statement = session.prepare('''
            INSERT INTO {table_name} ({cols})
            VALUES ({values})
            USING TTL {ttl}
        '''.format(
            table_name='{}.{}'.format(keyspace, table_name),
            cols=", ".join(cols),
            values=", ".join(['?'] * len(cols)),
            ttl=ttl,
        ))
        cls.wrap_execute(prepared_statement, tuple(values))

    @classmethod
    def read(cls, keyspace, table_name, read_cols, pk_cols, pk_values):
        session = cls._get_session()
        prepared_statement = session.prepare('''
                SELECT {read_cols} FROM {table_name}
                WHERE {conditions}
                LIMIT 1
            '''.format(
            table_name='{}.{}'.format(keyspace, table_name),
            read_cols=", ".join(read_cols),
            conditions=" AND ".join(["{pk_col} = ?".format(pk_col=pk_col) for pk_col in pk_cols]),
        ))
        rows = cls.wrap_execute(prepared_statement, tuple(pk_values))
        if not rows:
            return None
        return rows[0]

    @classmethod
    def delete(cls, keyspace, table_name, pk_cols, pk_values):
        session = cls._get_session()
        prepared_statement = session.prepare('''
            DELETE FROM {table_name}
            WHERE {conditions}
            IF EXISTS
        '''.format(
            table_name='{}.{}'.format(keyspace, table_name),
            conditions=" AND ".join(["{pk_col} = ?".format(pk_col=pk_col) for pk_col in pk_cols]),
        ))

        result = cls.wrap_execute(prepared_statement, tuple(pk_values))
        # `applied` field comes with CQL statement `IF EXISTS`,
        # and acts as an indicator of whether the command takes effect or not.
        delete_success = result[0].applied
        return delete_success

    @classmethod
    def exists(cls, keyspace, table_name, pk_cols, pk_values):
        session = cls._get_session()
        prepared_statement = session.prepare('''
            SELECT COUNT(*) FROM {table_name}
            WHERE {conditions}
        '''.format(
            table_name='{}.{}'.format(keyspace, table_name),
            conditions=" AND ".join(["{pk_col} = ?".format(pk_col=pk_col) for pk_col in pk_cols]),
        ))

        count = cls.wrap_execute(prepared_statement, tuple(pk_values))
        return count[0].count > 0

    @classmethod
    def wrap_execute(cls, query, parameters=None, trace=False):
        return cls._get_session().execute(
            query=query,
            parameters=parameters,
            trace=trace
        )
