"""
    Class for using Redshift as a source database
"""
from .postgres import Postgres


class Redshift(Postgres):
    _db_type = 'redshift+psycopg2'

    def __init__(self, *args, **kwargs):
        port = 5439
        if 'port' in kwargs:
            port = kwargs.pop('port')

        conn_params = {'sslmode': 'prefer'}

        super(Redshift, self).__init__(
            *args, port=port, conn_params=conn_params, **kwargs)
