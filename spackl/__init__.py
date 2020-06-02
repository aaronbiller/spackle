from spackl import db, file

__all__ = [db, file]
__version__ = '0.1.1'


DB_TYPE_MAP = {
    'bigquery': db.BigQuery,
    'postgres': db.Postgres,
    'redshift': db.Redshift,
}
FILE_TYPE_MAP = {
    'csv': file.CSV,
}


def get_db(name):
    conf = db.Config()
    for c in conf.dbs:
        if c['name'] == name:
            dbtype = c.get('type', 'postgres')
            _db = DB_TYPE_MAP.get(dbtype, db.Postgres)
            return _db(**c)
    raise ValueError('DB with name "{}" not found.'.format(name))


def get_file(filepath, filetype='csv', **kwargs):
    _file = FILE_TYPE_MAP.get(filetype, file.CSV)
    return _file(filepath, **kwargs)
