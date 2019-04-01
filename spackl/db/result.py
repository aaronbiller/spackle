"""
    Base classes to house the results of queries against a source databse
"""
from google.cloud.bigquery.table import RowIterator
from sqlalchemy.engine import ResultProxy

from spackl.result import BaseResult


class QueryResult(BaseResult):
    def __init__(self, query_iterator=None):
        keys = list()
        result = list()

        if query_iterator is not None:
            if not isinstance(query_iterator, (RowIterator, ResultProxy)):
                raise TypeError(
                    'DbQueryResult instantiated with invalid result type : %s. Must be a sqlalchemy.engine.ResultProxy,'
                    ' returned by a call to sqlalchemy.engine.Connection.execute(), or a google.cloud.bigquery.table.'
                    'RowIterator, returned by a call to google.cloud.bigquery.Client.query().result()'
                    % type(query_iterator))

            if isinstance(query_iterator, ResultProxy):
                keys = list(query_iterator.keys())
            elif isinstance(query_iterator, RowIterator):
                keys = [field.name for field in query_iterator.schema]
            result = list(query_iterator)

        super(QueryResult, self).__init__(keys, result)

    def __repr__(self):
        return '<QueryResult: {qr._result}>'.format(qr=self)
