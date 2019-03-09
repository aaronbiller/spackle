"""
    Base classes to house the results of queries against a source databse
"""
import six

from collections import OrderedDict
from google.cloud.bigquery.table import RowIterator
from sqlalchemy.engine import ResultProxy

from spackl.result import BaseResult


class QueryResult(BaseResult):
    def __init__(self, query_iterator=None):
        if query_iterator is not None:
            if not isinstance(query_iterator, (RowIterator, ResultProxy)):
                raise TypeError(
                    'DbQueryResult instantiated with invalid result type : %s. Must be a sqlalchemy.engine.ResultProxy,'
                    ' returned by a call to sqlalchemy.engine.Connection.execute(), or a google.cloud.bigquery.table.'
                    'RowIterator, returned by a call to google.cloud.bigquery.Client.query().result()'
                    % type(query_iterator))

            result = [OrderedDict(row) for row in query_iterator]
        else:
            result = list()

        if result:
            keys = list(six.iterkeys(result[0]))
            if not all(sorted(keys) == sorted(list(six.iterkeys(x))) for x in result):
                raise AttributeError('keys arg does not match all result keys')
        else:
            keys = list()

        super(QueryResult, self).__init__(keys, result)

    def __repr__(self):
        return '<QueryResult: {qr._result}>'.format(qr=self)
