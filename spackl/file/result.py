"""
    Base classes to house the results of reading files
"""
import collections
import six

from spackl.result import BaseResult


class FileResult(BaseResult):
    def __init__(self, result_dicts=None):
        if result_dicts is not None:
            if not isinstance(result_dicts, list):
                result_dicts = [result_dicts]

            for row in result_dicts:
                if not isinstance(row, collections.OrderedDict):
                    raise TypeError(
                        'FileResult instantiated with invalid row type : %s. Must be collections.OrderedDict'
                        % type(result_dicts))
        else:
            result_dicts = list()

        if result_dicts:
            keys = list(six.iterkeys(result_dicts[0]))
            if not all(sorted(keys) == sorted(list(six.iterkeys(x))) for x in result_dicts):
                raise AttributeError('keys arg does not match all result keys')
        else:
            keys = list()

        super(FileResult, self).__init__(keys, result_dicts)

    def __repr__(self):
        return '<FileResult: {fr._result}>'.format(fr=self)
