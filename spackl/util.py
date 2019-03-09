import abc
import collections
import csv
import datetime
import decimal
import json
import sys


if sys.version_info >= (3, 4):
    ABC = abc.ABC
else:
    ABC = abc.ABCMeta(str('ABC'), (), {})
abstractmethod = abc.abstractmethod

try:
    from pathlib import Path
    Path().expanduser()
except (ImportError, AttributeError):
    from pathlib2 import Path

Sniffer = csv.Sniffer


class DictReader(csv.DictReader):
    def __next__(self):
        """
            Copied from python3 source to force use of OrderedDict
        """
        if self.line_num == 0:
            # Used only for its side effect.
            self.fieldnames
        row = next(self.reader)
        self.line_num = self.reader.line_num

        # unlike the basic reader, we prefer not to return blanks,
        # because we will typically wind up with a dict full of None
        # values
        while row == []:
            row = next(self.reader)
        d = collections.OrderedDict(zip(self.fieldnames, row))
        lf = len(self.fieldnames)
        lr = len(row)
        if lf < lr:
            d[self.restkey] = row[lf:]
        elif lf > lr:
            for key in self.fieldnames[lr:]:
                d[key] = self.restval
        return d

    next = __next__


class DtDecEncoder(json.JSONEncoder):
    def default(self, obj):
        if (
                isinstance(obj, datetime.date) or
                isinstance(obj, datetime.datetime)):
            return obj.isoformat()
        elif isinstance(obj, decimal.Decimal):
            return float(obj)
        else:
            return super(DtDecEncoder, self).default(obj)
