"""
    Class for reading data from a CSV or CSV-like object
"""
import io
import logging
import six
import zipfile

from spackl.util import DictReader, Path, Sniffer

from .base import BaseFile
from .result import FileResult

_log = logging.getLogger(__name__)


class CSV(BaseFile):
    """
        A standard Postgresql database client

        Args:
            file_path_or_obj : str or file-like - The filepath of the csv, or a file-like object
                                                  that can be opened, read, and closed
        Kwargs:
            name : str - The canonical name to use for this instance
            csv_kwargs : Parameters to pass to the csv reader (fieldnames, delimiter, dialect, etc)
    """
    def __init__(self, file_path_or_obj, name=None, **csv_kwargs):
        self._name = name

        self._file = None
        self._csv_kwargs = dict(**csv_kwargs)

        self._set_file(file_path_or_obj)
        if self._file is None:
            raise AttributeError('Could not validate given file path or obj : %r', file_path_or_obj)

    def __repr__(self):
        return "<{self.__class__.__name__}('{self._file}')>".format(self=self)

    def __str__(self):
        return str(self._file)

    @property
    def name(self):
        return self._name

    def _set_file(self, file_path_or_obj):
        file = None

        if hasattr(file_path_or_obj, 'open') or hasattr(file_path_or_obj, 'read'):
            file = file_path_or_obj
        elif isinstance(file_path_or_obj, six.string_types):
            file = Path(file_path_or_obj).expanduser().resolve()
            if not file.exists():
                _log.warning('Provided path does not exist : %s', file_path_or_obj)
                file = None
            else:
                if zipfile.is_zipfile(str(file)):
                    file = zipfile.ZipFile(str(file))
        self._file = file

    def _open(self):
        if isinstance(self._file, zipfile.ZipFile):
            self._data = self._extract_zipfile()
        elif hasattr(self._file, 'open'):
            self._data = self._file.open()
        else:
            self._data = self._file

    def _close(self):
        try:
            self._data.close()
        except Exception as e:
            _log.warning('Could not close data : %s', e)

    def _extract_zipfile(self):
        data = io.StringIO()
        for name in self._file.namelist():
            data.write(self._file.read(name).decode('utf-8'))
        data.seek(0)
        return data

    def _get_dialect(self):
        dialect = Sniffer().sniff(self._data.read(1024*1024))
        self._data.seek(0)
        return dialect

    def query(self, **kwargs):
        _kwargs = dict(**self._csv_kwargs)
        _kwargs.update(**kwargs)

        self.open()

        if not _kwargs.get('dialect', None):
            _kwargs['dialect'] = self._get_dialect()

        reader = DictReader(self._data, **_kwargs)
        result = FileResult(list(reader))
        self.close()

        return result

    def query_df(self, **kwargs):
        from pandas import read_csv
        fp = self._file.filename if isinstance(self._file, zipfile.ZipFile) else self._file
        return read_csv(fp, **kwargs)
