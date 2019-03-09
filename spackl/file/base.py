from spackl.util import ABC, abstractmethod


class BaseFile(ABC):
    _opened = False
    _data = None

    @property
    def opened(self):
        return self._opened

    @abstractmethod
    def _open(self):
        """
            Open the source file

            Should set self._data with the readable object
        """
        raise NotImplementedError()

    def open(self):
        """Open the source file

        Returns:
            None
        """
        if not self._opened:
            self._open()
        if self._data:
            self._opened = True

    @abstractmethod
    def _close(self):
        """
            Close any open file
        """
        raise NotImplementedError()

    def close(self):
        """Close any open file
        """
        if self._data:
            self._close()
        self._data = None
        self._opened = False

    @abstractmethod
    def query(self, **kwargs):
        """
            Reads the open file object

            If not open, should call self.open() first

            Kwargs:
                kwargs : Arbitrary parameters to pass to the query method

            Returns:
                IOResult containing the file data
        """
        raise NotImplementedError()
