"""\
Base class for wrapping raw data into a suitable download format.
"""

import io

class BaseFormat(io.RawIOBase):
    """\
    Base class for wrapping raw data into a suitable download format.
    """
    
    def __init__(self, fd, description, comments=None):
        """\
        :param fd: readable and seekable raw data stream
        :type fd: file object
        :param description: Set of fields and types constituting the raw data
        :type description: Cursor.description
        """
        self._fd = fd
        self._description = description
        self._comments = comments
        
        self._header = ''
        self._footer = ''
        self._compression_config = ''
        self._row_format = ''
        
        pos = self._fd.tell()
        self._fd_length = self._fd.seek(0, io.SEEK_END)
        self._fd.seek(pos)
        
        self._initialized = False

    def _initialize(self):
        """\
        Compute length and current position with header
        """
        if self._initialized:
            return
        
        self._position = self._fd.tell()
        self._length = len(self._header) + self._fd_length + len(self._footer)
        if self._position > 0:
            self._position += len(self._header)
        
        self._initialized = True
    
    @property
    def header(self):
        """\
        Return header data.
        """
        return self._header
    
    @property
    def footer(self):
        """\
        Return footer data.
        """
        return self._footer
    
    @property
    def compression_config(self):
        """\
        Return compression codec configuration snippet.
        """
        return self._compression_config
    
    @property
    def row_format(self):
        """\
        Return row format configuration snippet.
        """
        return self._row_format
    
    def readinto(self, b):
        """\
        Read up to len(b) bytes into b.

        Returns number of bytes read (0 for EOF), or None if the object
        is set not to block and has no data to read.
        """
        self._initialize()
        
        if self._position < len(self._header):
            chunk = self._header[self._position:self._position+len(b)]
        elif self._position < len(self._header) + self._fd_length:
            chunk = self._fd.read(len(b))
        else:
            pos = self._position - len(self._header) - self._fd_length
            chunk = self._footer[pos:pos+len(b)]
            
        n = len(chunk)
        self._position += n
        
        # Return the data read
        try:
            b[:n] = chunk
        except TypeError as err:
            import array
            if not isinstance(b, array.array):
                raise err
            b[:n] = array.array(b'b', chunk)

        return n

    def seek(self, pos, whence=0):
        """\
        Change stream position.

        Change the stream position to byte offset pos. Argument pos is
        interpreted relative to the position indicated by whence.  Values
        for whence are:

        * 0 -- start of stream (the default); offset should be zero or positive
        * 1 -- current stream position; offset may be negative
        * 2 -- end of stream; offset is usually negative

        Return the new absolute position.
        """
        if self.closed:
            raise ValueError("seek on closed file")
        try:
            pos.__index__
        except AttributeError:
            raise TypeError("an integer is required")
        if not (0 <= whence <= 2):
            raise ValueError("invalid whence")
        
        self._initialize()
        
        # Quick case for tell()
        if pos==0 and whence==1:
            return self._position
        
        self._position = {
            0: max(0, pos),
            1: min(self._length, max(0, self._position + pos)),
            2: min(self._length, self._length + pos)
        }[whence]
        
        self._fd.seek(self._position - len(self._header), io.SEEK_SET)
 
        return self._position

    def readable(self):
        """\
        Return True if the stream can be read from. If False, `read()` will
        raise IOError.
        """
        return self._fd.readable()

    def seekable(self):
        """\
        Return True if the stream supports random access. If False, `seek()`,
        `tell()` and `truncate()` will raise IOError.
        """
        return self._fd.seekable()
