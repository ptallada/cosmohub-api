import collections
import copy
import io
import os

class HDFSPathReader(io.RawIOBase):
    """\
    Read an HDFS path (either file or directory) as a stream of bytes.
  
    If the path refers to a directory, the contents of the files (ordered by
    name) are concatenated in the resulting stream.
    """

    def __init__(self, client, path):
        """\
        :param client: HDFSClient to use to access the data
        :type client: `pyhdfs.HdfsClient`
        :param path: HDFS path to read
        :type path: str
        """
        self._client = client
        self._path = path

        self._length = None
        self._position = None
        self._all_files = None
        self._current_files = None

        self._initialize()

    def _initialize(self):
        """\
        Open the requested path and compute some internal parameters.

        Those parameters are needed to calculate the final size of the stream or
        to be able to seek inside the stream.
        """
        self._length = 0
        self._position = 0
        self._all_files = collections.deque()

        status = self._client.get_file_status(self._path)
        if status['type'] == 'FILE' and status['length']>0:
            self._all_files.append({
                'name': os.path.basename(self._path),
                'length': status['length'],
                'offset': 0,
            })
            self._path = os.path.dirname(self._path)
            self._length += status['length']

        else:
            for entry in self._client.list_status(self._path):
                if entry['type'] != 'FILE' or entry['length']==0:
                    continue
                self._all_files.append({
                    'name': entry['pathSuffix'],
                    'length': entry['length'],
                    'offset': 0,
                })
                self._length += entry['length']

        self._current_files = copy.deepcopy(self._all_files)
    
    def readinto(self, b):
        """\
        Read up to len(b) bytes into b.

        Returns number of bytes read (0 for EOF), or None if the object
        is set not to block and has no data to read.
        """
        if not self._current_files:
            return 0
      
        # Take the next file and read the next chunk
        entry = self._current_files.popleft()
        file_path = os.path.join(self._path, entry['name'])
        length = min(len(b), entry['length'] - entry['offset'])
        
        with self._client.open(file_path, offset=entry['offset'], length=length, buffersize=len(b)) as fd:
            chunk = fd.read(len(b))
        
        # Put back the file to the pool if there is still unread data in it
        n = len(chunk)
        self._position += n
        entry['offset'] += n
        if entry['offset'] < entry['length']:
            self._current_files.appendleft(entry)

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

        # Quick case for tell()
        if pos==0 and whence==1:
            return self._position

        self._current_files = collections.deque()
        skip = {
            0: max(0, pos),
            1: min(self._length, max(0, self._position + pos)),
            2: min(self._length, self._length + pos)
        }[whence]
        self._position = 0

        for entry in self._all_files:
            if entry['length'] <= skip:
                self._position += entry['length']
                skip -= entry['length']
                continue

            new_entry = entry.copy()
          
            if skip:
                new_entry['offset'] = skip
                self._position += skip
                skip = 0

            self._current_files.append(new_entry)

        return self._position

    def readable(self):
        """\
        Return True if the stream can be read from. If False, `read()` will
        raise IOError.
        """
        return True

    def seekable(self):
        """\
        Return True if the stream supports random access. If False, `seek()`,
        `tell()` and `truncate()` will raise IOError.
        """
        return True
