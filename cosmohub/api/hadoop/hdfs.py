import collections
import copy
import io
import os
import pkg_resources
import struct
import thriftpy2

from thriftpy2.transport import TMemoryBuffer
from thriftpy2.http import TFileObjectTransport
from thriftpy2.protocol import TCompactProtocol

parquet_thrift = thriftpy2.load(pkg_resources.resource_filename('cosmohub.resources', 'parquet.thrift'), module_name="parquet_thrift")

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

        status = self._client.status(self._path)
        if status['type'] == 'FILE' and status['length']>0:
            self._all_files.append({
                'name': os.path.basename(self._path),
                'length': status['length'],
                'offset': 0,
            })
            self._path = os.path.dirname(self._path)
            self._length += status['length']

        else:
            for _, entry in self._client.list(self._path, status=True):
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
        
        with self._client.read(file_path, offset=entry['offset'], length=length, buffer_size=len(b)) as fd:
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

class HDFSParquetReader(HDFSPathReader):
    """\
    Read an HDFS Parquet dataset (either file or directory) as a stream of bytes.
  
    If the path refers to a directory, the contents of the files (ordered by
    name) are concatenated in the resulting stream. 
    The offsets of each FileMetaData are adjusted and merged into one.
    """

    def _initialize(self):
        """\
        Open the requested path and compute some internal parameters.

        Those parameters are needed to calculate the final size of the stream or
        to be able to seek inside the stream.
        """
        self._header = b''
        self._footer = b''
        
        self._length = 0
        self._position = 0
        self._all_files = collections.deque()
        
        fmd_merged = None
        offset = 0

        status = self._client.status(self._path)
        if status['type']=='FILE' and status['length']>0:
            self._all_files.append({
                'name': os.path.basename(self._path),
                'length': status['length'],
                'offset': 0,
            })
            self._path = os.path.dirname(self._path)
            self._length = status['length']

        else:
            self._header = b'PAR1'

            for _, entry in self._client.list(self._path, status=True):
                if entry['type'] != 'FILE' or entry['length']==0:
                    continue
                
                with self._client.read(os.path.join(self._path, entry['pathSuffix']), offset=entry['length']-8, length=4) as fd:
                    fmd_len = struct.unpack('<i', fd.read(4))[0]
                
                with self._client.read(os.path.join(self._path, entry['pathSuffix']), offset=entry['length']-fmd_len-8, length=fmd_len) as fd:
                    tbuf = TFileObjectTransport(fd)
                    tprot = TCompactProtocol(tbuf)
                    tfmd = parquet_thrift.FileMetaData()
                    tfmd.read(tprot)
                    
                    if fmd_merged is None:
                        fmd_merged = copy.deepcopy(tfmd)
                        offset += entry['length'] - fmd_len -12
                    else:
                        for rg in tfmd.row_groups:
                            rg = copy.deepcopy(rg)
                            for c in rg.columns:
                                if c.file_offset:
                                    c.file_offset += offset
                                if c.meta_data.data_page_offset:
                                    c.meta_data.data_page_offset += offset
                            fmd_merged.row_groups.append(rg)
                        offset += entry['length'] - fmd_len -12
                
                self._all_files.append({
                    'name': entry['pathSuffix'],
                    'length': entry['length'] - fmd_len - 8,
                    'offset': 4,
                })
                self._length += entry['length'] - fmd_len - 12
            
            tmem = TMemoryBuffer()
            tprot = TCompactProtocol(tmem)
            fmd_merged.write(tprot)
            self._footer = tmem.getvalue()
            self._footer += struct.pack('<i', len(self._footer)) + b'PAR1'
        
        self._current_files = copy.deepcopy(self._all_files)
