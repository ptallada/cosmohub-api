"""\
Patch the FileMetaData provided by HDFSParquetReader to generate a single Parquet file.
"""
import pkg_resources
import struct
import textwrap
import thriftpy2

from thriftpy2.transport import TMemoryBuffer
from thriftpy2.protocol import TCompactProtocol

from .base import BaseFormat

parquet_thrift = thriftpy2.load(pkg_resources.resource_filename('cosmohub.resources', 'parquet.thrift'), module_name="parquet_thrift")

class ParquetFile(BaseFormat):
    """\
    Load header and footer from custom reader.
    """
    
    compression_config = textwrap.dedent(
        """\
        SET hive.exec.compress.output=false;
        SET mapreduce.output.fileoutputformat.compress=false;
        """
    )
    row_format = textwrap.dedent(
        """\
        STORED AS PARQUET
        """
    )
    
    def __init__(self, fd, description, comments):
        super(ParquetFile, self).__init__(fd, description)
        
        self._header = b'PAR1'
        fmd = fd._filemetadata # FIXME: Refactor

        # Rename columns
        colname_map = {}
        for i, column in enumerate(description):
            colname_map['_col' + str(i)] = column[0]

        for el in fmd.schema:
            if el.name in colname_map:
                el.name = colname_map[el.name]

        for rg in fmd.row_groups:
            for c in rg.columns:
                if c.meta_data.path_in_schema[0] in colname_map:
                    c.meta_data.path_in_schema[0] = colname_map[c.meta_data.path_in_schema[0]]

        # Add comment
        fmd.key_value_metadata.append(parquet_thrift.KeyValue('comments', comments))

        # Serialize footer
        tmem = TMemoryBuffer()
        tprot = TCompactProtocol(tmem)
        fmd.write(tprot)
        self._footer = tmem.getvalue()
        self._footer += struct.pack('<i', len(self._footer)) + b'PAR1'
