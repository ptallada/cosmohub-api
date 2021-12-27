"""\
Add a FITS header and padding to an existing stream of record array data.
"""
import textwrap

from .base import BaseFormat

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
        
        # FIXME: refactor
        self._header = fd._header
        self._footer = fd._footer
