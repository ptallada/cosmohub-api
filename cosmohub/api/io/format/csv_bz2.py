"""\
Add a proper CSV header to an existing CSV data stream.
"""
import bz2
import textwrap

from .base import BaseFormat

class CsvBz2File(BaseFormat):
    """\
    Add a proper CSV header to an existing CSV data stream.
    """
    
    compression_config = textwrap.dedent(
        """\
        SET hive.exec.compress.output=true;
        SET mapreduce.output.fileoutputformat.compress=true;
        SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.BZip2Codec;
        """
    )
    row_format = textwrap.dedent(
        """\
        ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
        """
    )
    
    def __init__(self, fd, description):
        """\
        Build the CSV header from the field names
        """
        super(CsvBz2File, self).__init__(fd, description)
        
        self._header = bz2.compress(','.join(f[0] for f in description) + '\n')
