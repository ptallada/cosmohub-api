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
        ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
        STORED AS TEXTFILE
        """
    )
    
    def __init__(self, fd, description, comments=None):
        """\
        Build the CSV header from the field names
        """
        super(CsvBz2File, self).__init__(fd, description, comments)
        
        header = '# ' + '\n# '.join(comments.split('\n')) +'\n'
        header += ','.join(f[0] for f in description) + '\n'
        
        self._header = bz2.compress(header.encode('utf8'))
