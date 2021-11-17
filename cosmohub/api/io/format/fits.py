"""\
Add a FITS header and padding to an existing stream of record array data.
"""
import re
import textwrap

from astropy.io import fits

from .base import BaseFormat

class FitsFile(BaseFormat):
    """\
    Add a FITS header and padding to an existing stream of record array data.
    """
    
    compression_config = textwrap.dedent(
        """\
        SET hive.exec.compress.output=false;
        SET mapreduce.output.fileoutputformat.compress=false;
        """
    )
    row_format = textwrap.dedent(
        """\
        ROW FORMAT SERDE 'es.pic.astro.hadoop.serde.RecArraySerDe'
        STORED AS
            INPUTFORMAT 'es.pic.astro.hadoop.io.BinaryOutputFormat'
            OUTPUTFORMAT 'es.pic.astro.hadoop.io.BinaryOutputFormat'
        """
    )
    
    _BLOCK_LENGTH = 2880
    
    _dtype = {
        'BIGINT_TYPE'    : 'K',
        'BOOLEAN_TYPE'   : 'L',
        'CHAR_TYPE'      : '255A',
        'DATE_TYPE'      : 'K',
        'DOUBLE_TYPE'    : 'D',
        'FLOAT_TYPE'     : 'E',
        'INT_TYPE'       : 'J',
        'SMALLINT_TYPE'  : 'I',
        'STRING_TYPE'    : '255A',
        'TIMESTAMP_TYPE' : 'K',
        'TINYINT_TYPE'   : 'B',
        'VARCHAR_TYPE'   : '255A',
    }
    
    _non_printable_re = re.compile(r'[^ -~]+')

    def __init__(self, fd, description, comments):
        """\
        Build the FITS header and footer
        """
        super(FitsFile, self).__init__(fd, description)
        
        columns = [
            fits.Column(name=str(c[0]), format=self._dtype[c[1]]) # @UndefinedVariable
            for c in self._description
        ]
        
        thdu = fits.BinTableHDU.from_columns(columns) # @UndefinedVariable
        rows = self._fd_length / int(thdu.header['NAXIS1'])
        thdu.header['NAXIS2'] = rows
        for comment in comments.split('\n'):
            thdu.header.add_comment(self._non_printable_re.sub('', comment))

        self._header = fits.PrimaryHDU().header.tostring() + thdu.header.tostring() # @UndefinedVariable
        
        if self._fd_length % self._BLOCK_LENGTH:
            self._footer = '\0' * (self._BLOCK_LENGTH - (self._fd_length % self._BLOCK_LENGTH))
