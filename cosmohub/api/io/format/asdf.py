"""\
Add a ASDF header to an existing stream of record array data.
"""

from __future__ import absolute_import

import asdf
import textwrap

from astropy.io import fits
from cStringIO import StringIO

from .base import BaseFormat

class AsdfFile(BaseFormat):
    """\
    Add a ASDF header to an existing stream of record array data.
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
    
    def _initialize(self):
        """\
        Build the CSV header from the field names
        """
        columns = [
            fits.Column(name=str(c[0]), format=self._dtype[c[1]]) # @UndefinedVariable
            for c in self._description
        ]
        
        thdu = fits.BinTableHDU.from_columns(columns) # @UndefinedVariable
        rows = self._fd_length / int(thdu.header['NAXIS1'])
        
        tree = {
            'catalog' : asdf.Stream([rows], thdu.columns.dtype.newbyteorder('>'))
        }
        
        header = StringIO()
        asdf.AsdfFile(tree).write_to(header)
        
        self._header = header.getvalue()
        
        super(AsdfFile, self)._initialize()
