import pandas as pd
import textwrap

from sqlalchemy import create_engine

from . import app

def reflect_catalog_columns():
    engine = create_engine(app.config['HIVE_METASTORE_URI'])
    
    sql = textwrap.dedent("""\
    SELECT
        tb."TBL_NAME" as tb_name,
        cd."INTEGER_IDX" AS idx, cd."COLUMN_NAME" AS name, cd."TYPE_NAME" AS type, cd."COMMENT" AS comment,
        COALESCE(cs."LONG_LOW_VALUE", cs."DOUBLE_LOW_VALUE") AS min,
        COALESCE(cs."LONG_HIGH_VALUE", cs."DOUBLE_HIGH_VALUE") AS max
    FROM "DBS" AS db
    JOIN "TBLS" AS tb
        ON db."DB_ID" = tb."DB_ID"
    JOIN "SDS" AS sd
        ON sd."SD_ID" = tb."SD_ID"
    JOIN "CDS"
        ON "CDS"."CD_ID" = sd."CD_ID"
    JOIN "COLUMNS_V2" AS cd
        ON cd."CD_ID" = sd."CD_ID"
    LEFT JOIN "TAB_COL_STATS" AS cs
        ON cs."TBL_ID" = tb."TBL_ID" AND cs."COLUMN_NAME" = cd."COLUMN_NAME"
    WHERE db."NAME" = %(db_name)s
    ORDER BY tb."TBL_ID", cd."CD_ID", idx
    """)
    
    data = pd.read_sql(
        sql, engine, index_col=['tb_name', 'idx'],
        params={'db_name' : app.config['HIVE_DATABASE']},
    )
    
    # Replace all NaN with nulls to stick to JSON specification
    return data.where(pd.notnull(data), None)
