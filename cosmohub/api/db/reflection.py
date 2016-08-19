import pandas as pd
import textwrap

from sqlalchemy import create_engine

def reflect_catalog_columns(metastore_uri, database):
    engine = create_engine(metastore_uri)

    sql = textwrap.dedent("""\
    SELECT
        tb."TBL_NAME" as tb_name,
        cd."INTEGER_IDX" AS idx, cd."COLUMN_NAME" AS name, cd."TYPE_NAME" AS type, cd."COMMENT" AS comment,
        COALESCE(cs."LONG_LOW_VALUE", cs."DOUBLE_LOW_VALUE", ps.min) AS min,
        COALESCE(cs."LONG_HIGH_VALUE", cs."DOUBLE_HIGH_VALUE", ps.max) AS max
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
    LEFT JOIN (
        SELECT 
            "DB_NAME", "TABLE_NAME", "COLUMN_NAME",
            MIN(COALESCE(ps."LONG_LOW_VALUE", ps."DOUBLE_LOW_VALUE")) AS min,
            MIN(COALESCE(ps."LONG_HIGH_VALUE", ps."DOUBLE_HIGH_VALUE")) AS max
        FROM "PART_COL_STATS" as ps
        GROUP BY "DB_NAME", "TABLE_NAME", "COLUMN_NAME"
    ) AS ps
        ON ps."DB_NAME" = db."NAME"
        AND ps."TABLE_NAME" = tb."TBL_NAME"
        AND ps."COLUMN_NAME" = cd."COLUMN_NAME"
    
    WHERE db."NAME" = %(database)s
    ORDER BY tb."TBL_ID", cd."CD_ID", idx
    """)

    data = pd.read_sql(
        sql, engine, index_col=['tb_name', 'idx'],
        params={'database' : database},
    )

    # Replace all NaN with nulls to stick to JSON specification
    return data.where(pd.notnull(data), None)
