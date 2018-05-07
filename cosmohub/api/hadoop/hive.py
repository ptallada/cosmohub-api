import pandas as pd
import pyparsing as pp
import textwrap

from sqlalchemy import create_engine

class HiveQLProgress(object):
    PREFIX = pp.Suppress('INFO  :')
    COUNTER = pp.Word(pp.nums).setParseAction(lambda s, l, t: int(t[0]))
    HYPHEN = pp.Literal('-').setParseAction(lambda s, l, t: 0)
    COUNT_OR_UNDEF = (HYPHEN | COUNTER)
    
    DONE = COUNT_OR_UNDEF
    TOTAL = COUNT_OR_UNDEF
    
    ACTIVE = pp.Suppress('+') + COUNTER
    FAILED = pp.Suppress('-') + COUNTER
    
    OPEN = pp.Optional(pp.Suppress('('))
    CLOSE = pp.Optional(pp.Suppress(')'))
    COMMA = pp.Optional(pp.Suppress(','))
    
    ACTIVE_AND_FAILED =OPEN + pp.Optional(ACTIVE, default=0) + COMMA + pp.Optional(FAILED, default=0) + CLOSE
    
    PROGRESS = pp.Group(DONE + ACTIVE_AND_FAILED + pp.Suppress('/') + TOTAL)
    
    STAGE = (pp.Suppress('Map') | pp.Suppress('Reducer')) + pp.Word(pp.nums).suppress() + pp.Suppress(':') + PROGRESS
    
    GRAMMAR = PREFIX + pp.OneOrMore(STAGE)
    
    @classmethod
    def parse(cls, message):
        done, running, failed, total = (0, 0, 0, 0)
    
        try:
            stages = cls.GRAMMAR.parseString(message)
    
            for stage in stages:
                done    += stage[0]
                running += stage[1]
                failed  += stage[2]
                total   += stage[3]
    
        except pp.ParseException:
            pass
    
        return (done, running, failed, total-done)

def parse_progress(report):
    idx = {
        'TOTAL'     : report.headerNames.index('TOTAL'),
        'COMPLETED' : report.headerNames.index('COMPLETED'),
        'RUNNING'   : report.headerNames.index('RUNNING'),
        'FAILED'    : report.headerNames.index('FAILED'),
    }
    
    total = sum([int(l[idx['TOTAL']]) for l in report.rows])
    completed = sum([int(l[idx['COMPLETED']]) for l in report.rows])
    running = sum([int(l[idx['RUNNING']]) for l in report.rows])
    failed = sum([int(l[idx['FAILED']]) for l in report.rows])
    
    return (completed, running, failed, total-completed)

def reflect_catalogs(metastore_uri, database):
    engine = create_engine(metastore_uri)

    engine.execute(
        textwrap.dedent("""\
            CREATE OR REPLACE FUNCTION double_or_null(v_input text)
            RETURNS DOUBLE PRECISION AS $$
            DECLARE v_value DOUBLE PRECISION DEFAULT NULL;
            BEGIN
                BEGIN
                    v_value := v_input::DOUBLE PRECISION;
                EXCEPTION WHEN OTHERS THEN
                    RETURN NULL;
                END;
            RETURN v_value;
            END;
            $$ LANGUAGE plpgsql;
        """
        )
    )

    sql = textwrap.dedent("""\
    SELECT *
    FROM (
        SELECT
            db."NAME" AS db_name,
            tb."TBL_NAME" as tb_name,
            1 AS kind,
            cd."INTEGER_IDX" AS idx,
            cd."COLUMN_NAME" AS name,
            cd."TYPE_NAME" AS type,
            cd."COMMENT" AS comment,
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
                "DB_NAME",
                "TABLE_NAME",
                "COLUMN_NAME",
                MIN(COALESCE(ps."LONG_LOW_VALUE", ps."DOUBLE_LOW_VALUE")) AS min,
                MAX(COALESCE(ps."LONG_HIGH_VALUE", ps."DOUBLE_HIGH_VALUE")) AS max
            FROM "PART_COL_STATS" as ps
            GROUP BY "DB_NAME", "TABLE_NAME", "COLUMN_NAME"
        ) AS ps
            ON ps."DB_NAME" = db."NAME"
            AND ps."TABLE_NAME" = tb."TBL_NAME"
            AND ps."COLUMN_NAME" = cd."COLUMN_NAME"
        
        UNION
        
        SELECT
            db."NAME" AS db_name,
            tb."TBL_NAME" AS tb_name,
            0 AS kind,
            pk."INTEGER_IDX" AS idx,
            pk."PKEY_NAME" AS name,
            pk."PKEY_TYPE" AS type,
            pk."PKEY_COMMENT" AS comment,
            MIN(DOUBLE_OR_NULL(pv."PART_KEY_VAL")) AS min,
            MAX(DOUBLE_OR_NULL(pv."PART_KEY_VAL")) AS max
        FROM "PARTITIONS" AS p
        JOIN "PARTITION_KEYS" AS pk
            ON pk."TBL_ID" = p."TBL_ID"
        JOIN "PARTITION_KEY_VALS" AS pv
            ON pv."PART_ID" = p."PART_ID"
            AND pk."INTEGER_IDX" = pv."INTEGER_IDX"
        JOIN "TBLS" AS tb
            ON tb."TBL_ID" = p."TBL_ID"
        JOIN "DBS" AS db
            ON db."DB_ID" = tb."DB_ID"
        GROUP BY
            db."NAME",
            tb."TBL_NAME",
            kind,
            pk."INTEGER_IDX",
            pk."PKEY_NAME",
            pk."PKEY_TYPE",
            pk."PKEY_COMMENT"
    ) AS t
    WHERE db_name = %(database)s
    ORDER BY tb_name, kind, idx
    """)

    data = pd.read_sql(
        sql, engine, index_col=['tb_name', 'idx'],
        params={'database' : database},
    )

    # Replace all NaN with nulls to stick to JSON specification
    return data.where(pd.notnull(data), None)
