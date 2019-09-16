#!/usr/bin/python3
# -*- coding: UTF-8 -*-

from samplings.db.base import query
from samplings.db.samp import Sampling

#################### SQL ####################

SQL_LIST_DBS = '''
SELECT datname FROM pg_database WHERE datistemplate=FALSE;
'''

SQL_LIST_TABLES = '''
SELECT tablename FROM pg_tables WHERE schemaname='public';
'''

SQL_LIST_COLUMNS = '''
SELECT pag.attname as name, pt.typname
FROM (
	SELECT attname, attnum, atttypid FROM pg_attribute
	WHERE attrelid IN (SELECT oid FROM pg_class WHERE relname=%s)
		AND attnum>0
) pag INNER JOIN pg_type pt ON pt.oid=pag.atttypid
ORDER BY pag.attnum;
'''

SQL_GET_PK = '''
SELECT indkey
FROM pg_index i INNER JOIN pg_class c 
	ON c.oid=i.indrelid
WHERE c.relname = %s
	AND i.indisprimary=TRUE;
'''

SQL_CREATE_TABLE = '''
(SELECT concat_ws(',', "table_name", "column_name", "ordinal_position", "is_nullable", "data_type") val_concat
FROM information_schema.COLUMNS 
WHERE table_schema='public' AND table_name=%s
ORDER BY ordinal_position)
UNION ALL
(SELECT concat_ws(',', indexname, indexdef) val_concat
FROM pg_indexes
WHERE schemaname='public' AND tablename=%s
ORDER BY indexname)
'''

#################### class ####################

class PgsqlSampling(Sampling):

    def __init__(self):
        pass

    def _list_tables(self, pool, db):
        table_tuples = query(pool, SQL_LIST_TABLES)
        return [table for (table,) in table_tuples] 

    def _list_columns_pks(self, pool, db, table):
        # 查询表所有列信息
        column_tuples = query(pool, SQL_LIST_COLUMNS, table)
        # 列信息 list
        columns = [
            {
                'name':name,
                'data_type':data_type
            } for (name, data_type) in column_tuples
        ]

        # 主键列信息, 注: 联合主键时, 主键列有多列
        pk_tuples = query(pool, SQL_GET_PK, table)
        pks = [columns[int(i)-1] for (i,) in pk_tuples]

        return (columns, pks)

    def _build_md5_concat(self, column_names):
        # https://www.postgresql.org/docs/9.6/functions-string.html
        # NULL arguments are ignored.
        # SELECT md5(concat("id","sso_userid","name","email","inserted_at")) FROM "users";
        return "md5(concat(" + ",".join([ '"%s"' % name for name in column_names]) + "))"

    def _table_structure(self, pool, db, table):
        val_concat_tuples = query(pool, SQL_CREATE_TABLE, table, table)
        return ';'.join(sorted([val_concat for (val_concat, ) in val_concat_tuples])).replace('public.', '')