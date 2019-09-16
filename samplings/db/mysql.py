#!/usr/bin/python3
# -*- coding: UTF-8 -*-

from samplings.db.base import query
from samplings.db.samp import Sampling

#################### SQL ####################

# AND TABLE_TYPE='BASE TABLE' 排除 VIEW
SQL_LIST_TABLES = '''
SELECT TABLE_NAME FROM information_schema.TABLES
    WHERE TABLE_SCHEMA=%s AND TABLE_TYPE='BASE TABLE'
    ORDER BY TABLE_NAME;
'''
# SHOW FULL TABLES WHERE TABLE_TYPE='BASE TABLE';

SQL_LIST_COLUMNS = '''
SELECT COLUMN_NAME, DATA_TYPE, COLUMN_KEY, EXTRA FROM information_schema.COLUMNS 
    WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s
    ORDER BY ORDINAL_POSITION;
'''

SQL_GET_PK = '''
SELECT COLUMN_NAME, SEQ_IN_INDEX
    FROM information_schema.STATISTICS
    WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND INDEX_NAME='PRIMARY';
'''

SQL_CREATE_TABLE = '''
(SELECT CONCAT_WS(',', COLUMN_NAME, ORDINAL_POSITION, IS_NULLABLE, DATA_TYPE, COLUMN_TYPE, COLUMN_KEY, EXTRA) val_concat
FROM information_schema.COLUMNS 
WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s
ORDER BY ORDINAL_POSITION)
UNION ALL
(SELECT CONCAT_WS(',', index_name, seq_in_index, column_name, index_type, non_unique) val_concat
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s
ORDER BY index_name);
'''

# SHOW INDEX FROM {table};

#################### class ####################

class MysqlSampling(Sampling):

    def __init__(self):
        pass

    def _list_tables(self, pool, db):
        table_tuples = query(pool, SQL_LIST_TABLES, db)
        return [table for (table,) in table_tuples]

    def _list_columns_pks(self, pool, db, table):
        # 查询表所有列信息
        column_tuples = query(pool, SQL_LIST_COLUMNS, db, table)
        # 列信息 list
        columns = [
            {
                'name':name,
                'data_type':data_type,
                'key':key,
                'extra':extra
            } for (name, data_type, key, extra) in column_tuples
        ]
        column_dict = {column['name'] : column for column in columns}

        # 主键列信息, 注: 联合主键时, 主键列有多列
        pk_tuples = query(pool, SQL_GET_PK, db, table)
        pks = [column_dict[name] for (name, seq) in pk_tuples]
        # 表中无显式指定主键, MySQL 会将索引列作为主键
        if len(pks) < 1:
            pks = [column for column in columns if column['key'] == 'PRI']
        
        return (columns, pks)

    def _build_md5_concat(self, column_names):
        # https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_concat
        # return "MD5(CONCAT(IFNULL(`" + "`,''),IFNULL(`".join(column_names) + "`,'')))"
        return "MD5(CONCAT(%s))" % ','.join([ "IFNULL(`%s`,'')" % name for name in column_names])

    def _table_structure(self, pool, db, table):
        # https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_concat-ws
        val_concat_tuples = query(pool, SQL_CREATE_TABLE, db, table, db, table)
        # 排序后再 join
        return ';'.join(sorted([val_concat for (val_concat, ) in val_concat_tuples]))
