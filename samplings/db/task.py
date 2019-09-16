#!/usr/bin/python3
# -*- coding: UTF-8 -*-

import json
import logging

from samplings.common import filetools
from samplings.common.tools import JsonEncoderX, now_time_millis, tostr
from samplings.db.base import Const, dingding_inconsistency, query, wrap

logger = logging.getLogger(__name__)
result_logger = logging.getLogger('result')

#################### SQL ####################

SQL_COUNT = '''SELECT COUNT(*) FROM {table};'''

SQL_SELECT_ALL_ITERATE = '''
SELECT
    {md5_concat} as md5_value
FROM {table};
'''

SQL_SINGLE_PK_GREATER_LIMIT_ITERATE = '''
SELECT
    {id},
    {md5_concat} as md5_value
FROM {table}
  WHERE {id} > %s
  ORDER BY {id} 
  LIMIT %s
'''

SQL_SINGLE_PK_RANGE_ITERATE = '''
SELECT {id},
    {md5_concat} as md5_value
FROM {table}
    WHERE {id} > %s AND {id} <= %s
    ORDER BY {id};
'''

SQL_COMPOSITE_PK_GREATER_LIMIT_ITERATE = '''
SELECT
    {pk_fields}, 
    {md5_concat} as md5_value
FROM {table}
    WHERE {begin_where}
    ORDER BY {pk_fields}
    LIMIT %s
'''

SQL_COMPOSITE_PK_RANGE_ITERATE = '''
SELECT
    {pk_fields}, 
    {md5_concat} as md5_value
FROM {table}
    WHERE ({begin_where}) AND ({end_where})
    ORDER BY {pk_fields}
'''

SQL_BY_ID = '''
SELECT *, {md5_concat} as md5_value
FROM {table}
WHERE ({pk_fields})=({value_params})
'''

#################### PK_TYPE ####################

# SINGLE PK
PK_AUTO_INCREMENT = 'PK_AUTO_INCREMENT'
PK_VARCHAR_INT = 'PK_VARCHAR_INT'
PK_OTHER_TYPE = 'PK_OTHER_TYPE'

# COMPOSITE PK
PK_COMPOSITE = 'PK_COMPOSITE'
PK_COMPOSITE_ABOVE_3 = 'PK_COMPOSITE_ABOVE_3'

# NO PK
PK_NO_PK = 'PK_NO_PK'

#################### class ####################

class TableCmpIterateTask:

    @classmethod
    def empty(cls):
        return cls(None, None, None)

    @classmethod
    def from_json(cls, body):
        task = cls.empty()
        task.__dict__ = json.loads(body)
        return task

    def __init__(self, table, pks, md5_concat):
        self.table = table # 表名
        self.pks = pks # 主键列 [{'name':, 'data_type':, 'last_max':}]
        self.pk_fields = None if pks == None else ','.join([wrap(pk['name']) for pk in self.pks])
        self.md5_concat = md5_concat # md5(concat(所有列)) 字符串

        self.pk_type = None # 主键类型, 如 PK_AUTO_INCREMENT、PK_COMPOSITE ...

        self.table_structure_matched = None # 表结构是否匹配

        self.count = 0 # 表总记录数
        self.got_stage = 0 # 当前执行阶段, 已获取记录数
        self.got_already = 0 # 已获取记录数
        self.finished = False # 表迭代比较任务是否完成

        self.create_time_millis = now_time_millis() # 创建时间戳(ms)
        self.update_time_millis = None # 更新时间戳(ms)

    def to_json(self):
        return json.dumps(self.__dict__, ensure_ascii=False, cls=JsonEncoderX)

    def save(self):
        self.update_time_millis = now_time_millis()
        filetools.write(Const.SAVE_DIR + self.table + Const.SUFFIX, self.to_json())

    def set_count(self):
        result_tuples = query(Const.SRC_POOL, SQL_COUNT.format(table=wrap(self.table)))
        self.count = result_tuples[0][0]
        logger.info("===== %s, db=%s table=%s, count=%s", Const.DB_TYPE, Const.DATABASE, self.table, self.count)

    def set_table_structure_matched(self, matched):
        self.table_structure_matched = matched

    def classify_pk_type(self):
        '''
        划分主键类别
        '''
        # 1. 无主键
        if len(self.pks) < 1:
            self.pk_type = PK_NO_PK
            self.msg = 'NO PK in table[%s]!' % self.table
            logger.error(self.msg)
        # 2. 单主键
        elif len(self.pks) == 1:
            # 2.1 自增主键
            if self.pks[0].get('extra') == 'auto_increment':
                self.pk_type = PK_AUTO_INCREMENT
                self.pks[0]['last_max'] = Const.MIN_NUM
            # 2.2 非自增主键: varchar, *int*
            elif self.pks[0]['data_type'] in ['varchar', 'char'] or self.pks[0]['data_type'].find('int') > -1:
                self.pk_type = PK_VARCHAR_INT
                self.pks[0]['last_max'] = Const.MIN_STR if self.pks[0]['data_type'] in ['varchar', 'char'] else Const.MIN_NUM
            # 2.3 非自增主键: 其它类型
            else:
                self.pk_type = PK_OTHER_TYPE
                self.msg = 'NOT varchar nor int PK in table[%s]!' % self.table
                logger.error(self.msg)
        # 3. 联合主键
        elif len(self.pks) <= 3:
            self.pk_type = PK_COMPOSITE
            for pk in self.pks:
                pk['last_max'] = Const.MIN_STR if pk['data_type'] in ['varchar', 'char'] else Const.MIN_NUM
        else:
            self.pk_type = PK_COMPOSITE_ABOVE_3
            self.msg = 'table[%s] pks num > 3!' % self.table
            logger.error(self.msg)

    def cmp_iterate(self):
        not_matches = []
        while True:
            got_this_time, not_matches_this_time = self._cmp_iterate_0()
            self.got_already += got_this_time
            self.got_stage += got_this_time
            logger.info('========== %s: got_this_time=%s, got_already=%s/%s, got_stage=%s/%s',
                self.table, got_this_time, self.got_already, self.count, self.got_stage,
                Const.MAX_STAGE if Const.MAX_STAGE < self.count else self.count)

            if not_matches_this_time:
                not_matches.extend(not_matches_this_time)

            if self.pk_type in [PK_NO_PK, PK_OTHER_TYPE, PK_COMPOSITE_ABOVE_3]:
                self.finished = True
                self.save()
                break
            else:
                # 不添加 or self.got_already >= self.count 作为结束条件:
                # count 时间在最开始, 执行过程中不断有增量同步新添加的记录
                if got_this_time < Const.STEPS:
                    self.finished = True
                    self.save()
                    self._double_check_log_dingding(not_matches)
                    break
                elif self.got_stage >= Const.MAX_STAGE:
                    self.got_stage = 0
                    self.save()
                    self._double_check_log_dingding(not_matches)
                    break
                else:
                    self.save()

    def _cmp_iterate_0(self):
        '''
        got_this_time, not_matches_this_time
        '''
        if self.pk_type in [PK_AUTO_INCREMENT, PK_VARCHAR_INT]:
            return self._single_pk_iterate()
        elif self.pk_type == PK_COMPOSITE:
            return self._composite_pk_iterate()
        else:
            if self.count < 10 * 10000:
                return self._select_all_iterate()
            else:
                logger.error('===== pk_type=%s && count > 100000 -> DO NOT SELECT ALL, SKIP! (%s %s.%s, count=%s)',
                    self.pk_type, Const.DB_TYPE, Const.DATABASE, self.table, self.count)
                return -1, []

    def _single_pk_iterate(self):
        # 从本次最小 id 开始(不包含), 按序查出接下来的 STEPS 条记录
        this_min = self.pks[0]['last_max']
        src_sql = SQL_SINGLE_PK_GREATER_LIMIT_ITERATE.format(
            table=wrap(self.table),
            id=wrap(self.pks[0]['name']),
            md5_concat=self.md5_concat)
        src_result_tuples = query(Const.SRC_POOL, src_sql, this_min, Const.STEPS)

        got_this_time = len(src_result_tuples)
        not_matches_this_time = []
        if got_this_time > 0:
            # 更新本次查询到的最大 id
            self.pks[0]['last_max'] = this_max = src_result_tuples[-1][0]
            # 按照区间, 查询目标表
            dest_sql = SQL_SINGLE_PK_RANGE_ITERATE.format(
                table=wrap(self.table),
                id=wrap(self.pks[0]['name']),
                md5_concat=self.md5_concat)
            dest_result_tuples = query(Const.DEST_POOL, dest_sql, this_min, this_max)
            # 比较记录的 md5 值
            not_matches_this_time = self._cmp_md5(src_result_tuples, dest_result_tuples)

        # 返回本次查询到记录数
        return got_this_time, not_matches_this_time

    def _composite_pk_iterate(self):
        # 本次最小 id
        pk0_begin = self.pks[0]['last_max']
        pk1_begin = self.pks[1]['last_max']
        pk2_begin = None

        begin_where = None
        end_where = None
        if len(self.pks) == 2:
            # (A, B) > (x, y) 等价于 A>x OR (A=x AND B>y)
            # 经实测, 前面的简单写法在 MySQL 中也用到了索引，但就是比后面的写法慢
            begin_where = "{pk0}>%s OR ({pk0}=%s AND {pk1}>%s)".format(pk0=wrap(self.pks[0]['name']), pk1=wrap(self.pks[1]['name']))
            end_where = "{pk0}<%s OR ({pk0}=%s AND {pk1}<=%s)".format(pk0=wrap(self.pks[0]['name']), pk1=wrap(self.pks[1]['name']))
        else:
            # (A, B, C) > (x, y, z) 等价于 A>x OR (A=x AND (B>y OR (B=y AND C>z)))
            # 所有 > 3 主键列的表, 数据量都 < 10000, 采用前面的简单写法
            begin_where = '({pk_fields}) > (%s, %s, %s)'.format(pk_fields=self.pk_fields)
            end_where = '({pk_fields}) <= (%s, %s, %s)'.format(pk_fields=self.pk_fields)
            pk2_begin = self.pks[2]['last_max']

        # 从本次最小 id 开始(不包含), 按序查出接下来的 STEPS 条记录
        src_args = self._build_args([pk0_begin, pk1_begin, pk2_begin])
        src_args.append(Const.STEPS)
        src_sql = SQL_COMPOSITE_PK_GREATER_LIMIT_ITERATE.format(
            table=wrap(self.table),
            md5_concat=self.md5_concat,
            pk_fields=self.pk_fields,
            begin_where=begin_where)
        src_result_tuples = query(Const.SRC_POOL, src_sql, *src_args)

        got_this_time = len(src_result_tuples)
        not_matches_this_time = []
        if got_this_time > 0:
            # 更新本次查询到的联合主键最大值, 排除最后一个(md5_concat)
            for i in range(len(src_result_tuples[-1]) - 1):
                self.pks[i]['last_max'] = src_result_tuples[-1][i]
            # 按照区间, 查询目标表
            dest_sql = SQL_COMPOSITE_PK_RANGE_ITERATE.format(
                table=wrap(self.table),
                md5_concat=self.md5_concat,
                pk_fields=self.pk_fields,
                begin_where=begin_where,
                end_where=end_where)
            begin_args = self._build_args([pk0_begin, pk1_begin, pk2_begin])
            end_args = self._build_args([pk['last_max'] for pk in self.pks])
            dest_args = begin_args + end_args
            dest_result_tuples = query(Const.DEST_POOL, dest_sql, *dest_args)
            # 比较记录的 md5 值
            not_matches_this_time = self._cmp_md5(src_result_tuples, dest_result_tuples)

        # 返回本次查询到记录数
        return got_this_time, not_matches_this_time

    def _select_all_iterate(self):
        sql = SQL_SELECT_ALL_ITERATE.format(
            table=wrap(self.table),
            md5_concat=self.md5_concat)
        src_result_tuples = query(Const.SRC_POOL, sql)

        got_this_time = len(src_result_tuples)
        not_matches_this_time = []
        if got_this_time > 0:
            # 查询目标表
            dest_result_tuples = query(Const.DEST_POOL, sql)
            # 比较记录的 md5 值
            not_matches_this_time = self._cmp_md5(src_result_tuples, dest_result_tuples)

        # 返回本次查询到记录数
        return got_this_time, not_matches_this_time

    def _cmp_md5(self, src_result_tuples, dest_result_tuples):
        '''
        return [(pk0, pk1, pk2, md5), (pk0, pk1, pk2, md5)]
        '''
        src_result_dict = self._tuples_to_dict(src_result_tuples)
        dest_result_dict = self._tuples_to_dict(dest_result_tuples)

        not_matches = []
        for (id, record_tup) in src_result_dict.items():
            if id not in dest_result_dict or record_tup[-1] != dest_result_dict[id][-1]:
                not_matches.append(record_tup)

        return not_matches
    
    def _tuples_to_dict(self, result_tuples):
        '''
        {'pk0,pk1,pk2': (pk0, pk1, pk2, md5)}
        '''
        if len(self.pks) == 1:
            return {id : (id, md5) for (id, md5) in result_tuples}
        elif len(self.pks) == 2:
            return {','.join([tostr(pk0), tostr(pk1)]) : (pk0, pk1, md5)
                for (pk0, pk1, md5) in result_tuples}
        elif len(self.pks) == 3:
            return {','.join([tostr(pk0), tostr(pk1), tostr(pk2)]) : (pk0, pk1, pk2, md5)
                for (pk0, pk1, pk2, md5) in result_tuples}
        else:
            return {md5 : (md5, ) for (md5, ) in result_tuples}

    def _build_args(self, args):
        return [args[0], args[0], args[1]] if len(self.pks) == 2 else [args[0], args[1], args[2]]

    def _double_check_log_dingding(self, not_matches):
        if not not_matches:
            return

        if self.pk_type not in [PK_AUTO_INCREMENT, PK_VARCHAR_INT, PK_COMPOSITE]:
            return

        errors = []
        sql = SQL_BY_ID.format(
            table=wrap(self.table),
            pk_fields=self.pk_fields,
            md5_concat=self.md5_concat,
            value_params=', '.join(['%s' for i in range(len(self.pks))])
        )

        for record_tup in not_matches:
            src_result_tuples = query(Const.SRC_POOL, sql, *record_tup[0:-1])
            dest_result_tuples = query(Const.DEST_POOL, sql, *record_tup[0:-1])

            error_dict = {
                'db_type': Const.DB_TYPE,
                'database': Const.DATABASE,
                'table': self.table,
                'pk_fields': self.pk_fields
            }

            if not src_result_tuples:
                if dest_result_tuples:
                    # str(dest_result_tuples[0]) 避免数据库字段对应类转换为 json 异常
                    error_dict.update({'error': Const.NO_ID_IN_SRC, 'dest':str(dest_result_tuples[0])})
                    errors.append(error_dict)
            elif not dest_result_tuples:
                error_dict.update({'error': Const.NO_ID_IN_DEST, 'src':str(src_result_tuples[0])})
                errors.append(error_dict)
            elif src_result_tuples[0][-1] != dest_result_tuples[0][-1]:
                error_dict.update({'error': Const.MD5_NOT_EQUAL, 'src':str(src_result_tuples[0]), 'dest':str(dest_result_tuples[0])})
                errors.append(error_dict)

        if errors:
            result_logger.info(json.dumps(errors, ensure_ascii=False, cls=JsonEncoderX))
            dingding_inconsistency('data inconsistency', errors)
