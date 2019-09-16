#!/usr/bin/python3
# -*- coding: UTF-8 -*-

import abc
import logging
import os
from concurrent import futures

from samplings.common import filetools
from samplings.common.tools import logtime, now_time_millis, retry, tostr
from samplings.db.base import Const, dingding_inconsistency, query
from samplings.db.task import TableCmpIterateTask

logger = logging.getLogger(__name__)

#################### class ####################

class Sampling(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def _list_tables(self, pool, db):
        '''
        查询数据库中的所有表
        return
        '''
        pass

    @abc.abstractmethod
    def _list_columns_pks(self, pool, db, table):
        '''
        查询表中的所有列、主键
        return
        '''
        pass

    @abc.abstractmethod
    def _build_md5_concat(self, column_names):
        '''
        构建 md5(concat(所有列)) 字符串
        return
        '''
        pass

    @abc.abstractmethod
    def _table_structure(self, pool, db, table):
        '''
        表结构信息
        return str
        '''
        pass

    def main(self):
        # 查询数据库中的所有表
        tables = self._list_tables(Const.SRC_POOL, Const.DATABASE)
        # just for test
        # tables = ['act_hi_actinst', 'yimei_artisan_user', 'active_groupon_activity', 'performance_domain', 'zmw_order_finish']

        times = 0
        while True:
            times += 1
            need_continue, db_count, db_got_already, db_task_finished, tasks = self.__build_db_iterate_tasks(tables)
            logger.info('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> db=%s, times=%s, db_got_already=%s/%s, db_task_finished=%s/%s',
                Const.DATABASE, times, db_got_already, db_count, db_task_finished, len(tasks))
            
            if not need_continue:
                break

            # IO 密集型任务 -> 线程池并发 -> 迭代比较
            with futures.ThreadPoolExecutor(max_workers=Const.MAX_THREAD, thread_name_prefix='cmp_pool') as executor:
                future_list = []
                for i, task in enumerate(tasks, start=1):
                    if not task.finished:
                        logger.info('==================== db=%s: current=%s/%s, table=%s, finished=%s',
                            Const.DATABASE, i, len(tasks), task.table, task.finished)
                        future = executor.submit(task.cmp_iterate)
                        future_list.append(future)
                # 阻塞主线程, 直到线程池里面的所有任务都完成 ALL_COMPLETED
                futures.wait(future_list, return_when=futures.ALL_COMPLETED)

                # 线程抛异常, 抛出来, 避免异常被吞没
                for future in future_list:
                    # return the result of the call that the future represents,
                    # or raise Exception If the call raised,
                    # or raise CancelledError If the future was cancelled,
                    # or raise TimeoutError If the future didn't finish executing before the giventimeout
                    future.result()


    def __build_db_iterate_tasks(self, tables):
        need_continue = False
        db_count = 0
        db_got_already = 0
        db_task_finished = 0
        tasks = []

        # IO 密集型任务 -> 线程池并发 -> 迭代比较
        with futures.ThreadPoolExecutor(max_workers=Const.MAX_THREAD, thread_name_prefix='build_pool') as executor:
            future_list = []
            total = len(tables)
            for i, table in enumerate(tables, start=1):
                future = executor.submit(self.__build_table_iterate_task, table, i, total)
                future_list.append(future)
            # 阻塞主线程, 直到线程池里面的所有任务都完成 ALL_COMPLETED
            futures.wait(future_list, return_when=futures.ALL_COMPLETED)
            for future in future_list:
                task = future.result()
                # 添加到任务列表
                tasks.append(task)
                db_count += task.count
                db_got_already += task.got_already
                if task.finished:
                    db_task_finished += 1
                else:
                    need_continue = True

        return need_continue, db_count, db_got_already, db_task_finished, tasks

    def __build_table_iterate_task(self, table, i, total):
        file_path = Const.SAVE_DIR + table + Const.SUFFIX
        if os.path.exists(file_path):
            # 1. 首先从文件反序列化得到表迭代任务信息
            lines = filetools.read(file_path)
            task = TableCmpIterateTask.from_json(lines[0])
        else:
            # 2. 从数据库查询表迭代任务信息
            task = self.__build_table_iterate_task_from_db(table)

        logger.info('build_table_iterate_task OK: current=%s/%s, table=%s', i, total, table)
        return task

    def __build_table_iterate_task_from_db(self, table):
        # 查询出表的所有列、主键
        (columns, pks) = self._list_columns_pks(Const.SRC_POOL, Const.DATABASE, table)

        # MD5(CONCAT(所有列))
        md5_concat = self._build_md5_concat([column['name'] for column in columns])

        # TableCmpIterator
        task = TableCmpIterateTask(table, pks, md5_concat)
        # classify
        task.classify_pk_type()
        # count
        task.set_count()
        # table structure
        task.set_table_structure_matched(self.__cmp_table_structure(table))

        # save into file
        task.save()

        return task

    def __cmp_table_structure(self, table):
        src_table_structure = self._table_structure(Const.SRC_POOL, Const.DATABASE, table)
        dest_table_structure = self._table_structure(Const.DEST_POOL, Const.DATABASE, table)
        if src_table_structure != dest_table_structure:
            logger.error("===== %s table structure\nsrc: %s\ndst: %s", table, src_table_structure, dest_table_structure)
            msg = "%s db=%s, table=%s, %s %s" % \
                (Const.DB_TYPE, Const.DATABASE, table, 'table structure', 'NOT_EQUAL')
            logger.error(msg)
            # dingding_inconsistency('table structure inconsistency', [msg])
            return False
        else:
            logger.debug("===== %s table structure\nsrc: %s\ndst: %s", table, src_table_structure, dest_table_structure)
            return True