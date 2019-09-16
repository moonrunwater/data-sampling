# data-sampling
data sampling check: MySQL、PostgreSQL、MongoDB


## python 环境

```sh
# virtualenv
mkvirtualenv --python=python3 data-sampling

workon data-sampling
echo $VIRTUAL_ENV
cd data-sampling-path
echo $(pwd)
setvirtualenvproject $VIRTUAL_ENV $(pwd)

# 安装依赖
pip install -r requirements.txt

# 也可直接安装
pip install pyyaml
pip install pymongo
pip install mysql-connector-python
pip install dbutils
pip install psycopg2-binary
pip install requests
# pip freeze > requirements.txt
```


## 执行

```sh
# cd 到工程根目录
cd /.../data-sampling

# mysql
nohup \
python3 -m samplings \
    -t mysql -d hlj \
    -sh src_host:3306 \
    -su src_user:src_pwd \
    -dh dest_host:6006 \
    -du dest_user:dest_pwd \
    --steps 10000 \
>/dev/null 2>&1 &

# pgsql
nohup \
python3 -m samplings \
    -t pgsql -d magic_stg \
    -sh src_host:15432 \
    -su src_user:src_pwd \
    -dh dest_host:3432 \
    -du dest_user:dest_pwd \
    --steps 10000 \
>/dev/null 2>&1 &

# mongo
nohup \
python3 -m samplings \
    -t mongo -d profile \
    -sh src1_mongo_host:20000,src2_mongo_host:20000,src3_mongo_host:20000 \
    -su "src_user:src_pwd" \
    -dh dest1_mongo_host:20000,dest2_mongo_host:20000,dest3_mongo_host:20000 \
    -du "dest_user:dest_pwd" \
    --steps 1000 \
>/dev/null 2>&1 &
```


## 查看结果

### 日志
```sh
/tmp/python/logs/info.log
/tmp/python/logs/error.log
```

### 不一致记录
```sh
/tmp/python/logs/mysql.log
/tmp/python/logs/pgsql.log
/tmp/python/logs/mongo.log
```

### 覆盖率

```sh
# 搜索日志 /tmp/python/logs/info.log，db_got_already 可以统计该数据库抽验覆盖率
>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> db=%s, times=%s, db_got_already=%s/%s, db_task_finished=%s/%s

# 如
[2019-03-24 21:12:32,215] [MainThread] [samplings.db.samp] [INFO] [samp.py:63]: >>>>>>>>>>>>>>>>>>>>>>>>>>>>>> db=hlj, times=3, db_got_already=233208463/735625400, db_task_finished=870/949
```

## 可靠性

### 断点续跑

> 万一跑挂了，可以从断开处开始跑，而不是每次都从零开始跑，实现 >= 99.9% 覆盖率目标

```sh
/tmp/python/samplings/${dbtype}/${database}/${table}.json
```

### 钉钉提醒，了解运行状况

1. 脚本出错提醒
2. 数据不一致提醒

### 出错，发送钉钉提醒，并自动循环执行（最多 20 次）


## 待优化
1. 任务多线程执行
2. 部分表，迁移后的表结构，与原表有微小变化，需要弄清楚原因，确认是否会对业务造成影响
