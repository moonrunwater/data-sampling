#!/bin/bash
# author:wangming


function python_samp() {
    srchost=$1
    srcdb=$2
    dsthost=$3
    dstdb=$4
    echo "BEGIN: src $srchost, $srcdb; dst $dsthost, $dstdb ..."

    # 清除校验进度
    rm -rf /tmp/python/samplings

    # 执行校验
    python3 -m samplings \
        -t mysql \
        -sh ${srchost} \
        -d $srcdb \
        -su user:pwd \
        -dh $dsthost \
        -dd $dstdb \
        -du user:pwd \
        --steps 10000 \
    >/dev/null 2>&1

    echo "END: src $srchost, $srcdb; dst $dsthost, $dstdb."
}

# 查看进程
# ps -ef | grep samplings | grep -v grep

# 查看日志、进度
# tail -f /home/sysop/huohu/python/data-sampling/logs/info.log
# grep ">>>>>>>>>>" /home/sysop/huohu/python/data-sampling/logs/info.log

# 同步错误信息
# 会发到 "DTS 同步数据校验" 钉钉群里

source /home/sysop/.bashrc

# 清除日志
rm -rf /home/sysop/huohu/python/data-sampling/logs

# 进入工程根目录
cd /home/sysop/huohu/python/data-sampling
echo "PWD=$PWD"

# 执行前需要设置一下所需的 virtualenv
workon data-sampling
echo "VIRTUAL_ENV=$VIRTUAL_ENV"

echo "========== 商品 =========="
python_samp "aliyuncs.com:3306" "product" "aliyuncs.com:3306" "product"
echo "========== 订单 =========="
python_samp "aliyuncs.com:3306" "order" "aliyuncs.com:3306" "order"
echo "========== 履约 =========="
python_samp "aliyuncs.com:3306" "performance" "aliyuncs.com:3306" "performance"

# 清除环境
deactivate
echo "VIRTUAL_ENV=$VIRTUAL_ENV"

# 查看日志
grep ">>>>>>>>>>" /home/sysop/huohu/python/data-sampling/logs/info.log

# mysql --help
# -h, --host=name
# -p, --password[=name]
# -u, --user=name
# -P, --port=#

# $ mysql -h aliyuncs.com -P 3306 -u user -p
# Enter password: 
# MySQL [(none)]> show databases;
# +--------------------+
# | Database           |
# +--------------------+
# | information_schema |
# | order          |
# +--------------------+
# 2 rows in set (0.00 sec)
# MySQL [(none)]> use order;
# MySQL [order]> show tables;
# MySQL [order]> exit
# Bye

nohup \
python3 -m samplings \
    -t mysql \
    -sh aliyuncs.com:3306 \
    -d order \
    -su user:pwd \
    -dh aliyuncs.com:3306 \
    -dd order \
    -du user:pwd \
    --steps 10000 \
>/dev/null 2>&1 &


nohup \
python3 -m samplings \
    -t mysql \
    -sh aliyuncs.com:3306 \
    -d product \
    -su user:pwd \
    -dh aliyuncs.com:3306 \
    -dd product \
    -du user:pwd \
    --steps 10000 \
>/dev/null 2>&1 &


nohup \
python3 -m samplings \
    -t mysql \
    -sh aliyuncs.com:3306 \
    -d performance \
    -su user:pwd \
    -dh aliyuncs.com:3306 \
    -dd performance \
    -du user:pwd \
    --steps 10000 \
>/dev/null 2>&1 &
