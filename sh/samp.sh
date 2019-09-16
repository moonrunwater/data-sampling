#!/bin/bash
# author:huohu

USER_PWD='user:pwd'
IP1='***.***.***.***'
IP2='***.***.***.***'
IP3='***.***.***.***'

core='***.com:3306'
ncore='***.com:3306'
bi='***.com:3306'

function python_samp() {
    database=$1
    srchost=$2
    group=$3
    dsthost=`eval echo '$'$3` # 将字符串作为变量名
    # echo $database $srchost $dsthost

    rm -rf ~/code/$group/${database}/data-sampling/logs/
    rm -rf /tmp/python/samplings/mysql/${database}/
    echo "rm -rf $group:$database logs and json.dir"

    echo "BEGIN: $group:$database"
    cd ~/code/$group/${database}/data-sampling
    /usr/local/bin/python3 -m samplings -t mysql -d ${database} -sh ${srchost} -su ${USER_PWD} -dh ${dsthost} -du ${USER_PWD} --steps 10000 >/dev/null 2>&1
    echo "========== END: $group:$database =========="
}

DBS=(
    # core
    "sms $IP1:10080 core"

    # ncore
    "galaxy $IP2:10081 ncore"
    "activity $IP2:10082 ncore"

    # bi
    "app $IP3:10088 bi"
)

function samp_1by1() {
    for ((i=0; i<${#DBS[*]}; i++)); do
        echo $i ${DBS[i]} | awk '{printf("%s %s %s %s\n", $1, $2, $3, $4)}'
        db=$(echo ${DBS[$i]} | awk '{print $1}')
        srchost=$(echo ${DBS[$i]} | awk '{print $2}')
        dsthost=$(echo ${DBS[$i]} | awk '{print $3}')
        python_samp $db $srchost $dsthost
    done
    echo "========== ALL DONE =========="
}

# samp one by one
samp_1by1
