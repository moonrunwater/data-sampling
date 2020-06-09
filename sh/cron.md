# cron

```sh
mkdir -p /tmp/cron/
# echo '00 22 * * * /usr/bin/sh /root/bin/samp.sh > /tmp/cron/samp.sh.log' > /var/spool/cron/root
echo '00 0/2 * * * /usr/bin/sh /home/sysop/huohu/python/data-sampling/sh/samp_1by1.sh > /tmp/cron/samp_1by1.sh.log' > /var/spool/cron/root

vi /var/spool/cron/root
# 00 22 * * * /usr/bin/sh /root/bin/samp.sh > /tmp/cron/samp.sh.log

crontab -l
# 00 22 * * * /usr/bin/sh /root/bin/samp.sh > /tmp/cron/samp.sh.log

tail -f /var/log/cron
# ...
# Mar 29 11:00:01 AliYun crond[1927]: (root) RELOAD (/var/spool/cron/root)
# Mar 29 11:00:01 AliYun CROND[24813]: (root) CMD (/usr/lib64/sa/sa1 1 1)
# Mar 29 11:01:01 AliYun CROND[24819]: (root) CMD (/usr/bin/sh /root/bin/samp.sh > /tmp/cron/samp.sh.log)

tail -100f /tmp/cron/samp.sh.log
# 0 sms ***:10080 ***:3306
# rm -rf logs and db dir
# ========== BEGIN: sms ==========
# ========== END: sms ==========
# 1 galaxy ***:10081 ***:3306
# rm -rf logs and db dir
# ========== BEGIN: galaxy ==========
# ========== END: galaxy ==========
# 2 activity ***:10082 ***:3306
# rm -rf logs and db dir
# ========== BEGIN: activity ==========
# ========== END: activity ==========
# ...
```