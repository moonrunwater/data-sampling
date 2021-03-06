# 数据迁移一致性抽验 code view

## 问题

1. 数据不一致情况，要立即记录现场信息 [Done]
   - 解决方案：查询处源表、目标表全部字段数据，并打印 log：库名、表名、ID，全字段数据
   - 已修改完成

2. 部分表，迁移后的表结构，与原表有微小变化
   需要弄清楚原因，确认是否会对业务造成影响

3. 执行期间，对数据库的性能影响
   与 @魔戒 一起，监控一下执行期间，数据库资源（cpu、memory、io）占用情况

4. 表没有主键、没有索引情况下，一次性查出表所有记录 SELECT * FROM TABLE [Done]
   - 若遇到大表，存在数据库性能急剧下降、内存爆掉的风险，是个不定时炸弹
   - 解决方案：SELECT ALL 之前，先查下表记录，若不超过阈值（如 10 万条），则 SELECT ALL；超过，则直接 logger.error，不做处理
   - 已修改完成

5. 任务多线程执行存在 bug
   - 查找相关资料，fix 掉
   - 待完成

6. 文件路径，直接写死，可能存在权限问题 [Done]
   - 解决方案：文件路径，可以作为可选参数，如果不设置，则使用默认路径
   - 已修改完成

7. task 字段需要添加注释说明 [Done]
   - 已添加
