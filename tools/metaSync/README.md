# 目的
Mysql的主备复制能够实现多个mysql节点之间的元数据同步问题，但备机只能提供读服务，无法充分利用多个mysql节点的能力。Mysql的ddl操作同步工具的设计目的就是为了解决多个mysql节点之间的元数据同步问题，但目前仍有使用限制，详见限制章节。
Mysql的ddl操作同步工具的实现原理是通过解析mysql的审计日志，连接远端mysql机器，执行审计日志中符合要求的sql语句。
由于mysql的审计插件目前只在mysql企业版中提供，社区版本暂时没有审计功能，但可以使用第三方提供的审计插件实现mysql的审计，目前第三方审计插件有：McAfee的MySQL Audit插件，MariaDB的server_audit插件以及Percona的audit log插件。本文档采用marriadb提供的server_audit-1.4.0的审计插件实现。

# 限制
1. 由于解析符合要求的sql后，连接到每台mysql服务器进行执行，会存在数据一致性的问题
2. 所有mysql节点的服务端口需相同
3. 需要在每台mysql节点的服务器上部署
4. Mysql节点服务器之间需要使用主机名互通
5. 目前支持同步ALTER，CREATE，DECLARE，GRANT，REVOKE，FLUSH操作
6. 只同步在本机上的ddl操作或者从非mysql节点机器上连接mysql节点执行的ddl操作
7. 目前支持python2

# 审计插件安装
> 注意：为了避免增加审计插件后发生mysql无法启动的情况，强烈建议安装审计日志插件前，mysql环境需要已经搭好并启动运行，安装完插件后再重启mysql。

1. 创建用于同步MySQL元数据的Mysql用户
```sql
CREATE USER 'sdbadmin'@'%' IDENTIFIED BY 'sdbadmin';
GRANT all on *.* TO 'sdbadmin'@'%' with grant option;
```
2. 将server_audit.so文件复制至mysql的安装目录下的lib/plugin目录下，server_audit.so下载地址：
[server_audit.so下载](https://downloads.mariadb.com/Audit-Plugin/MariaDB-Audit-Plugin/)
3. 修改mysql配置文件/etc/my.cnf,在mysqld下添加以下内容：
```config
# 加载审计插件
plugin-load=server_audit=server_audit.so
# 审计记录的审计，建议只记录需要同步的DCL和DDL操作
server_audit_events=CONNECT,QUERY_DDL,QUERY_DCL
# 开启审计
server_audit_logging=ON
# 审计日志文件名
server_audit_file_path=/opt/sequoiasql/mysql/data/auditlog/server_audit.log
# 强制切分审计日志文件
server_audit_file_rotate_now=OFF
# 审计日志文件大小10MB，超过该大小进行切割，单位为byte
server_audit_file_rotate_size=10485760
# 审计日志保留个数，超过后会丢弃最旧的
server_audit_file_rotations=999
# 输出类型为文件
server_audit_output_type=file
# 限制每行查询日志的大小为100kb，若表比较复杂，对应的操作语句比较长，建议增大该值
server_audit_query_log_limit=102400
```
4. 重启mysql
```config
sdb_sql_ctl restart mysqld3306
```

# 工具使用说明
1. config配置如下:
```config
[mysql]
# mysql节点主机名，只能填主机名
hosts = redhat-xjh-01,redhat-xjh-02
# mysql服务端口
port = 3306
# 密码类型，0代表密码为明文，1代表密码为密文
mysql_password_type = 0
# mysql用户
mysql_user = sdbadmin
# mysql密码
mysql_password = sdbadmin
# mysql安装目录
install_dir = /opt/sequoiasql/mysql

[execute]
# ddl同步间隔
interval_time = 5

[parse]
# 审计日志存储目录
parse_log_directory = /opt/sequoiasql/mysql/data3306/auditlog
# 审计日志文件名
audit_log_file_name = server_audit.log
# 最后扫描文件的最后修改时间
file_last_modified_time = 0
# 首行审计日志开始时间戳
file_first_line_time = 0
# 首行审计日志开始线程号
file_first_line_thread_id = 0
# 首行审计日志开始序号
file_first_line_seq = 0
# 最后扫描行号
last_parse_row = 0
```
2. log.config日志配置如下：
```
[loggers]
keys=root,ddlLogger

[handlers]
keys=rotatingFileHandler

[formatters]
keys=loggerFormatter

[logger_root]
level=INFO
handlers=rotatingFileHandler

[logger_ddlLogger]
level=INFO
handlers=rotatingFileHandler
qualname=ddlLogger
propagate=0

[handler_rotatingFileHandler]
class=logging.handlers.RotatingFileHandler
# 日志级别，支持ERROR,INFO,DEBUG
level=INFO
# 日志格式
formatter=loggerFormatter
# 第1个参数为运行日志文件名,路径对应的目录必须已存在
# 第2个参数为写入模式，值为'a+',不建议修改
# 第3个参数为日志文件大小，单位为byte
# 第4个参数为备份日志文件，即日志文件总数为10+1
args=('run.log', 'a+', 104857600, 10)

[formatter_loggerFormatter]
format=%(asctime)s [%(levelname)s] %(message)s
datefmt=

```
3. 将config,log.config,meta_sync.py放置同一目录，在sdbadmin用户下，执行以下命令执行程序
```config
python /home/sdbadmin/meta_sync.py &
```
4. 配置定时任务，定期检查程序是否存活，若不再运行，则重新运行程序
```bash
crontab -e
#每一分钟运行一次
*/1 * * * * python /home/sdbadmin/meta_sync.py &

```

# 审计日志配置参数说明
[marriadb-audit-plugin官方说明](https://mariadb.com/kb/en/library/mariadb-audit-plugin-system-variables/)

|参数|说明|备注|
|---|---|---|
|server_audit_events|需要记录审计日志事件||
|server_audit_excl_users|不记录的用户||
|server_audit_file_path|审计日志文件存储位置||
|server_audit_file_rotate_now|强制切分文档||
|server_audit_file_rotations|保留切分文档数量||
|server_audit_incl_users|记录的用户||
|server_audit_logging||开启审计|
|server_audit_mode||用于开发测试使用|
|server_audit_output_type|审计日志输出类型||
|server_audit_query_log_limit|查询日志大小限制|范围：0至2147483647|
|server_audit_syslog_facility|输出到syslog的参数||
|server_audit_syslog_ident|输出到syslog的参数||
|server_audit_syslog_info|输出到syslog的参数||
|server_audit_syslog_priority|输出到syslog的参数||

## 审计日志事件说明
> 是指server_audit_events的值

|类型|说明|备注|
|---|---|---|
|CONNECT|记录连接信息||
|QUERY|记录所有操作||
|TABLE|Tables affected by query execution|暂时未知用途,建议配置|
|QUERY_DDL|记录ddl操作||
|QUERY_DML|记录DML操作||
|QUERY_DML_NO_SELECT|记录DML操作,不包括SELECT||
|QUERY_DCL|记录DCL操作||

# 审计日志格式
timestamp,serverhost,username,host,connectionid,queryid,operation,database,object,retcode
说明如下：

|项|说明|备注|
|---|---|---|
|timestamp|时间|格式:“年月日 时:分:秒”|
|serverhost|服务主机名||
|username|用户名||
|connectionid|连接id||
|queryid|查询id||
|operation|操作||
|database|数据库||
|object|sql||
|retcode|返回码||
