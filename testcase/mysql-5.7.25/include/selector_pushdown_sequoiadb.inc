#
# seqDB-18648
# selector pushdown testing
#
CREATE TABLE t1 ( id INT PRIMARY KEY, c CHAR(16), vc VARCHAR(16), tt TEXT );
INSERT INTO t1 VALUES (1,'op','op','op'),(-1,'bc','bc','bc'),(10,'123','123','123'),(7,'ui','ui','ui'),(18,'a1','a1','a1');
INSERT INTO t1 VALUES (-10,'hello','hello','hello'),(0,'bc','bc','bc'),(8,'test','test','test'),(11,'foo','foo','foo');
--sorted_result
SELECT id FROM t1 ignore index(PRIMARY) WHERE id > 0;
--source include/query_pushdown_condition_sequoiadb.inc
SELECT c, id FROM t1 ignore index(PRIMARY) ORDER BY c,id;
--source include/query_pushdown_condition_sequoiadb.inc
DROP TABLE t1;