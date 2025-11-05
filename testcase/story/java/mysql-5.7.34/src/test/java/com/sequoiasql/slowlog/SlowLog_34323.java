package com.sequoiasql.slowlog;

import java.util.ArrayList;
import java.util.List;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Pattern;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.sequoiadb.threadexecutor.ResultStore;
import com.sequoiadb.threadexecutor.ThreadExecutor;
import com.sequoiadb.threadexecutor.annotation.ExecuteOrder;
import com.sequoiadb.base.DBCursor;
import com.sequoiadb.base.Sequoiadb;
import com.sequoiasql.testcommon.*;

/**
 * @Descreption seqDB-34323:复杂慢查询测试
 * @Author linsuqiang
 * @CreateDate 2025.08.20
 */
public class SlowLog_34323 extends MysqlTestBase {

    private String dbName = "db_34323";
    private String tbName = "tb_34323";
    private String clFullName = dbName + "." + tbName;
    private JdbcInterface jdbc = null;
    private List< String > slowResult = null;

    private boolean runSucess = false;

    @BeforeClass
    public void setUp() throws Exception {
        try {
            jdbc = JdbcInterfaceFactory
                    .build( JdbcWarpperType.JdbcWarpperOfHaInst1 );
            jdbc.update( "SET long_query_time = 1;" );
            jdbc.update( "SET GLOBAL slow_query_log = ON;" );
            jdbc.update( "SET GLOBAL log_output = 'TABLE';" );
            jdbc.update( "SET GLOBAL sdb_slow_log = ON;" );
            jdbc.update( "TRUNCATE mysql.slow_log;" );
            try {
                jdbc.update( "TRUNCATE mysql.sdb_slow_log;" );
            } catch ( SQLException e ) {
                if ( "42S02".equals( e.getSQLState() ) ) {
                    // wait background thread to create it
                    Thread.sleep(1000);
                } else {
                    throw e;
                }
            }
            jdbc.dropDatabase( dbName );
            jdbc.createDatabase( dbName );
        } catch ( Exception e ) {
            if ( jdbc != null ) {
                jdbc.close();
            }
            throw e;
        }
    }

    @Test
    public void test() throws Exception {
        jdbc.update(
                "CREATE TABLE " + clFullName + "_1 (a int, b int, c int);" );
        jdbc.update(
                "CREATE TABLE " + clFullName + "_2 (a int, b int, c int);" );

        String sql = "INSERT INTO " + clFullName + "_1 VALUES";
        for (int i = 0; i < 1000; i++) {
            if (i != 0) {
                sql += ",";
            }
            sql += "(" + i + "," + i + "," + i + ")";
        }
        sql += ";";
        jdbc.update( sql );

        sql = "INSERT INTO " + clFullName + "_2 VALUES";
        for (int i = 0; i < 1000; i++) {
            if (i != 0) {
                sql += ",";
            }
            sql += "(" + i + "," + i + "," + i + ")";
        }
        sql += ";";
        jdbc.update( sql );

        jdbc.query(
            "SELECT /*+NO_BNL()*/ * FROM " + clFullName + "_1 AS t1 " +
            "JOIN " + clFullName + "_2 AS t2 ON " +
            "t1.a = t2.a;" );

        int max_retry_times = 10;
        int retry_interval_ms = 500;
        for (int i = 0; i < max_retry_times; i++) {
            slowResult = jdbc.query( "SELECT * FROM mysql.sdb_slow_log;" );
            if (slowResult.size() == 0) {
                System.out.println("waiting for slow log write");
                Thread.sleep(retry_interval_ms);
                continue;
            }
            break;
        }

        Assert.assertEquals(slowResult.size(), 1);
        System.out.println("slow log:");
        System.out.println(slowResult.get(0));
        String[] slowFields = slowResult.get(0).split("\\|");

        String startTimeStr = slowFields[SlowLogUtils.LOG_IDX_START_TIME].split("\\.")[0];
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date startDate = sdf.parse(startTimeStr);
        long startTime = startDate.getTime();

        String endTimeStr = slowFields[SlowLogUtils.LOG_IDX_END_TIME].split("\\.")[0];
        Date endDate = sdf.parse(endTimeStr);
        long endTime = endDate.getTime();
        long timeDiff = (endTime - startTime);
        if (1000 > timeDiff) {
            System.out.println(timeDiff);
            Assert.assertTrue(false);
        }

        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_DIGEST],
                "8ac659a3eb7a72b26480684455e64e2e");
        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_DIGEST_TEXT],
                "SELECT /*+ NO_BNL ( ) */ * FROM `db_34323` . `tb_34323_1` AS `t1` " +
                "JOIN `db_34323` . `tb_34323_2` AS `t2` ON `t1` . `a` = `t2` . `a` ");
        // [LOG_IDX_USER_HOST] manually checked

        String queryTimeStr = slowFields[SlowLogUtils.LOG_IDX_QUERY_TIME];
        double queryTimeMs = Double.parseDouble(queryTimeStr);
        Assert.assertTrue(1000.0 <= queryTimeMs);

        String lockTimeStr = slowFields[SlowLogUtils.LOG_IDX_LOCK_TIME];
        double lockTimeMs = Double.parseDouble(lockTimeStr);
        Assert.assertTrue(lockTimeMs > 0.0);

        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_ROWS_SENT], "1000");
        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_DB], "");
        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_LAST_INSERT_ID], "0");
        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_INSERT_ID], "0");

        String getInstanceIDSql =
            "SELECT INSTANCE_ID FROM information_schema.SDB_INSTANCE_LIST " +
            "WHERE HOST_NAME = @@hostname AND PORT = @@port;";
        List< String > instanceIDResult = jdbc.query( getInstanceIDSql );
        Assert.assertEquals( instanceIDResult.size(), 1 );
        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_INSTANCE_ID],
                            instanceIDResult.get(0));



        List< String > hostResult = jdbc.query( "SELECT @@hostname;" );
        Assert.assertEquals( hostResult.size(), 1 );
        String host = hostResult.get(0);

        List< String > portResult = jdbc.query( "SELECT @@port;" );
        Assert.assertEquals( portResult.size(), 1 );
        String port = portResult.get(0);

        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_INSTANCE_URL],
                            host + ":" + port);



        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_SQL_TEXT],
                "SELECT /*+NO_BNL()*/ * FROM db_34323.tb_34323_1 AS t1 JOIN " +
                "db_34323.tb_34323_2 AS t2 ON t1.a = t2.a");
        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_CONNECT_COUNT], "0");
        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_CONNECT_TIME], "0");
        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_GET_CL_COUNT], "2");
        double getCLTime = Double.parseDouble(slowFields[SlowLogUtils.LOG_IDX_GET_CL_TIME]);
        Assert.assertTrue(getCLTime > 0.0);
        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_BEGIN_TRANS_COUNT], "1");
        double beginTransTime = Double.parseDouble(slowFields[SlowLogUtils.LOG_IDX_BEGIN_TRANS_TIME]);
        Assert.assertTrue(beginTransTime > 0.0);
        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_EXEC_INNER_SQL_COUNT], "0");
        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_EXEC_INNER_SQL_TIME], "0");

        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_FIND_COUNT], "1001");

        String findTimeStr = slowFields[SlowLogUtils.LOG_IDX_FIND_TIME];
        double findTimeMs = Double.parseDouble(findTimeStr);
        Assert.assertTrue(500.0 <= findTimeMs);

        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_AGGREGATE_COUNT], "0");
        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_AGGREGATE_TIME], "0");
        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_GET_COUNT_COUNT], "0");
        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_GET_COUNT_TIME], "0");
        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_INSERT_COUNT], "0");
        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_INSERT_TIME], "0");
        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_UPDATE_COUNT], "0");
        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_UPDATE_TIME], "0");
        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_DELETE_COUNT], "0");
        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_DELETE_TIME], "0");
        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_GET_DETAIL_COUNT], "0");
        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_GET_DETAIL_TIME], "0");
        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_GET_INDEX_STAT_COUNT], "0");
        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_GET_INDEX_STAT_TIME], "0");
        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_HA_RETRY_TIMES], "0");
        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_HA_SYNC_WAIT_TIME], "0");
        String pushdown_info = slowFields[SlowLogUtils.LOG_IDX_PUSHDOWN_INFO];
        Assert.assertTrue( pushdown_info.length() <= 1024 );

        runSucess = true;
    }

    @AfterClass
    public void tearDown() throws Exception {
        if ( jdbc != null ) {
            try {
                jdbc.update( "SET GLOBAL slow_query_log = OFF;" );
                if ( runSucess ) {
                    jdbc.dropDatabase( dbName );
                }
            } finally {
                jdbc.close();
            }
        }
    }
}
