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
 * @Descreption seqDB-34317:慢 delete 测试
 * @Author linsuqiang
 * @CreateDate 2025.08.20
 */
public class SlowLog_34317 extends MysqlTestBase {

    private String dbName = "db_34317";
    private String tbName = "tb_34317";
    private String clFullName = dbName + "." + tbName;
    private JdbcInterface jdbc1 = null;
    private JdbcInterface jdbc2 = null;
    private Sequoiadb sdb = null;

    private BSONObject snapResult = null;
    private List< String > stdResult = null;
    private List< String > infoResult = null;
    private List< String > showResult = null;
    private List< String > slowResult = null;
    private String opConnID = "";

    private boolean runSucess = false;

    @BeforeClass
    public void setUp() throws Exception {
        try {
            sdb = new Sequoiadb( MysqlTestBase.coordUrl, "", "" );
            sdb.updateConfig( new BasicBSONObject( "mongroupmask", "all:detail" ) );
            sdb.updateConfig( new BasicBSONObject( "monslowquerythreshold", 1000 ) );

            jdbc1 = JdbcInterfaceFactory
                    .build( JdbcWarpperType.JdbcWarpperOfHaInst1 );
            jdbc2 = JdbcInterfaceFactory
                    .build( JdbcWarpperType.JdbcWarpperOfHaInst1 );
            jdbc1.update( "SET long_query_time = 1;" );
            jdbc2.update( "SET long_query_time = 1;" );
            jdbc1.update( "SET GLOBAL slow_query_log = ON;" );
            jdbc1.update( "SET GLOBAL log_output = 'TABLE';" );
            jdbc1.update( "SET GLOBAL sdb_slow_log = ON;" );
            jdbc1.update( "TRUNCATE mysql.slow_log;" );
            try {
                jdbc1.update( "TRUNCATE mysql.sdb_slow_log;" );
            } catch ( SQLException e ) {
                if ( "42S02".equals( e.getSQLState() ) ) {
                    // wait background thread to create it
                    Thread.sleep(1000);
                } else {
                    throw e;
                }
            }
            jdbc1.dropDatabase( dbName );
            jdbc1.createDatabase( dbName );
        } catch ( Exception e ) {
            if ( jdbc1 != null ) {
                jdbc1.close();
            }
            if ( jdbc2 != null ) {
                jdbc2.close();
            }
            if ( sdb != null ) {
                sdb.close();
            }
            throw e;
        }
    }

    @Test
    public void test() throws Exception {
        jdbc1.update(
                "CREATE TABLE " + clFullName + " (id INT, name CHAR(16));" );
        jdbc1.update( "BEGIN;" );
        for (int i = 0; i < 2000; i++) {
            String sql = "INSERT INTO " + clFullName + " VALUES";
            for (int j = 0; j < 10; j++) {
                if (j != 0) {
                    sql += ",";
                }
                sql += "(" + ( i * 10 + j ) + ",'" + i + "')";
            }
            sql += ";";
            jdbc1.update( sql );
        }
        List< String > connIDResult = jdbc2.query( "SELECT CONNECTION_ID();" );
        opConnID = connIDResult.get(0);

        ThreadExecutor es = new ThreadExecutor( 300000 );
        DeleteThread deleteThread = new DeleteThread();
        CheckThread checkThread = new CheckThread();
        es.addWorker( deleteThread );
        es.addWorker( checkThread );
        es.run();



        Assert.assertEquals(stdResult.size(), 1);
        System.out.println("standard processlist:");
        System.out.println(stdResult.get(0));

        Assert.assertEquals(infoResult.size(), 1);
        System.out.println("processlist_for_sequoiadb:");
        System.out.println(infoResult.get(0));

        String[] stdFields = stdResult.get(0).split("\\|");
        String[] infoFields = infoResult.get(0).split("\\|");
        Assert.assertTrue(snapResult != null);
        String queryID = snapResult.get("QueryID").toString();

        Assert.assertEquals(infoFields[SlowLogUtils.PFS_IDX_ID],
                            stdFields[SlowLogUtils.PFS_IDX_ID]);
        Assert.assertEquals(infoFields[SlowLogUtils.PFS_IDX_USER],
                            stdFields[SlowLogUtils.PFS_IDX_USER]);
        Assert.assertEquals(infoFields[SlowLogUtils.PFS_IDX_HOST],
                            stdFields[SlowLogUtils.PFS_IDX_HOST]);
        Assert.assertEquals(infoFields[SlowLogUtils.PFS_IDX_DB],
                            stdFields[SlowLogUtils.PFS_IDX_DB]);
        Assert.assertEquals(infoFields[SlowLogUtils.PFS_IDX_COMMAND],
                            stdFields[SlowLogUtils.PFS_IDX_COMMAND]);
        double time = Double.parseDouble(infoFields[SlowLogUtils.PFS_IDX_TIME]);
        Assert.assertTrue(1000.0 < time && time < 2000.0);
        Assert.assertEquals(infoFields[SlowLogUtils.PFS_IDX_STATE],
                            stdFields[SlowLogUtils.PFS_IDX_STATE]);
        Assert.assertEquals(infoFields[SlowLogUtils.PFS_IDX_INFO],
                            stdFields[SlowLogUtils.PFS_IDX_INFO]);
        Assert.assertEquals(infoFields[SlowLogUtils.PFS_IDX_QUERY_ID], queryID);
        Assert.assertEquals(infoFields[SlowLogUtils.PFS_IDX_CONNECT_COUNT], "0");
        Assert.assertEquals(infoFields[SlowLogUtils.PFS_IDX_CONNECT_TIME], "0");
        Assert.assertEquals(infoFields[SlowLogUtils.PFS_IDX_GET_CL_COUNT], "1");
        double getCLTime = Double.parseDouble(infoFields[SlowLogUtils.PFS_IDX_GET_CL_TIME]);
        Assert.assertTrue(getCLTime > 0);
        Assert.assertEquals(infoFields[SlowLogUtils.PFS_IDX_BEGIN_TRANS_COUNT], "0");
        Assert.assertEquals(infoFields[SlowLogUtils.PFS_IDX_BEGIN_TRANS_TIME], "0");
        Assert.assertEquals(infoFields[SlowLogUtils.PFS_IDX_EXEC_INNER_SQL_COUNT], "0");
        Assert.assertEquals(infoFields[SlowLogUtils.PFS_IDX_EXEC_INNER_SQL_TIME], "0");
        Assert.assertEquals(infoFields[SlowLogUtils.PFS_IDX_FIND_COUNT], "0");
        Assert.assertEquals(infoFields[SlowLogUtils.PFS_IDX_FIND_TIME], "0");
        Assert.assertEquals(infoFields[SlowLogUtils.PFS_IDX_AGGREGATE_COUNT], "0");
        Assert.assertEquals(infoFields[SlowLogUtils.PFS_IDX_AGGREGATE_TIME], "0");
        Assert.assertEquals(infoFields[SlowLogUtils.PFS_IDX_GET_COUNT_COUNT], "0");
        Assert.assertEquals(infoFields[SlowLogUtils.PFS_IDX_GET_COUNT_TIME], "0");
        Assert.assertEquals(infoFields[SlowLogUtils.PFS_IDX_INSERT_COUNT], "0");
        Assert.assertEquals(infoFields[SlowLogUtils.PFS_IDX_INSERT_TIME], "0");
        Assert.assertEquals(infoFields[SlowLogUtils.PFS_IDX_UPDATE_COUNT], "0");
        Assert.assertEquals(infoFields[SlowLogUtils.PFS_IDX_UPDATE_TIME], "0");
        Assert.assertEquals(infoFields[SlowLogUtils.PFS_IDX_DELETE_COUNT], "1");
        Assert.assertEquals(infoFields[SlowLogUtils.PFS_IDX_DELETE_TIME], "0");
        Assert.assertEquals(infoFields[SlowLogUtils.PFS_IDX_GET_DETAIL_COUNT], "0");
        Assert.assertEquals(infoFields[SlowLogUtils.PFS_IDX_GET_DETAIL_TIME], "0");
        Assert.assertEquals(infoFields[SlowLogUtils.PFS_IDX_GET_INDEX_STAT_COUNT], "0");
        Assert.assertEquals(infoFields[SlowLogUtils.PFS_IDX_GET_INDEX_STAT_TIME], "0");
        Assert.assertEquals(infoFields[SlowLogUtils.PFS_IDX_HA_RETRY_TIMES], "0");
        Assert.assertEquals(infoFields[SlowLogUtils.PFS_IDX_HA_SYNC_WAIT_TIME], "0");
        String pushdown_info = infoFields[SlowLogUtils.PFS_IDX_PUSHDOWN_INFO];
        pushdown_info = pushdown_info.replaceAll("\"ClientPort\": \\d+, \"ClientQID\": \\d+", "");
        Assert.assertEquals(pushdown_info,
                "delete db_34317.tb_34317 condition: { \"id\": { \"$lte\": 1 } } hint: " +
                "{ \"$ClientInfo\": {  } } flags: 0x4 ; ");



        String matchedString = null;
        for (int i = 0; i < showResult.size(); i++) {
            String[] fields = showResult.get(i).split("\\|");
            if (fields[SlowLogUtils.PFS_IDX_ID].equals(opConnID)) {
                matchedString = showResult.get(i);
                break;
            }
        }
        Assert.assertTrue(matchedString != null);
        System.out.println("show processlist for sequoiadb:");
        System.out.println(matchedString);

        String[] showFields = matchedString.split("\\|");
        Assert.assertEquals(showFields[SlowLogUtils.PFS_IDX_ID],
                            stdFields[SlowLogUtils.PFS_IDX_ID]);
        Assert.assertEquals(showFields[SlowLogUtils.PFS_IDX_USER],
                            stdFields[SlowLogUtils.PFS_IDX_USER]);
        Assert.assertEquals(showFields[SlowLogUtils.PFS_IDX_HOST],
                            stdFields[SlowLogUtils.PFS_IDX_HOST]);
        Assert.assertEquals(showFields[SlowLogUtils.PFS_IDX_DB],
                            stdFields[SlowLogUtils.PFS_IDX_DB]);
        Assert.assertEquals(showFields[SlowLogUtils.PFS_IDX_COMMAND],
                            stdFields[SlowLogUtils.PFS_IDX_COMMAND]);
        time = Double.parseDouble(showFields[SlowLogUtils.PFS_IDX_TIME]);
        Assert.assertTrue(1000.0 < time && time < 2000.0);
        Assert.assertEquals(showFields[SlowLogUtils.PFS_IDX_STATE],
                            stdFields[SlowLogUtils.PFS_IDX_STATE]);
        Assert.assertEquals(showFields[SlowLogUtils.PFS_IDX_INFO],
                            stdFields[SlowLogUtils.PFS_IDX_INFO]);
        Assert.assertEquals(showFields[SlowLogUtils.PFS_IDX_QUERY_ID], queryID);
        Assert.assertEquals(showFields[SlowLogUtils.PFS_IDX_CONNECT_COUNT], "0");
        Assert.assertEquals(showFields[SlowLogUtils.PFS_IDX_CONNECT_TIME], "0");
        Assert.assertEquals(showFields[SlowLogUtils.PFS_IDX_GET_CL_COUNT], "1");
        getCLTime = Double.parseDouble(infoFields[SlowLogUtils.PFS_IDX_GET_CL_TIME]);
        Assert.assertTrue(getCLTime > 0);
        Assert.assertEquals(showFields[SlowLogUtils.PFS_IDX_BEGIN_TRANS_COUNT], "0");
        Assert.assertEquals(showFields[SlowLogUtils.PFS_IDX_BEGIN_TRANS_TIME], "0");
        Assert.assertEquals(showFields[SlowLogUtils.PFS_IDX_EXEC_INNER_SQL_COUNT], "0");
        Assert.assertEquals(showFields[SlowLogUtils.PFS_IDX_EXEC_INNER_SQL_TIME], "0");
        Assert.assertEquals(showFields[SlowLogUtils.PFS_IDX_FIND_COUNT], "0");
        Assert.assertEquals(showFields[SlowLogUtils.PFS_IDX_FIND_TIME], "0");
        Assert.assertEquals(showFields[SlowLogUtils.PFS_IDX_AGGREGATE_COUNT], "0");
        Assert.assertEquals(showFields[SlowLogUtils.PFS_IDX_AGGREGATE_TIME], "0");
        Assert.assertEquals(showFields[SlowLogUtils.PFS_IDX_GET_COUNT_COUNT], "0");
        Assert.assertEquals(showFields[SlowLogUtils.PFS_IDX_GET_COUNT_TIME], "0");
        Assert.assertEquals(showFields[SlowLogUtils.PFS_IDX_INSERT_COUNT], "0");
        Assert.assertEquals(showFields[SlowLogUtils.PFS_IDX_INSERT_TIME], "0");
        Assert.assertEquals(showFields[SlowLogUtils.PFS_IDX_UPDATE_COUNT], "0");
        Assert.assertEquals(showFields[SlowLogUtils.PFS_IDX_UPDATE_TIME], "0");
        Assert.assertEquals(showFields[SlowLogUtils.PFS_IDX_DELETE_COUNT], "1");
        Assert.assertEquals(showFields[SlowLogUtils.PFS_IDX_DELETE_TIME], "0");
        Assert.assertEquals(showFields[SlowLogUtils.PFS_IDX_GET_DETAIL_COUNT], "0");
        Assert.assertEquals(showFields[SlowLogUtils.PFS_IDX_GET_DETAIL_TIME], "0");
        Assert.assertEquals(showFields[SlowLogUtils.PFS_IDX_GET_INDEX_STAT_COUNT], "0");
        Assert.assertEquals(showFields[SlowLogUtils.PFS_IDX_GET_INDEX_STAT_TIME], "0");
        Assert.assertEquals(showFields[SlowLogUtils.PFS_IDX_HA_RETRY_TIMES], "0");
        Assert.assertEquals(showFields[SlowLogUtils.PFS_IDX_HA_SYNC_WAIT_TIME], "0");
        pushdown_info = showFields[SlowLogUtils.PFS_IDX_PUSHDOWN_INFO];
        pushdown_info = pushdown_info.replaceAll("\"ClientPort\": \\d+, \"ClientQID\": \\d+", "");
        Assert.assertEquals(pushdown_info,
                "delete db_34317.tb_34317 condition: { \"id\": { \"$lte\": 1 } } hint: " +
                "{ \"$ClientInfo\": {  } } flags: 0x4 ; ");



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
        if (!(1000 <= timeDiff && timeDiff < 3000)) {
            System.out.println(timeDiff);
            Assert.assertTrue(false);
        }

        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_QUERY_ID], queryID);
        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_DIGEST],
                "415606707f8ada6c2948a79302e1c455");
        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_DIGEST_TEXT],
                "DELETE FROM `db_34317` . `tb_34317` WHERE `id` <= ? ");
        // [LOG_IDX_USER_HOST] manually checked

        String queryTimeStr = slowFields[SlowLogUtils.LOG_IDX_QUERY_TIME];
        double queryTimeMs = Double.parseDouble(queryTimeStr);
        Assert.assertTrue(1000.0 <= queryTimeMs && queryTimeMs < 3000.0);

        String lockTimeStr = slowFields[SlowLogUtils.LOG_IDX_LOCK_TIME];
        double lockTimeMs = Double.parseDouble(lockTimeStr);
        Assert.assertTrue(lockTimeMs > 0.0);

        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_ROWS_SENT], "0");
        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_DB], "");
        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_LAST_INSERT_ID], "0");
        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_INSERT_ID], "0");

        String getInstanceIDSql =
            "SELECT INSTANCE_ID FROM information_schema.SDB_INSTANCE_LIST " +
            "WHERE HOST_NAME = @@hostname AND PORT = @@port;";
        List< String > instanceIDResult = jdbc1.query( getInstanceIDSql );
        Assert.assertEquals( instanceIDResult.size(), 1 );
        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_INSTANCE_ID],
                            instanceIDResult.get(0));

        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_SQL_TEXT],
                "DELETE FROM db_34317.tb_34317 WHERE id <= 1");
        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_THREAD_ID], opConnID);
        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_CONNECT_COUNT], "0");
        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_CONNECT_TIME], "0");
        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_GET_CL_COUNT], "1");
        getCLTime = Double.parseDouble(slowFields[SlowLogUtils.LOG_IDX_GET_CL_TIME]);
        Assert.assertTrue(getCLTime > 0.0);
        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_BEGIN_TRANS_COUNT], "0");
        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_BEGIN_TRANS_TIME], "0");
        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_EXEC_INNER_SQL_COUNT], "0");
        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_EXEC_INNER_SQL_TIME], "0");
        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_FIND_COUNT], "0");
        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_FIND_TIME], "0");
        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_AGGREGATE_COUNT], "0");
        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_AGGREGATE_TIME], "0");
        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_GET_COUNT_COUNT], "0");
        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_GET_COUNT_TIME], "0");
        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_INSERT_COUNT], "0");
        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_UPDATE_COUNT], "0");
        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_UPDATE_TIME], "0");
        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_DELETE_COUNT], "1");

        String deleteTimeStr = slowFields[SlowLogUtils.LOG_IDX_DELETE_TIME];
        double deleteTimeMs = Double.parseDouble(deleteTimeStr);
        Assert.assertTrue(1000.0 <= deleteTimeMs && deleteTimeMs < 3000.0);

        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_GET_DETAIL_COUNT], "0");
        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_GET_DETAIL_TIME], "0");
        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_GET_INDEX_STAT_COUNT], "0");
        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_GET_INDEX_STAT_TIME], "0");
        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_HA_RETRY_TIMES], "0");
        Assert.assertEquals(slowFields[SlowLogUtils.LOG_IDX_HA_SYNC_WAIT_TIME], "0");
        pushdown_info = slowFields[SlowLogUtils.LOG_IDX_PUSHDOWN_INFO];
        pushdown_info = pushdown_info.replaceAll("\"ClientPort\": \\d+, \"ClientQID\": \\d+", "");
        Assert.assertEquals(pushdown_info,
                "delete db_34317.tb_34317 condition: { \"id\": { \"$lte\": 1 } } hint: " +
                "{ \"$ClientInfo\": {  } } flags: 0x4 ; ");

        runSucess = true;
    }

    @AfterClass
    public void tearDown() throws Exception {
        if ( jdbc2 != null ) {
            try {
                if ( runSucess ) {
                    jdbc2.dropDatabase( dbName );
                }
            } finally {
                jdbc2.close();
            }
        }
        if ( jdbc1 != null ) {
            try {
                jdbc1.update( "SET GLOBAL slow_query_log = OFF;" );
            } finally {
                jdbc1.close();
            }
        }
        if ( sdb != null ) {
            try {
                sdb.deleteConfig(
                        new BasicBSONObject( "mongroupmask", 1 ),
                        new BasicBSONObject() );
                sdb.deleteConfig(
                        new BasicBSONObject( "monslowquerythreshold", 1 ),
                        new BasicBSONObject() );
            } finally {
                sdb.close();
            }
        }
    }

    private class DeleteThread extends ResultStore {
        @ExecuteOrder(step = 1)
        private void exec() throws Exception {
            jdbc2.update( "DELETE FROM " + clFullName + " WHERE id <= 1;" );
            System.out.println("done delete");
        }
    }

    private class CheckThread extends ResultStore {
        @ExecuteOrder(step = 1)
        private void exec() throws Exception {
            try {
                Thread.sleep( 1200 );
                DBCursor cursor = sdb.getSnapshot(Sequoiadb.SDB_SNAP_QUERIES,
                        new BasicBSONObject("Name", clFullName),
                        new BasicBSONObject(),
                        new BasicBSONObject(),
                        new BasicBSONObject("$Options",
                                new BasicBSONObject("viewHistory", false)),
                        0,
                        -1);
                snapResult = cursor.getNext();

                stdResult = jdbc1.query( "SELECT * FROM information_schema." +
                        "processlist WHERE ID = " + opConnID + ";" );

                infoResult = jdbc1.query( "SELECT * FROM information_schema." +
                       "processlist_for_sequoiadb WHERE ID = " + opConnID + ";" );

                showResult = jdbc1.query( "SHOW PROCESSLIST FOR SEQUOIADB;" );
                jdbc1.update( "COMMIT;" );
                System.out.println("done commit");



                int max_retry_times = 10;
                int retry_interval_ms = 500;
                for (int i = 0; i < max_retry_times; i++) {
                    slowResult = jdbc1.query( "SELECT * FROM mysql.sdb_slow_log;" );
                    if (slowResult.size() == 0) {
                        System.out.println("waiting for slow log write");
                        Thread.sleep(retry_interval_ms);
                        continue;
                    }
                    break;
                }

            } catch (Exception e) {
                try {
                    jdbc1.update("COMMIT;");
                } catch (Exception e2) {
                    // ignore
                }
                System.out.println(e.getStackTrace());
                throw e;
            }
        }
    }
}
