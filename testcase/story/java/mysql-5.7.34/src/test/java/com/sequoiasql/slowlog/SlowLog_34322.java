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
 * @Descreption seqDB-34322:long_query_include_lock_time 配置项测试
 * @Author linsuqiang
 * @CreateDate 2025.08.20
 */
public class SlowLog_34322 extends MysqlTestBase {

    private String dbName = "db_34322";
    private String tbName = "tb_34322";
    private String clFullName = dbName + "." + tbName;
    private JdbcInterface jdbc1 = null;
    private JdbcInterface jdbc2 = null;
    private List< String > slowResult = null;

    private boolean runSucess = false;

    @BeforeClass
    public void setUp() throws Exception {
        try {
            jdbc1 = JdbcInterfaceFactory
                    .build( JdbcWarpperType.JdbcWarpperOfHaInst1 );
            jdbc2 = JdbcInterfaceFactory
                    .build( JdbcWarpperType.JdbcWarpperOfHaInst1 );
            jdbc1.update( "SET GLOBAL long_query_time = 1;" );
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
            throw e;
        }
    }

    @Test
    public void test() throws Exception {
        jdbc1.update(
                "CREATE TABLE " + clFullName + " (id INT, name CHAR(16));" );
        jdbc1.update( "BEGIN;" );
        String sql = "INSERT INTO " + clFullName + " VALUES";
        for (int i = 0; i < 10; i++) {
            if (i != 0) {
                sql += ",";
            }
            sql += "(" + i + ",'" + i + "')";
        }
        sql += ";";
        jdbc1.update( sql );

        jdbc1.update( "SET GLOBAL long_query_include_lock_time = OFF;" );
        jdbc1.update( "LOCK TABLES " + clFullName + " WRITE;" );

        ThreadExecutor es = new ThreadExecutor( 300000 );
        QueryThread queryThread = new QueryThread();
        CheckThread checkThread = new CheckThread();
        es.addWorker( queryThread );
        es.addWorker( checkThread );
        es.run();

        Assert.assertEquals(slowResult.size(), 0);

        jdbc1.update( "SET GLOBAL long_query_include_lock_time = ON;" );
        jdbc1.update( "LOCK TABLES " + clFullName + " WRITE;" );
        ThreadExecutor es2 = new ThreadExecutor( 300000 );
        es2.addWorker( queryThread );
        es2.addWorker( checkThread );
        es2.run();

        Assert.assertEquals(slowResult.size(), 1);
        System.out.println("slow log:");
        System.out.println(slowResult.get(0));
        String[] slowFields = slowResult.get(0).split("\\|");

        String queryTimeStr = slowFields[SlowLogUtils.LOG_IDX_QUERY_TIME];
        double queryTimeMs = Double.parseDouble(queryTimeStr);
        Assert.assertTrue(1000.0 <= queryTimeMs && queryTimeMs < 3000.0);

        String lockTimeStr = slowFields[SlowLogUtils.LOG_IDX_LOCK_TIME];
        double lockTimeMs = Double.parseDouble(lockTimeStr);
        Assert.assertTrue(1000.0 <= lockTimeMs && lockTimeMs < 3000.0);

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
    }

    private class QueryThread extends ResultStore {
        @ExecuteOrder(step = 1)
        private void exec() throws Exception {
            jdbc2.query( "SELECT * FROM " + clFullName + " WHERE id != 0 ORDER BY id;" );
            System.out.println("done query");
        }
    }

    private class CheckThread extends ResultStore {
        private Sequoiadb sdb = null;

        public CheckThread() {
            sdb = new Sequoiadb( MysqlTestBase.coordUrl, "", "" );
        }

        @ExecuteOrder(step = 1)
        private void exec() throws Exception {
            try {
                Thread.sleep( 1200 );
                jdbc1.update( "UNLOCK TABLES;" );
                System.out.println("done unlock");

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
                    jdbc1.update("UNLOCK TABLES;");
                } catch (Exception e2) {
                    // ignore
                }
                System.out.println(e.getStackTrace());
                throw e;
            } finally {
                if (sdb != null) {
                    sdb.close();
                }
            }
        }
    }
}