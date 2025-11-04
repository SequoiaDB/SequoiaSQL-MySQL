package com.sequoiasql.slowlog;

import java.util.ArrayList;
import java.util.List;
import java.sql.SQLException;

import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.sequoiadb.threadexecutor.ResultStore;
import com.sequoiadb.threadexecutor.ThreadExecutor;
import com.sequoiadb.threadexecutor.annotation.ExecuteOrder;
import com.sequoiadb.base.Sequoiadb;
import com.sequoiasql.testcommon.*;

/**
 * @Descreption seqDB-34305:sdb_slow_log 与 slow_query_log, log_output 组合测试
 * @Author linsuqiang
 * @CreateDate 2025.08.19
 */
public class SlowLog_34305 extends MysqlTestBase {

    private JdbcInterface jdbc;

    @BeforeClass
    public void setUp() throws Exception {
        try {
            jdbc = JdbcInterfaceFactory
                    .build( JdbcWarpperType.JdbcWarpperOfHaInst1 );
            jdbc.update( "SET long_query_time = 0.3;" );
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
        } catch ( Exception e ) {
            if ( jdbc != null ) {
                jdbc.close();
            }
            throw e;
        }
    }

    @Test
    public void test() throws Exception {
        // slow_query_log = off, log_output = 'TABLE', sdb_slow_log = on. no log.
        jdbc.update( "SET GLOBAL slow_query_log = OFF;" );
        jdbc.update( "SET GLOBAL log_output = 'TABLE';" );
        jdbc.update( "SET GLOBAL sdb_slow_log = ON;" );
        jdbc.query( "SELECT SLEEP(0.31);" );
        List< String > act1 = jdbc.query( "SELECT COUNT(*) FROM mysql.sdb_slow_log;" );
        List< String > exp1 = new ArrayList<>();
        exp1.add( "0" );
        Assert.assertEquals( act1, exp1 );
        List< String > act2 = jdbc.query( "SELECT COUNT(*) FROM mysql.slow_log;" );
        List< String > exp2 = new ArrayList<>();
        exp2.add( "0" );
        Assert.assertEquals( act2, exp2 );

        // slow_query_log = on, log_output = 'FILE', sdb_slow_log = on. no table log.
        jdbc.update( "SET GLOBAL slow_query_log = ON;" );
        jdbc.update( "SET GLOBAL log_output = 'FILE';" );
        jdbc.update( "SET GLOBAL sdb_slow_log = ON;" );
        jdbc.query( "SELECT SLEEP(0.32);" );
        List< String > act3 = jdbc.query( "SELECT COUNT(*) FROM mysql.sdb_slow_log;" );
        List< String > exp3 = new ArrayList<>();
        exp3.add( "0" );
        Assert.assertEquals( act3, exp3 );
        List< String > act4 = jdbc.query( "SELECT COUNT(*) FROM mysql.slow_log;" );
        List< String > exp4 = new ArrayList<>();
        exp4.add( "0" );
        Assert.assertEquals( act4, exp4 );

        // slow_query_log = on, log_output = 'TABLE', sdb_slow_log = off. log into mysql.slow_log.
        jdbc.update( "SET GLOBAL slow_query_log = ON;" );
        jdbc.update( "SET GLOBAL log_output = 'TABLE';" );
        jdbc.update( "SET GLOBAL sdb_slow_log = OFF;" );
        jdbc.query( "SELECT SLEEP(0.33);" );
        List< String > act5 = jdbc.query( "SELECT COUNT(*) FROM mysql.sdb_slow_log;" );
        List< String > exp5 = new ArrayList<>();
        exp5.add( "0" );
        Assert.assertEquals( act5, exp5 );
        List< String > act6 = jdbc.query( "SELECT COUNT(*) FROM mysql.slow_log;" );
        List< String > exp6 = new ArrayList<>();
        exp6.add( "1" );
        Assert.assertEquals( act6, exp6 );

        // slow_query_log = on, log_output = 'TABLE', sdb_slow_log = on. log into mysql.sdb_slow_log.
        jdbc.update( "SET GLOBAL slow_query_log = ON;" );
        jdbc.update( "SET GLOBAL log_output = 'TABLE';" );
        jdbc.update( "SET GLOBAL sdb_slow_log = ON;" );
        jdbc.query( "SELECT SLEEP(0.34);" );
        List< String > act7 = jdbc.query( "SELECT COUNT(*) FROM mysql.sdb_slow_log;" );
        List< String > exp7 = new ArrayList<>();
        exp7.add( "1" );
        Assert.assertEquals( act7, exp7 );
        List< String > act8 = jdbc.query( "SELECT COUNT(*) FROM mysql.slow_log;" );
        List< String > exp8 = new ArrayList<>();
        exp8.add( "1" );
        Assert.assertEquals( act8, exp8 );
    }

    @AfterClass
    public void tearDown() throws Exception {
        try {
            jdbc.update( "SET GLOBAL slow_query_log = OFF;" );
        } catch (Exception e) {
            jdbc.close();
        }
    }
}
