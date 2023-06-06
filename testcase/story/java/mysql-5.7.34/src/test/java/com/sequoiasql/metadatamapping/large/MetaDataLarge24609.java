package com.sequoiasql.metadatamapping.large;

import com.sequoiadb.base.Sequoiadb;
import com.sequoiadb.threadexecutor.ResultStore;
import com.sequoiadb.threadexecutor.ThreadExecutor;
import com.sequoiadb.threadexecutor.annotation.ExecuteOrder;
import com.sequoiasql.testcommon.*;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.util.List;

/**
 * @Description seqDB-24609:表数量/映射表规则测试（M * N）。默认10 * 1024。
 * @Author wangxingming
 * @Date 2023/6/05
 */
public class MetaDataLarge24609 extends MysqlTestBase {
    private String dbName = "db_24609_large";
    private String tableNamePrefix = "tb_24609_large_";
    private Sequoiadb sdb = null;
    private int mappingCLNum = 10 * 1024;
    private JdbcInterface jdbc;

    @BeforeClass
    public void setUp() throws Exception {
        try {
            sdb = new Sequoiadb( MysqlTestBase.coordUrl, "", "" );
            if ( CommLib.isStandAlone( sdb ) ) {
                throw new SkipException( "is standalone skip testcase" );
            }
            if ( sdb.isCollectionSpaceExist( dbName ) ) {
                sdb.dropCollectionSpace( dbName );
            }
            jdbc = JdbcInterfaceFactory
                    .build( JdbcWarpperType.JdbcWarpperOfHaInst1 );
            jdbc.dropDatabase( dbName );
            jdbc.createDatabase( dbName );
        } catch ( Exception e ) {
            if ( sdb != null )
                sdb.close();
            if ( jdbc != null )
                jdbc.close();
            throw e;
        }
    }

    @Test
    public void test() throws Exception {
        // 创建表个数达到默认的 10 * 1024
        ThreadExecutor es = new ThreadExecutor( 2000000 );
        for ( int i = 0; i < mappingCLNum; i += 128 ) {
            CreateTable createTable = new CreateTable( i, i + 128 );
            es.addWorker( createTable );
        }
        es.run();

        // 查询表的数量
        List< String > act = jdbc.query(
                "select count(*) from information_schema.tables where table_schema='"
                        + dbName + "';" );
        int tableNum = Integer.parseInt( act.get( 0 ) );

        // 当映射表个数达到默认的 10 * 1024 时，再次创建一个普通表或添加分区，超过默认的最大映射表个数创建失败
        if ( tableNum == 10 * 1024 ) {
            // 创建普通表
            try {
                jdbc.update( "create table " + dbName + "." + tableNamePrefix
                        + "test" + "(a int,b int)" );
                Assert.fail( "should error but success" );
            } catch ( SQLException e ) {
                if ( e.getErrorCode() != 1030 ) {
                    throw e;
                }
            }
            // 创建分区表
            try {
                jdbc.update( "create table " + dbName + "." + tableNamePrefix
                        + "range" + "(a int,b int) partition by range(a) ("
                        + "partition p1 values less than(1500),"
                        + "partition p2 values less than(3000));" );
                Assert.fail( "should error but success" );
            } catch ( SQLException e ) {
                if ( e.getErrorCode() != 1030 ) {
                    throw e;
                }
            }
        }
    }

    @AfterClass
    public void tearDown() throws Exception {
        try {
            jdbc.dropDatabase( dbName );
        } finally {
            sdb.close();
            jdbc.close();
        }
    }

    private class CreateTable extends ResultStore {
        private int startNum;
        private int endNum;

        public CreateTable( int startNum, int endNum ) {
            this.startNum = startNum;
            this.endNum = endNum;
        }

        @ExecuteOrder(step = 1)
        public void exec() throws Exception {
            JdbcInterface jdbc = JdbcInterfaceFactory
                    .build( JdbcWarpperType.JdbcWarpperOfHaInst1 );
            try {
                for ( int i = startNum; i < endNum; i++ ) {
                    int currTimes = 0;
                    int retryTimes = 300;
                    while ( currTimes <= retryTimes ) {
                        try {
                            jdbc.update( "create table " + dbName + "."
                                    + tableNamePrefix + i + "(a int,b int)" );
                            jdbc.update( "insert into " + dbName + "."
                                    + tableNamePrefix + i
                                    + " values(1000,2000),(2000,2000)" );
                            break;
                        } catch ( SQLException e ) {
                            // ERROR 1030: Got error 30013 from storage engine
                            // ERROR 40190: Incompatible lock
                            if ( e.getErrorCode() == 1030
                                    || e.getErrorCode() == 40190 ) {
                                Thread.sleep(
                                        ( ( int ) Math.random() * 200 ) + 100 );
                                currTimes++;
                                if ( currTimes >= retryTimes ) {
                                    throw new Exception(
                                            "Retry timeout, create table failed." );
                                }
                            } else {
                                throw e;
                            }
                        }
                    }
                }
            } finally {
                jdbc.close();
            }
        }
    }
}
