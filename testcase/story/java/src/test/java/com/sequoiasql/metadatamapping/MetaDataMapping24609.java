package com.sequoiasql.metadatamapping;

import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;

import com.sequoiadb.threadexecutor.ResultStore;
import com.sequoiadb.threadexecutor.ThreadExecutor;
import com.sequoiadb.threadexecutor.annotation.ExecuteOrder;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.sequoiadb.base.Sequoiadb;
import com.sequoiasql.testcommon.*;

/**
 * @Description seqDB-24609:表数量/映射表规则测试（M * N）
 * @Author liuli
 * @Date 2021.11.13
 * @UpdateAuthor liuli
 * @UpdateDate 2021.11.13
 * @version 1.10
 */
public class MetaDataMapping24609 extends MysqlTestBase {
    private String csName = "cs_24609";
    private String clName = "cl_24609_";
    private Sequoiadb sdb = null;
    private int mappingCount = 1024;
    private int mappingSize = 9;
    private JdbcInterface jdbc;

    @BeforeClass
    public void setUp() throws Exception {
        sdb = new Sequoiadb( MysqlTestBase.coordUrl, "", "" );
        if ( CommLib.isStandAlone( sdb ) ) {
            throw new SkipException( "is standalone skip testcase" );
        }
        if ( sdb.isCollectionSpaceExist( csName ) ) {
            sdb.dropCollectionSpace( csName );
        }

        jdbc = JdbcInterfaceFactory
                .build( JdbcWarpperType.JdbcWarpperOfHaInst1 );
        jdbc.dropDatabase( csName );
        jdbc.createDatabase( csName );
    }

    @Test(enabled = false)
    public void test() throws Exception {
        // 创建256个range分区表每个表包含2个分区
        for ( int i = 0; i < 256; i++ ) {
            jdbc.update( "create table " + csName + "." + clName + "range" + i
                    + "(a int,b int) partition by range(a) (partition p1 values less than(1500),partition p2 values less than(3000));" );
            jdbc.update( "insert into " + csName + "." + clName + "range" + i
                    + " values(1000,2000),(2000,2000)" );
        }
        // 创建256个hash分区表
        for ( int i = 0; i < 256; i++ ) {
            jdbc.update( "create table " + csName + "." + clName + "hash" + i
                    + "(a int,b int) partition by hash(a) partitions 4" );
            jdbc.update( "insert into " + csName + "." + clName + "hash" + i
                    + " values(1000,2000),(2000,2000)" );
        }
        // 创建255个普通表
        for ( int i = 0; i < 255; i++ ) {
            jdbc.update( "create table " + csName + "." + clName + i
                    + "(a int,b int)" );
            jdbc.update( "insert into " + csName + "." + clName + i
                    + " values(1000,2000),(2000,2000)" );
        }
        // 在一个range分区表中添加一个分区
        jdbc.update( "alter table " + csName + "." + clName + "range1 "
                + "add partition (partition p3 values less than(5000))" );
        // 校验sdb端的集合空间
        String instanceGroupName = MetaDataMappingUtils
                .getInstGroupName( sdb, MysqlTestBase.mysql1 ).toUpperCase();
        Assert.assertTrue( sdb.isCollectionSpaceExist(
                instanceGroupName + "#" + csName + "#1" ) );
        Assert.assertFalse( sdb.isCollectionSpaceExist(
                instanceGroupName + "#" + csName + "#2" ) );

        // 删除一个分区后再创建一个hash分区表
        jdbc.update( "alter table " + csName + "." + clName + "range1 "
                + "drop partition p3" );
        jdbc.update( "create table " + csName + "." + clName + "hash" + 257
                + "(a int,b int) partition by hash(a) partitions 4" );
        // 校验sdb端的集合空间
        Assert.assertTrue( sdb.isCollectionSpaceExist(
                instanceGroupName + "#" + csName + "#1" ) );
        Assert.assertFalse( sdb.isCollectionSpaceExist(
                instanceGroupName + "#" + csName + "#2" ) );

        // 再次在一个range分区表中添加一个分区
        jdbc.update( "alter table " + csName + "." + clName + "range1 "
                + "add partition (partition p3 values less than(5000))" );
        // 校验sdb端的集合空间
        Assert.assertTrue( sdb.isCollectionSpaceExist(
                instanceGroupName + "#" + csName + "#1" ) );
        Assert.assertTrue( sdb.isCollectionSpaceExist(
                instanceGroupName + "#" + csName + "#2" ) );

        // 现在一共存在1025个表，继续创建普通表，使表总数达到1024*10个
        ThreadExecutor es = new ThreadExecutor( 2000000 );
        int startNum = 1025;
        int tableNum;
        for ( tableNum = startNum + 512; tableNum < mappingCount
                * mappingSize; tableNum = tableNum + 512 ) {
            CreateTable createTable = new CreateTable( startNum, tableNum );
            es.addWorker( createTable );
            startNum = startNum + 512;
        }
        es.addWorker( new CreateTable( startNum, mappingCount * mappingSize ) );
        es.run();

        // 校验sdb端的集合空间
        for ( int i = 1; i < mappingSize + 1; i++ ) {
            Assert.assertTrue( sdb.isCollectionSpaceExist(
                    instanceGroupName + "#" + csName + "#" + i ) );
        }
        Assert.assertFalse( sdb.isCollectionSpaceExist( instanceGroupName + "#"
                + csName + "#" + ( mappingSize + 1 ) ) );

        // 再次创建一个普通表
        try {
            jdbc.update( "create table " + csName + "." + clName + "test"
                    + "(a int,b int)" );
            Assert.fail( "should error but success" );
        } catch ( SQLException e ) {
            if ( e.getErrorCode() != 30013 ) {
                throw e;
            }
        }

        // 再次在一个range分区表中添加一个分区
        try {
            jdbc.update( "alter table " + csName + "." + clName + "range2 "
                    + "add partition (partition p3 values less than(5000))" );
            Assert.fail( "should error but success" );
        } catch ( SQLException e ) {
            // ERROR 1296 (HY000): Got error 30013 'There is no error object' from SequoiaDB
            if ( e.getErrorCode() != 1296 ) {
                throw e;
            }
        }

        // mysql端删除数据库
        jdbc.update( "drop database " + csName );

        // sdb端校验集合空间被删除
        for ( int i = 1; i < mappingSize + 1; i++ ) {
            Assert.assertFalse( sdb.isCollectionSpaceExist(
                    instanceGroupName + "#" + csName + "#" + i ) );
        }
    }

    @AfterClass
    public void tearDown() throws Exception {
        sdb.close();
        jdbc.close();
    }

    private class CreateTable extends ResultStore {
        private int startNum;
        private int endNum;

        public CreateTable( int startNum, int endNum ) {
            this.startNum = startNum;
            this.endNum = endNum;
        }

        @ExecuteOrder(step = 1)
        public void exec() throws SQLException, InvocationTargetException,
                ClassNotFoundException, InstantiationException,
                NoSuchMethodException, IllegalAccessException {
            JdbcInterface jdbc = JdbcInterfaceFactory
                    .build( JdbcWarpperType.JdbcWarpperOfHaInst1 );
            int i = 0;
            int failNum = 0;
            try {
                for ( i = startNum; i < endNum; i++ ) {
                    try {
                        // 连续失败10次抛错
                        if ( failNum > 10 ) {
                            Assert.fail(
                                    "10 consecutive failures, the number of tables may exceed the upper limit" );
                        }
                        jdbc.update( "create table " + csName + "." + clName + i
                                + "(a int,b int)" );
                        jdbc.update( "insert into " + csName + "." + clName + i
                                + " values(1000,2000),(2000,2000)" );
                        failNum = 0;
                    } catch ( SQLException e ) {
                        // ERROR 30013 (HY000): Too many tables in current
                        // database, the upper limit is 10240
                        if ( e.getErrorCode() != 30013 ) {
                            throw e;
                        }
                        i--;
                        failNum++;
                    }
                }
            } finally {
                jdbc.close();
            }
        }
    }
}
