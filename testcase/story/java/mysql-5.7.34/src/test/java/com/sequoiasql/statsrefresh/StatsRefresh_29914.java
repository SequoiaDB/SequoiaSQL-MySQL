package com.sequoiasql.statsrefresh;

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

import java.util.ArrayList;

/**
 * @Descreption seqDB-29914:并发执行DDL语句和refresh语句
 * @Author chenzejia
 * @CreateDate 2023.02.09
 * @UpdateUser chenzejia
 * @UpdateDate 2023.02.09
 * @Version
 */
public class StatsRefresh_29914 extends MysqlTestBase {
    private String dbName = "db_29914";
    private Sequoiadb sdb;
    private JdbcInterface jdbc;

    @BeforeClass
    public void setUp() throws Exception {
        try {
            sdb = new Sequoiadb( MysqlTestBase.coordUrl, "", "" );
            if ( CommLib.isStandAlone( sdb ) ) {
                throw new SkipException( "is standalone skip testcase" );
            }
            jdbc = JdbcInterfaceFactory
                    .build( JdbcWarpperType.JdbcWarpperOfHaInst1 );
            if ( sdb.isCollectionSpaceExist( dbName ) ) {
                sdb.dropCollectionSpace( dbName );
            }
            jdbc.dropDatabase( dbName );
            jdbc.createDatabase( dbName );
        } catch ( Exception e ) {
            if ( sdb != null ) {
                sdb.close();
            }
            if ( jdbc != null ) {
                jdbc.close();
            }
        }
    }

    // DDL执行失败，用例屏蔽，问题单：http://jira.web:8080/browse/SEQUOIASQLMAINSTREAM-1706
    @Test(enabled = false)
    public void test() throws Exception {
        String tb_part = "tb_part";
        String tb_normal = "tb_normal";
        String tb_new = "tb_new";
        // 清除其他数据表的sdb缓存，避免对用例造成影响
        jdbc.update( "flush tables;" );

        // 创建分区表和普通表并插入数据
        StatsRefreshUtils.createTableAndInsert( jdbc, dbName, tb_part, tb_normal );

        // sdb侧刷新统计信息
        sdb.analyze();

        String refresh_sql1 = "refresh tables stats;";
        String refresh_sql2 = "refresh tables " + tb_normal + "," + tb_part
                + " stats;";
        String refresh_sql3 = "refresh tables " + tb_part + " stats;";
        String ddl_sql1 = "alter table " + tb_normal
                + " add column c varchar(28);";
        String ddl_sql2 = "alter table " + tb_normal + " drop index b;";
        String ddl_sql3 = "alter table " + tb_part + " rename " + tb_new + ";";

        Operate ddl1 = new Operate( ddl_sql1 );
        Operate ddl2 = new Operate( ddl_sql2 );
        Operate ddl3 = new Operate( ddl_sql3 );
        Operate refresh1 = new Operate( refresh_sql1 );
        Operate refresh2 = new Operate( refresh_sql2 );
        Operate refresh3 = new Operate( refresh_sql3 );

        ThreadExecutor es1 = new ThreadExecutor();
        es1.addWorker( ddl1 );
        es1.addWorker( ddl2 );
        es1.addWorker( ddl3 );
        es1.addWorker( refresh1 );
        es1.addWorker( refresh2 );
        es1.addWorker( refresh3 );

        // 获取mysql当前时间
        long currentTime = StatsRefreshUtils.getMysqlTime( jdbc );
        es1.run();

        // 更改分区表表名，预期成功
        ArrayList< String > tables = jdbc.query( "show tables;" );
        ArrayList< String > actTables = new ArrayList<>();
        actTables.add( tb_new );
        actTables.add( tb_normal );
        Assert.assertEquals( actTables, tables );

        // 普通表统计信息预期成功刷新
        Assert.assertTrue( StatsRefreshUtils.isRefresh( jdbc, currentTime,
                dbName, tb_normal ) );

        // 获取普通表结果，预期修改表结构成功
        ArrayList< String > query = jdbc
                .query( "show create table tb_normal;" );
        String[] split = query.get( 0 ).split( "\\|" );
        String createNormalTb = "CREATE TABLE `tb_normal` (\n"
                + "  `a` int(11) NOT NULL,\n"
                + "  `b` varchar(128) COLLATE utf8mb4_bin DEFAULT NULL,\n"
                + "  `c` varchar(28) COLLATE utf8mb4_bin DEFAULT NULL,\n"
                + "  PRIMARY KEY (`a`)\n"
                + ") ENGINE=SequoiaDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin";
        Assert.assertEquals( createNormalTb, split[ 1 ] );
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

    private class Operate extends ResultStore {
        private String sqlStr;

        public Operate( String sqlStr ) {
            this.sqlStr = sqlStr;
        }

        @ExecuteOrder(step = 1)
        public void exec() throws Exception {
            JdbcInterface jdbc = JdbcInterfaceFactory
                    .build( JdbcWarpperType.JdbcWarpperOfHaInst1 );
            try {
                jdbc.update( "use " + dbName + ";" );
                jdbc.update( sqlStr );
            } finally {
                jdbc.close();
            }
        }
    }

}