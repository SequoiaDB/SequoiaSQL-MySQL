package com.sequoiasql.statsrefresh;

import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.sequoiadb.base.Sequoiadb;
import com.sequoiadb.threadexecutor.ResultStore;
import com.sequoiadb.threadexecutor.ThreadExecutor;
import com.sequoiadb.threadexecutor.annotation.ExecuteOrder;
import com.sequoiasql.testcommon.*;

/**
 * @Descreption seqDB-29912:一个实例执行refresh语句，另一个实例查看统计信息刷新结果
 * @Author chenzejia
 * @CreateDate 2023.02.09
 * @UpdateDate 2023.02.09
 * @UpdateRemark chenzejia
 */
public class StatsRefresh_29912 extends MysqlTestBase {

    private String dbName = "db_29912";
    private Sequoiadb sdb;
    private JdbcInterface jdbc1;
    private JdbcInterface jdbc2;

    @BeforeClass
    public void setUp() throws Exception {
        try {
            sdb = new Sequoiadb( MysqlTestBase.coordUrl, "", "" );
            if ( CommLib.isStandAlone( sdb ) ) {
                throw new SkipException( "is standalone skip testcase" );
            }
            jdbc1 = JdbcInterfaceFactory
                    .build( JdbcWarpperType.JdbcWarpperOfHaInst1 );
            jdbc2 = JdbcInterfaceFactory
                    .build( JdbcWarpperType.JdbcWarpperOfHaInst2 );
            if ( sdb.isCollectionSpaceExist( dbName ) ) {
                sdb.dropCollectionSpace( dbName );
            }
            jdbc1.dropDatabase( dbName );
            jdbc1.createDatabase( dbName );
        } catch ( Exception e ) {
            if ( sdb != null ) {
                sdb.close();
            }
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
        String tb_part = "tb_part";
        String tb_normal = "tb_normal";

        // 清除其他数据表的sdb缓存，避免对用例造成影响
        jdbc1.update( "flush tables;" );
        jdbc2.update( "flush tables;" );

        // 创建分区表和普通表并插入数据
        StatsRefreshUtils.createTableAndInsert( jdbc1, dbName, tb_part, tb_normal );
        jdbc2.update( "use " + dbName + ";" );

        // sdb侧刷新统计信息
        sdb.analyze();

        String refresh_sql1 = "refresh tables stats;";
        String refresh_sql2 = "refresh tables " + tb_normal + "," + tb_part
                + " stats;";
        String refresh_sql3 = "refresh tables " + tb_part + " stats;";

        Refresh refresh1 = new Refresh( refresh_sql1, jdbc1 );
        Refresh refresh2 = new Refresh( refresh_sql2, jdbc1 );
        Refresh refresh3 = new Refresh( refresh_sql3, jdbc1 );

        ThreadExecutor es1 = new ThreadExecutor();
        es1.addWorker( refresh1 );
        es1.addWorker( refresh2 );
        es1.addWorker( refresh3 );

        long currentTime1 = StatsRefreshUtils.getMysqlTime( jdbc1 );

        es1.run();

        // 实例1统计信息刷新成功
        Assert.assertTrue( StatsRefreshUtils.isRefresh( jdbc1, currentTime1,
                dbName, tb_normal ) );
        Assert.assertTrue( StatsRefreshUtils.isRefresh( jdbc1, currentTime1,
                dbName, tb_part ) );

        // 实例2统计信息未刷新
        Long lastRefreshTime1 = StatsRefreshUtils.getLastRefreshTime( jdbc2,
                dbName, tb_normal );
        Long lastRefreshTime2 = StatsRefreshUtils.getLastRefreshTime( jdbc2,
                dbName, tb_part );
        Assert.assertNull( lastRefreshTime1 );
        Assert.assertNull( lastRefreshTime2 );

    }

    @AfterClass
    public void tearDown() throws Exception {
        try {
            jdbc1.dropDatabase( dbName );
        } finally {
            sdb.close();
            jdbc1.close();
            jdbc2.close();
        }
    }

    private class Refresh extends ResultStore {
        private String sqlStr;
        private JdbcInterface jdbc;

        public Refresh( String sqlStr, JdbcInterface jdbc ) {
            this.sqlStr = sqlStr;
            this.jdbc = jdbc;
        }

        @ExecuteOrder(step = 1)
        public void exec() throws Exception {
            jdbc.update( sqlStr );
        }
    }
}