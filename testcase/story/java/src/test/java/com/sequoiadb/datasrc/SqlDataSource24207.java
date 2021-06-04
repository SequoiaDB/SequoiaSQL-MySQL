package com.sequoiadb.datasrc;

import com.sequoiadb.base.CollectionSpace;
import com.sequoiadb.base.Sequoiadb;
import com.sequoiadb.testcommon.CommLib;
import com.sequoiadb.testcommon.SdbTestBase;
import com.sequoiadb.threadexecutor.ResultStore;
import com.sequoiadb.threadexecutor.ThreadExecutor;
import com.sequoiadb.threadexecutor.annotation.ExecuteOrder;

import org.bson.BasicBSONObject;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.SQLException;

/**
 * @Description seqDB-24207:多实例mysql端集合映射普通表并发执行数据操作
 * @author liuli
 * @Date 2021.05.28
 * @version 1.0
 */
public class SqlDataSource24207 extends SdbTestBase {
    private SqlUtils utils = new SqlUtils();
    private String url1;
    private String url2;
    private String csName = "cs_24207";
    private String clName = "cl_24207";
    private Sequoiadb sdb = null;
    private Sequoiadb srcdb = null;
    private String dataSrcName = "datasource24207";

    @BeforeClass
    public void setUp() throws Exception {

        url1 = utils.getUrls().get( 0 );
        url2 = utils.getUrls().get( 1 );
        utils.update( "drop database if exists " + csName, url1 );
        utils.update( "create database " + csName, url1 );
        // 创建数据源
        sdb = new Sequoiadb( SdbTestBase.coordUrl, "", "" );
        srcdb = new Sequoiadb( DataSrcUtils.getSrcUrl(), DataSrcUtils.getUser(),
                DataSrcUtils.getPasswd() );
        if ( CommLib.isStandAlone( sdb ) ) {
            throw new SkipException( "is standalone skip testcase" );
        }
        DataSrcUtils.clearDataSource( sdb, csName, dataSrcName );
        DataSrcUtils.createDataSource( sdb, dataSrcName,
                new BasicBSONObject( "TransPropagateMode", "notsupport" ) );
        DataSrcUtils.createCSAndCL( srcdb, csName, clName );
        CollectionSpace cs = sdb.createCollectionSpace( csName );
        BasicBSONObject options = new BasicBSONObject();
        options.put( "DataSource", dataSrcName );
        options.put( "Mapping", csName + "." + clName );
        cs.createCollection( clName, options );
        utils.update( "create table " + csName + "." + clName
                + "(id int,value varchar(50));", url1 );
        utils.update( "create procedure " + csName
                + ".insertValue() begin declare i int; set i=1; while i<10000 do insert into "
                + csName + "." + clName
                + " values (i,'test'); set i=i+1; end while; end", url1 );
        Thread.sleep( 5000 );
    }

    @Test
    public void test() throws Exception {
        ThreadExecutor t = new ThreadExecutor( 180000 );
        Insert insert = new Insert();
        Truncate truncate = new Truncate();
        t.addWorker( insert );
        t.addWorker( truncate );
        t.run();
        Assert.assertEquals( insert.getRetCode(), 0 );
        Assert.assertEquals( truncate.getRetCode(), 0 );
    }

    @AfterClass
    public void tearDown() throws Exception {
        try {
            utils.update( "drop database if exists " + csName, url2 );
            DataSrcUtils.clearDataSource( sdb, csName, dataSrcName );
        } finally {
            if ( sdb != null ) {
                sdb.close();
            }
            if ( srcdb != null ) {
                srcdb.close();
            }
        }
    }

    class Insert extends ResultStore {
        @ExecuteOrder(step = 1)
        public void exec() throws Exception {
            try {
                utils.update( "call " + csName + ".insertValue()", url1 );
            } catch ( SQLException e ) {
                if ( !( e.getMessage().equals( "Collection is truncated" ) ) ) {
                    throw e;
                }
            }
        }
    }

    class Truncate extends ResultStore {
        @ExecuteOrder(step = 1)
        public void exec() throws Exception {
            utils.update( "truncate " + csName + "." + clName, url2 );
        }

    }

}
