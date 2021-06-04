package com.sequoiadb.datasrc;

import com.sequoiadb.base.CollectionSpace;
import com.sequoiadb.base.DBCollection;
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

/**
 * @Description seqDB-24205:mysql端集合映射主表并发执行插入和truncate操作
 * @author YiPan
 * @Date 2021.05.28
 * @version 1.0
 */
public class SqlDataSource24205 extends SdbTestBase {
    private com.sequoiadb.datasrc.SqlUtils utils = new com.sequoiadb.datasrc.SqlUtils();
    private String url;
    private String csname = "cs_24205";
    private String clname = "cl_24205";
    private String srcCLName = "subCL_24205";
    private String mainCLName = "mainCL_24205";
    private String subCLName = "subCL_24205";
    private Sequoiadb sdb = null;
    private Sequoiadb srcdb = null;
    private String dataSrcName = "datasource24205";
    private DBCollection maincl;
    private int recordNum = 5000;

    @BeforeClass
    public void setUp() throws Exception {
        url = utils.getUrl();
        utils.update( "drop database if exists " + csname, url );
        utils.update( "create database " + csname, url );
        // 创建数据源
        sdb = new Sequoiadb( SdbTestBase.coordUrl, "", "" );
        srcdb = new Sequoiadb( DataSrcUtils.getSrcUrl(), DataSrcUtils.getUser(),
                DataSrcUtils.getPasswd() );
        if ( CommLib.isStandAlone( sdb ) ) {
            throw new SkipException( "is standalone skip testcase" );
        }
        DataSrcUtils.clearDataSource( sdb, csname, dataSrcName );
        DataSrcUtils.createDataSource( sdb, dataSrcName,
                new BasicBSONObject( "TransPropagateMode", "notsupport" ) );
        // 源集群创建主表
        CollectionSpace cs = sdb.createCollectionSpace( csname );
        BasicBSONObject options = new BasicBSONObject();
        options.put( "IsMainCL", true );
        options.put( "ShardingKey", new BasicBSONObject( "id", 1 ) );
        options.put( "ShardingType", "range" );
        maincl = sdb.getCollectionSpace( csname ).createCollection( mainCLName,
                options );

        // 源集群创建子表挂载
        cs.createCollection( clname );
        BasicBSONObject subCLBound = new BasicBSONObject();
        subCLBound.put( "LowBound", new BasicBSONObject( "id", 0 ) );
        subCLBound.put( "UpBound", new BasicBSONObject( "id", 500 ) );
        maincl.attachCollection( csname + "." + clname, subCLBound );

        // 数据源创建子表
        DataSrcUtils.createCSAndCL( srcdb, csname, srcCLName );
        // 源集群创建表映射数据源
        options.clear();
        options.put( "DataSource", dataSrcName );
        options.put( "Mapping", srcCLName );
        cs.createCollection( subCLName, options );
        subCLBound.clear();
        subCLBound.put( "LowBound", new BasicBSONObject( "id", 500 ) );
        subCLBound.put( "UpBound", new BasicBSONObject( "id", 1000 ) );
        maincl.attachCollection( csname + "." + subCLName, subCLBound );
        utils.update( "create table " + csname + "." + mainCLName
                + "(id int,value varchar(50),age int);", url );
    }

    @Test
    public void test() throws Exception {
        // 创建插入数据存储过程
        utils.update( "create procedure " + csname + "." + "insertValue()"
                + "begin " + "declare i int;" + "set i = 0;" + "while (i<"
                + recordNum / 2 + ") do " + "insert into " + csname + "."
                + mainCLName + " values(i,'test',i);" + "set i = i+1;"
                + "end while;" + "end", url );
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
            utils.update( "drop database if exists " + csname, url );
            DataSrcUtils.clearDataSource( sdb, csname, dataSrcName );
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
            utils.update( "call " + csname + ".insertValue()", url );
        }
    }

    class Truncate extends ResultStore {
        @ExecuteOrder(step = 1)
        public void exec() throws Exception {
            for ( int i = 250; i < 500; i++ ) {
                utils.update( "truncate " + csname + "." + mainCLName, url );
            }
        }

    }

}
