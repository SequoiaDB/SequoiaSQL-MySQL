package com.sequoiadb.datasrc;

import com.sequoiadb.base.CollectionSpace;
import com.sequoiadb.base.DBCollection;
import com.sequoiadb.base.Sequoiadb;
import com.sequoiadb.testcommon.CommLib;
import com.sequoiadb.testcommon.SdbTestBase;
import com.sequoiadb.threadexecutor.ThreadExecutor;
import com.sequoiadb.threadexecutor.annotation.ExecuteOrder;
import org.bson.BasicBSONObject;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @Description seqDB-24206:多实例mysql端集合映射主表并发执行数据操作
 * @author YiPan
 * @Date 2021.05.28
 * @version 1.0
 */
public class SqlDataSource24206 extends SdbTestBase {
    private com.sequoiadb.datasrc.SqlUtils utils = new com.sequoiadb.datasrc.SqlUtils();
    private String url1;
    private String url2;
    private String csname = "cs_24206";
    private String clname = "cl_24206";
    private String srcCLName = "subCL_24206";
    private String mainCLName = "mainCL_24206";
    private String subCLName = "subCL_24206";
    private Sequoiadb sdb = null;
    private Sequoiadb srcdb = null;
    private String dataSrcName = "datasource24206";
    private DBCollection maincl;
    private int recordNum = 2000;

    @BeforeClass
    public void setUp() throws Exception {
        url1 = utils.getUrls().get( 0 );
        url2 = utils.getUrls().get( 1 );
        utils.update( "drop database if exists " + csname, url1 );
        utils.update( "create database " + csname, url1 );
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
        subCLBound.put( "UpBound", new BasicBSONObject( "id", 5000 ) );
        maincl.attachCollection( csname + "." + clname, subCLBound );

        // 数据源创建子表
        DataSrcUtils.createCSAndCL( srcdb, csname, srcCLName );
        // 源集群创建表映射数据源
        options.clear();
        options.put( "DataSource", dataSrcName );
        options.put( "Mapping", srcCLName );
        cs.createCollection( subCLName, options );
        subCLBound.clear();
        subCLBound.put( "LowBound", new BasicBSONObject( "id", 5000 ) );
        subCLBound.put( "UpBound", new BasicBSONObject( "id", 10000 ) );
        maincl.attachCollection( csname + "." + subCLName, subCLBound );
        utils.update( "create table " + csname + "." + mainCLName
                + "(id int,value varchar(50),age int);", url1 );
    }

    @Test
    public void test() throws Exception {
        // 创建插入数据存储过程
        utils.update( "create procedure " + csname + "." + "insertValue1()"
                + "begin " + "declare i int;" + "set i = 0;" + "while (i<"
                + recordNum / 2 + ") do " + "insert into " + csname + "."
                + mainCLName + " values(i,'test',i);" + "set i = i+1;"
                + "end while;" + "end", url1 );

        // 创建插入数据存储过程
        utils.update( "create procedure " + csname + "." + "insertValue2()"
                + "begin " + "declare i int;" + "set i = " + recordNum / 2 + ";"
                + "while (i<" + recordNum + ") do " + "insert into " + csname
                + "." + mainCLName + " values(i,'test',i);" + "set i = i+1;"
                + "end while;" + "end", url1 );
        // 创建更新数据存储过程
        utils.update( "create procedure " + csname + "." + "updateValue()"
                + "begin" + " declare i int;" + "set i = 0;" + "while i<"
                + recordNum / 4 + " do " + "update " + csname + "." + mainCLName
                + " set value = 'new' where id = i;" + "set i = i+1;"
                + "end while;" + "end", url1 );
        // 创建删除存储过程
        utils.update( "create procedure " + csname + "." + "deleteValue()"
                + "begin" + " declare i int;" + "set i = " + recordNum / 4 + ";"
                + "while i<" + recordNum / 2 + " do " + "delete from " + csname
                + "." + mainCLName + " where id = i;" + "set i = i+1;"
                + "end while;" + "end", url1 );
        // 插入部分数据
        Thread.sleep( 5000 );
        utils.update( "call " + csname + ".insertValue1()", url2 );

        ThreadExecutor t = new ThreadExecutor();
        t.addWorker( new Insert() );
        t.addWorker( new Update() );
        t.addWorker( new Delete() );
        t.run();

        // 预期结果
        List< String > expresults = new ArrayList<>();
        for ( int i = 0; i < recordNum / 4; i++ ) {
            expresults.add( i + "|" + "new" + "|" + i );
        }
        for ( int i = recordNum / 2; i < recordNum; i++ ) {
            expresults.add( i + "|" + "test" + "|" + i );
        }
        List< String > results = utils
                .query( "select * from " + csname + "." + mainCLName, url2 );
        Collections.sort( results );
        Collections.sort( expresults );
        Assert.assertEquals( results.toString(), expresults.toString() );
    }

    @AfterClass
    public void tearDown() throws Exception {
        try {
            utils.update( "drop database if exists " + csname, url1 );
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

    class Insert {
        @ExecuteOrder(step = 1)
        public void exec() throws Exception {
            utils.update( "call " + csname + ".insertValue2()", url1 );
        }
    }

    class Update {
        @ExecuteOrder(step = 1)
        public void exec() throws Exception {
            utils.update( "call " + csname + ".updateValue()", url2 );
        }
    }

    class Delete {
        @ExecuteOrder(step = 1)
        public void exec() throws Exception {
            utils.update( "call " + csname + ".deleteValue()", url1 );
        }
    }
}
