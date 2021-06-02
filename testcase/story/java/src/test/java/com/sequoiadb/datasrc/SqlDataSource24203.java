package com.sequoiadb.datasrc;

import com.sequoiadb.base.CollectionSpace;
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
 * @Description seqDB-24203:mysql端集合映射普通表并发执行数据操作
 * @author YiPan
 * @Date 2021.05.28
 * @version 1.0
 */
public class SqlDataSource24203 extends SdbTestBase {
    private com.sequoiadb.datasrc.SqlUtils utils = new com.sequoiadb.datasrc.SqlUtils();
    private String url;
    private String csname = "cs_24203";
    private String clname = "cl_24203";
    private Sequoiadb sdb = null;
    private Sequoiadb srcdb = null;
    private String dataSrcName = "datasource24203";
    private int recordNum = 2000;

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
        DataSrcUtils.createCSAndCL( srcdb, csname, clname );
        BasicBSONObject options = new BasicBSONObject();
        options.put( "DataSource", dataSrcName );
        options.put( "Mapping", clname );
        CollectionSpace cs = sdb.createCollectionSpace( csname );
        cs.createCollection( clname, options );
        utils.update( "create table " + csname + "." + clname
                + "(id int,value varchar(50),age int);", url );
    }

    @Test
    public void test() throws Exception {
        // 创建插入数据存储过程1
        utils.update( "create procedure " + csname + "." + "insertValue1()"
                + "begin " + "declare i int;" + "set i = 0;" + "while (i<"
                + recordNum / 2 + ") do " + "insert into " + csname + "."
                + clname + " values(i,'test',i);" + "set i = i+1;"
                + "end while;" + "end", url );
        // 创建插入数据存储过程2
        utils.update( "create procedure " + csname + "." + "insertValue2()"
                + "begin " + "declare i int;" + "set i = " + recordNum / 2 + ";"
                + "while (i<" + recordNum + ") do " + "insert into " + csname
                + "." + clname + " values(i,'test',i);" + "set i = i+1;"
                + "end while;" + "end", url );
        // 创建更新存储过程
        utils.update( "create procedure " + csname + "." + "updateValue()"
                + "begin" + " declare i int;" + "set i = 0;" + "while i<"
                + recordNum / 4 + " do " + "update " + csname + "." + clname
                + " set value = 'new' where id = i;" + "set i = i+1;"
                + "end while;" + "end", url );
        // 创建删除存储过程
        utils.update( "create procedure " + csname + "." + "deleteValue()"
                + "begin" + " declare i int;" + "set i = " + recordNum / 4 + ";"
                + "while i<" + recordNum / 2 + " do " + "delete from " + csname
                + "." + clname + " where id = i;" + "set i = i+1;"
                + "end while;" + "end", url );
        // 插入部分数据
        utils.update( "call " + csname + ".insertValue1()", url );

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
                .query( "select * from " + csname + "." + clname, url );
        Collections.sort( results );
        Collections.sort( expresults );
        Assert.assertEquals( results.toString(), expresults.toString() );
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

    class Insert {
        @ExecuteOrder(step = 1)
        public void exec() throws Exception {
            utils.update( "call " + csname + ".insertValue2()", url );
        }
    }

    class Update {
        @ExecuteOrder(step = 1)
        public void exec() throws Exception {
            utils.update( "call " + csname + ".updateValue()", url );
        }
    }

    class Delete {
        @ExecuteOrder(step = 1)
        public void exec() throws Exception {
            utils.update( "call " + csname + ".deleteValue()", url );
        }

    }

}
