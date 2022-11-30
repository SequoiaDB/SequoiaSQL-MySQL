package com.sequoiasql.metadatasync.serial;

import com.sequoiadb.base.Sequoiadb;
import com.sequoiasql.testcommon.JdbcInterface;
import com.sequoiasql.testcommon.MysqlTestBase;
import org.bson.BasicBSONObject;
import org.testng.Assert;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeoutException;

public class DDLUtils extends MysqlTestBase {
    public static void checkJdbcUpdateResult( JdbcInterface jdbc,
            String statement, int errorCode ) throws SQLException {
        try {
            jdbc.update( statement );
            Assert.fail( "expected fail but success" );
        } catch ( SQLException e ) {
            if ( e.getErrorCode() != errorCode ) {
                throw e;
            }
        }
    }

    public static void checkPendingInfoIsCleared( Sequoiadb sdb,
            String instanceGroupName )
            throws TimeoutException, InterruptedException {
        int retry = 0;
        while ( true ) {
            if ( !( sdb
                    .getCollectionSpace(
                            "HAInstanceGroup_" + instanceGroupName )
                    .getCollection( "HAPendingLog" ).query().hasNext()
                    || sdb.getCollectionSpace(
                            "HAInstanceGroup_" + instanceGroupName )
                            .getCollection( "HAPendingObject" ).query()
                            .hasNext() ) ) {
                // 检测到HAPendingLog的记录完全被清除则退出循环
                break;
            }
            // 计时超过timeout时HAPendingLog的记录还没有被完全清除则抛异常
            retry = retry + 1;
            if ( retry > 20 ) {
                throw new TimeoutException( "retry timed out." );
            }
            // HAPendingLog的记录没有被完全清除则休眠5s再进入下一次循环
            Thread.sleep( 5000 );
        }
    }

    public static void fillRecycleBin( Sequoiadb sdb, JdbcInterface jdbc,
                                       String dbName ) throws SQLException {
        sdb.getRecycleBin().dropAll( null );
        sdb.getRecycleBin().alter( new BasicBSONObject( "AutoDrop", false ) );
        sdb.getRecycleBin().alter( new BasicBSONObject( "MaxItemNum", 3 ) );
        jdbc.update( "use " + dbName + ";" );
        jdbc.update( "create table t1(id int)" );
        jdbc.update( "create table t2(id int)" );
        jdbc.update( "create table t3(id int)" );
        jdbc.update( "drop table t1,t2,t3 ;" );

        // 验证回收站是否已被填满，预期有三条记录
        Assert.assertEquals( sdb.getRecycleBin().getCount( null ), 3 );
    }

    public static void restoreRecycleBinConf( Sequoiadb sdb ) {
        sdb.getRecycleBin().alter( new BasicBSONObject( "AutoDrop", true ) );
        sdb.getRecycleBin().alter( new BasicBSONObject( "MaxItemNum", 100 ) );
    }

    public static void checkRecycleBin( Sequoiadb sdb ) {
        sdb.getRecycleBin().dropAll( null );
        if ( !sdb.getRecycleBin().getDetail().get( "AutoDrop" )
                .equals( true ) ) {
            sdb.getRecycleBin()
                    .alter( new BasicBSONObject( "AutoDrop", true ) );
        }
        if ( !sdb.getRecycleBin().getDetail().get( "MaxItemNum" )
                .equals( 100 ) ) {
            sdb.getRecycleBin()
                    .alter( new BasicBSONObject( "MaxItemNum", 100 ) );
        }
    }
}
