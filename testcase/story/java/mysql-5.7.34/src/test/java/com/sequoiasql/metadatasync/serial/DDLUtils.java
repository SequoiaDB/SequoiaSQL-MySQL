package com.sequoiasql.metadatasync.serial;

import com.sequoiadb.base.DBCollection;
import com.sequoiadb.base.DBCursor;
import com.sequoiadb.base.Sequoiadb;
import com.sequoiasql.testcommon.JdbcInterface;
import com.sequoiasql.testcommon.MysqlTestBase;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.testng.Assert;

import java.sql.SQLException;
import java.util.ArrayList;
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

    public static void checkInstanceIsSync( Sequoiadb sdb,
            String instanceGroupName )
            throws TimeoutException, InterruptedException {
        int retry = 0;
        boolean allSame = false;
        while ( !allSame ) {
            List< Integer > list = new ArrayList<>();
            BSONObject orderBy = new BasicBSONObject( "_id", -1 );
            BSONObject rs;
            // 获取最后一条HASQLLog日志的SQLID
            DBCursor lastSQLLog = sdb
                    .getCollectionSpace(
                            "HAInstanceGroup_" + instanceGroupName )
                    .getCollection( "HASQLLog" )
                    .query( null, null, orderBy, null, 0, 1 );
            while ( lastSQLLog.hasNext() ) {
                rs = lastSQLLog.getNext();
                int SQLID1 = Integer.parseInt( rs.get( "SQLID" ).toString() );
                list.add( SQLID1 );
            }
            // 获取HAInstanceState表中所有实例最新的SQLID
            DBCursor allInstance = sdb
                    .getCollectionSpace(
                            "HAInstanceGroup_" + instanceGroupName )
                    .getCollection( "HAInstanceState" ).query();
            while ( allInstance.hasNext() ) {
                int SQLID2 = Integer.parseInt(
                        allInstance.getNext().get( "SQLID" ).toString() );
                list.add( SQLID2 );
            }
            // 验证实例组下的实例是否为最新的同步
            for ( int i = 1; i < list.size(); i++ ) {
                if ( !list.get( i ).equals( list.get( 0 ) ) ) {
                    break;
                } else {
                    allSame = true;
                }
            }
            // 计时超过timeout时实例组还未同步则抛异常
            retry = retry + 1;
            if ( retry > 60 ) {
                throw new TimeoutException( "retry timed out." );
            }
            // 实例组还未同步则休眠1s再进入下一次循环
            Thread.sleep( 1000 );
        }
    }

    public static String getInstGroupName( Sequoiadb db, String mysqlUrl )
            throws Exception {
        String[] mysql1 = mysqlUrl.split( ":" );
        int port = Integer.parseInt( mysql1[ 1 ].toString() );
        String instanceGroupName = "";
        DBCollection dbcl = db.getCollectionSpace( "HASysGlobalInfo" )
                .getCollection( "HARegistry" );
        BasicBSONObject matcher = new BasicBSONObject();
        matcher.put( "Port", port );
        // 判断地址类型为ip/hostname/localhost
        String addr = mysql1[ 0 ];
        if ( addr.contains( "." ) ) {
            matcher.put( "IP", addr );
        } else if ( addr.equals( "localhost" ) ) {
            throw new Exception( "mysql url not support 'localhost'" );
        } else {
            matcher.put( "HostName", addr );
        }
        BasicBSONObject selector = new BasicBSONObject();
        selector.put( "InstanceGroupName", "" );
        DBCursor cursor = dbcl.query( matcher, selector, null, null );
        while ( cursor.hasNext() ) {
            instanceGroupName = ( String ) cursor.getNext()
                    .get( "InstanceGroupName" );
        }
        if ( instanceGroupName.equals( "" ) ) {
            Assert.fail( "instance group not exist" );
        }
        return instanceGroupName;
    }
}
