package com.sequoiadb.testcommon;

import com.sequoiadb.base.DBCollection;
import com.sequoiadb.base.DBCursor;
import com.sequoiadb.base.Sequoiadb;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.testng.Assert;

import java.sql.SQLException;
import java.util.*;

public class JdbcAssert {
    public static void execInvalidUpdate( JdbcInterface jdbcWarpper, String sql,
            int errorCode ) throws Exception {
        isJdbcWarpper( jdbcWarpper );
        try {
            jdbcWarpper.update( sql );
            throw new Exception( "expected fail but success" );
        } catch ( SQLException e ) {
            if ( e.getErrorCode() != errorCode ) {
                throw e;
            }
        }
    }

    public static void execInvalidQuery( JdbcInterface jdbcWarpper, String sql,
            int errorCode ) throws Exception {
        isJdbcWarpper( jdbcWarpper );
        try {
            jdbcWarpper.query( sql );
            throw new Exception( "expected fail but success" );
        } catch ( SQLException e ) {
            if ( e.getErrorCode() != errorCode ) {
                throw e;
            }
        }
    }

    public static void checkTableDataWithSql( String sql,
            JdbcInterface jdbcWarpperMgr ) throws Exception {
        isJdbcWarpperMgr( jdbcWarpperMgr );
        HashMap< String, List< String > > data = jdbcWarpperMgr.query( sql );
        Collections.sort( data.get( "sequoiadb" ) );
        Collections.sort( data.get( "innodb" ) );
        Assert.assertEquals( data.get( "sequoiadb" ), data.get( "innodb" ) );
    }

    public static void checkTableData( String FullTableName,
            JdbcInterface jdbcWarpperMgr ) throws Exception {
        isJdbcWarpperMgr( jdbcWarpperMgr );
        String sql = "select * from " + FullTableName;
        checkTableDataWithSql( sql, jdbcWarpperMgr );
    }

    public static void checkTableMetaWithIgnore( String fullTableName,
            List< String > ignore, JdbcInterface jdbcWarpperMgr )
            throws Exception {
        isJdbcWarpperMgr( jdbcWarpperMgr );
        String sql = "show create table " + fullTableName;
        HashMap< String, String > metas = jdbcWarpperMgr.querymeta( sql );
        String sequoiadbMeta = metas.get( "sequoiadb" );
        String innodbMeta = metas.get( "innodb" );
        for ( String s : ignore ) {
            sequoiadbMeta = sequoiadbMeta.replace( s, "" );
            innodbMeta = innodbMeta.replace( s, "" );
        }
        Assert.assertEquals( sequoiadbMeta, innodbMeta );
    }

    public static void checkTableMeta( String fullTableName,
            JdbcInterface jdbcWarpperMgr ) throws Exception {
        isJdbcWarpperMgr( jdbcWarpperMgr );
        List< String > ignore = new ArrayList<>();
        ignore.add( "ENGINE=SEQUOIADB" );
        ignore.add( "ENGINE=InnoDB" );
        checkTableMetaWithIgnore( fullTableName, ignore, jdbcWarpperMgr );
    }

    public static void checkTable( String fullTableName,
            JdbcInterface jdbcWarpperMgr ) throws Exception {
        isJdbcWarpperMgr( jdbcWarpperMgr );
        checkTableMeta( fullTableName, jdbcWarpperMgr );
        checkTableData( fullTableName, jdbcWarpperMgr );
    }

    public static void checkDatabase( String databaseName,
            JdbcInterface jdbcWarpperMgr ) throws Exception {
        isJdbcWarpperMgr( jdbcWarpperMgr );
        String sql = "show tables from " + databaseName;
        HashMap< String, List< String > > tables = jdbcWarpperMgr.query( sql );
        Assert.assertEquals( tables.get( "act" ), tables.get( "exp" ) );
        for ( String tableName : tables.get( "act" ) ) {
            checkTable( databaseName + "." + tableName, jdbcWarpperMgr );
        }
    }

    private static void isJdbcWarpperMgr( JdbcInterface jdbcWarpperMgr )
            throws Exception {
        String name = jdbcWarpperMgr.getClass().getName();
        if ( name != JdbcWarpperMgr.class.getName() ) {
            throw new Exception( "patamer jdbcWarpperMgr should be "
                    + JdbcWarpperMgr.class.getName() );
        }
    }

    private static void isJdbcWarpper( JdbcInterface... jdbcWarpper )
            throws Exception {
        for ( int i = 0; i < jdbcWarpper.length; i++ ) {
            String name = jdbcWarpper[ i ].getClass().getName();
            if ( name != JdbcWarpper.class.getName() ) {
                throw new Exception( "patamer jdbcWarpper  should be "
                        + JdbcWarpper.class.getName() );
            }
        }
    }

}
