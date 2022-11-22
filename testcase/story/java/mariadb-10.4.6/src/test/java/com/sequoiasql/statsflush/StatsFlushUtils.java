package com.sequoiasql.statsflush;

import com.sequoiasql.testcommon.JdbcInterface;
import com.sequoiasql.testcommon.MysqlTestBase;

import java.sql.SQLException;
import java.util.ArrayList;

public class StatsFlushUtils extends MysqlTestBase {
    public static String[] getLineOfExplain( JdbcInterface jdbc, String sql,
            int lineIndex ) throws SQLException {
        ArrayList< String > explainList = jdbc.query( sql );
        return explainList.get( lineIndex ).split( "\\|" );
    }

    public static ArrayList< String > getExplain( JdbcInterface jdbc,
            String sql ) throws SQLException {
        return jdbc.query( sql );
    }

    public static void insertData( JdbcInterface jdbc, String tableName,
            int num ) throws SQLException {
        String sql = "insert into " + tableName
                + " values (null,\"123\",\"123\")";
        for ( int i = 1; i < num; i++ ) {
            sql = sql + ",(null,\"123\",\"123\")";
        }
        sql = sql + ";";
        jdbc.update( sql );
    }
}
