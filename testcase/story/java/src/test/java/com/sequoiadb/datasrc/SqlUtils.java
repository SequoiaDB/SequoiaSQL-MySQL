package com.sequoiadb.datasrc;

import com.sequoiadb.testcommon.SdbTestBase;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @Descreption 简单封装的jdbc帮助类
 * @Author YiPan
 * @Date 2021/6/1
 */
public class SqlUtils extends SdbTestBase {
    /**
     * @param sql
     * @param url
     * @throws Exception
     */
    public void update( String sql, String url ) throws Exception {
        Connection conn = null;
        try {
            // 注册驱动,处理异常
            Class.forName( "com.mysql.jdbc.Driver" );
            // 连接数据库 处理异常
            conn = DriverManager.getConnection( url );
            Statement statement = conn.createStatement();
            // 正常执行修改语句无返回结果
            statement.executeUpdate( sql );
        } finally {
            if ( conn != null ) {
                conn.close();
            }
        }
    }

    /**
     *
     * @param sql
     * @param url
     * @return 查询结果
     * @throws Exception
     */
    public List< String > query( String sql, String url ) throws Exception {
        Connection conn = null;
        try {
            // 注册驱动,处理异常
            Class.forName( "com.mysql.jdbc.Driver" );
            // 连接数据库 处理异常
            conn = DriverManager.getConnection( url );
            Statement statement = conn.createStatement();
            // 查询结果
            ResultSet resultSet = statement.executeQuery( sql );
            String result;
            List< String > results = new ArrayList< String >();
            // 遍历
            while ( resultSet.next() ) {
                // 获取列数
                ResultSetMetaData ssdata = resultSet.getMetaData();
                int k = ssdata.getColumnCount();
                int i = 1;
                // 遍历每一列拼接
                result = "";
                while ( true ) {
                    if ( i <= k ) {
                        if ( result != "" ) {
                            result = result + "|" + resultSet.getString( i );
                        } else {
                            result = resultSet.getString( i );
                        }
                        i++;
                    } else {
                        break;
                    }
                }
                results.add( result );
                result = "";
            }
            return results;
        } finally {
            if ( conn != null ) {
                conn.close();
            }
        }
    }

    /**
     * @param url
     * @return
     * @throws Exception
     */
    public Connection getConn( String url ) throws Exception {
        Connection conn = null;
        // 注册驱动,处理异常
        Class.forName( "com.mysql.jdbc.Driver" );
        // 连接数据库 处理异常
        return DriverManager.getConnection( url );
    }

    /**
     * 获取xml传参的单个实例url
     * 
     * @return
     */
    public String getUrl() {
        return "jdbc:mysql://" + mysql1 + "?user=" + mysqluser + "&password="
                + mysqlpasswd
                + "&useUnicode=true&useSSL=false&characterEncoding=utf-8&autoReconnect=true";
    }

    /**
     * 获取xml传参的所有实例url
     * 
     * @return
     */
    public List< String > getUrls() {
        List< String > urls = new ArrayList<>();
        urls.add( "jdbc:mysql://" + mysql1 + "?user=" + mysqluser + "&password="
                + mysqlpasswd
                + "&useUnicode=true&useSSL=false&characterEncoding=utf-8" );
        urls.add( "jdbc:mysql://" + mysql2 + "?user=" + mysqluser + "&password="
                + mysqlpasswd
                + "&useUnicode=true&useSSL=false&characterEncoding=utf-8" );
        return urls;
    }

}
