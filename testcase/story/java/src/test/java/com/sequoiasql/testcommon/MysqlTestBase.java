package com.sequoiasql.testcommon;

import org.testng.annotations.AfterSuite;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Parameters;

public class MysqlTestBase {
    protected static String coordUrl;
    protected static String hostName;
    protected static String serviceName;
    protected static String dsHostName;
    protected static String dsServiceName;
    protected static String mysql;
    protected static String innodb;
    protected static String mysqluser;
    protected static String mysqlpasswd;

    @Parameters({ "HOSTNAME", "SVCNAME", "DSHOSTNAME", "DSSVCNAME", "MYSQLURL",
            "INNODB", "MYSQLUSER", "MYSQLPASSWD" })
    @BeforeSuite(alwaysRun = true)
    public static void initSuite( String HOSTNAME, String SVCNAME,
            String DSHOSTNAME, String DSSVCNAME, String MYSQLURL, String INNODB,
            String MYSQLUSER, String MYSQLPASSWD ) {
        System.out.println( "initSuite....." );
        innodb = INNODB;
        hostName = HOSTNAME;
        serviceName = SVCNAME;
        dsHostName = DSHOSTNAME;
        dsServiceName = DSSVCNAME;
        coordUrl = HOSTNAME + ":" + SVCNAME;
        mysql = MYSQLURL;
        mysqluser = MYSQLUSER;
        mysqlpasswd = MYSQLPASSWD;
    }

    @BeforeTest()
    public static synchronized void initTestGroups() {
    }

    @AfterTest()
    public static synchronized void finiTestGroups() {
    }

    @AfterSuite()
    public static void finiSuite() {
    }

    public static String initUrl( String addr, String user, String passwd ) {
        String urlbase = "jdbc:mysql://%s?user=%s&password=%s&useUnicode=true&useSSL=false&characterEncoding=utf-8&autoReconnect=true";
        String url = String.format( urlbase, addr, user, passwd );
        return url;
    }

    public static String getUrlOfSequoiadb() {
        return initUrl( MysqlTestBase.mysql, MysqlTestBase.mysqluser,
                MysqlTestBase.mysqlpasswd );
    }

    public static String getUrlOfInnodb() {
        return initUrl( MysqlTestBase.innodb, MysqlTestBase.mysqluser,
                MysqlTestBase.mysqlpasswd );
    }
}
