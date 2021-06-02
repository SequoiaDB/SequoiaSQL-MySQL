package com.sequoiadb.testcommon;

import org.testng.annotations.AfterSuite;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Parameters;

public class SdbTestBase {
    protected static String coordUrl;
    protected static String hostName;
    protected static String serviceName;
    protected static String dsHostName;
    protected static String dsServiceName;
    protected static String mysql1;
    protected static String mysql2;
    protected static String mysqluser;
    protected static String mysqlpasswd;

    @Parameters({ "HOSTNAME", "SVCNAME", "DSHOSTNAME", "DSSVCNAME", "MYSQL1",
            "MYSQL2", "MYSQLUSER", "MYSQLPASSWD" })
    @BeforeSuite(alwaysRun = true)
    public static void initSuite( String HOSTNAME, String SVCNAME,
            String DSHOSTNAME, String DSSVCNAME, String MYSQL1, String MYSQL2,
            String MYSQLUSER, String MYSQLPASSWD ) {
        System.out.println( "initSuite....." );
        hostName = HOSTNAME;
        serviceName = SVCNAME;
        dsHostName = DSHOSTNAME;
        dsServiceName = DSSVCNAME;
        coordUrl = HOSTNAME + ":" + SVCNAME;
        mysql1 = MYSQL1;
        mysql2 = MYSQL2;
        mysqluser = MYSQLUSER;
        mysqlpasswd = MYSQLPASSWD;
    }

    @BeforeTest()
    public static synchronized void initTestGroups() {
    }

    @AfterTest()
    public static synchronized void finiTestGroups() {
    }

    @AfterSuite(alwaysRun = true)
    public static void finiSuite() {
    }
}
