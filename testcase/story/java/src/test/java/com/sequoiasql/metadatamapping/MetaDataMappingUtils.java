package com.sequoiasql.metadatamapping;

import com.sequoiadb.base.DBCursor;
import com.sequoiadb.base.DBCollection;
import com.sequoiadb.base.Sequoiadb;
import com.sequoiasql.testcommon.MysqlTestBase;
import org.bson.BasicBSONObject;
import org.testng.Assert;

public class MetaDataMappingUtils extends MysqlTestBase {

    public static String getInstGroupName( Sequoiadb db, String mysqlUrl ) {
        String[] mysql1 = mysqlUrl.split( ":" );
        int port = Integer.parseInt( mysql1[ 1 ].toString() );
        String instanceGroupName = "";
        DBCollection dbcl = db.getCollectionSpace( "HASysGlobalInfo" )
                .getCollection( "HARegistry" );
        BasicBSONObject matcher = new BasicBSONObject();
        matcher.put( "Port", port );
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
