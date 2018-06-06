package com.pentaho.bigsql;

import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.apache.commons.beanutils.BeanUtils;
import org.pentaho.platform.engine.core.system.PentahoSessionHolder;
import org.pentaho.platform.engine.core.system.PentahoSystem;

import com.ibm.db2.jcc.DB2ConnectionPoolDataSource;
import com.ibm.db2.jcc.DB2Driver;
import com.ibm.db2.jcc.DB2PooledConnection;

public class PentahoBigSqlDriver implements Driver {

  private static final String JDBC_PREFIX = "jdbc:pen-bigsql";
  private static final String USER_PROPERTY = "user";
  private static final String PASS_PROPERTY = "password";
  
  // used just to delegate version methods.  Actual connection from .connect should use a DB2TrustedPooledConnection
  private final DB2Driver db2Driver = new DB2Driver();

  @Override
  public Connection connect( String url, Properties i ) throws SQLException {
    // Parse URL to pull out server/port/db
    //String user = getCurrentPentahoUser();

    final Properties info;
    if(i == null) {
      info = new Properties();
    } else {
      info = (Properties) i.clone();
    }
    
    String user = "myuser2";
    
    URI uri;
    try {
      if(url.toLowerCase().startsWith( JDBC_PREFIX )) {
        uri = new URI( url.substring( 5 ) );
      } else {
        throw new Exception("Malformed URL");
      }
    } catch(Exception e) {
      throw new SQLException(e);
    }

    //Set all properties that were specified in the queryString
    if(uri.getQuery() != null) {
    Arrays.stream( uri.getQuery().split( ";" ) )
          .forEach( kv -> { 
                            String[] sp = kv.split( "="); 
                            info.setProperty(sp[0].trim(), sp[1].trim());
                          } );
    }
    
    //Parse user and optional password from uri
    String userInfo = uri.getUserInfo();
    if(userInfo != null && !userInfo.trim().isEmpty()) {
      int indexOfSplit = userInfo.indexOf( ":" );
      //Contains a password
      if(indexOfSplit >= 0) {
        info.setProperty( USER_PROPERTY, userInfo.substring( 0, indexOfSplit ) );
        info.setProperty( PASS_PROPERTY, userInfo.substring( indexOfSplit + 1 ) );
      } else {
        info.setProperty( "user", userInfo );
      }
    }
    
    info.setProperty( "serverName", uri.getHost() );
    info.setProperty( "portNumber", Integer.toString( uri.getPort() ) );
    info.setProperty( "databaseName", uri.getPath().substring( 1 ) );
    
    if( !info.containsKey( "driverType" )) {
      info.setProperty( "driverType", "4" );
    }
    
    DB2ConnectionPoolDataSource db2Ds = new DB2ConnectionPoolDataSource();
    
    //Convert the properties to a Map<String, String>
    Map<String, String> propMap = info.stringPropertyNames().stream().collect( 
        Collectors.toMap( x -> x, x -> info.getProperty( x ) ) );
    
    try {
      BeanUtils.populate( db2Ds, propMap );
    } catch(InvocationTargetException | IllegalAccessException e) {
      throw new SQLException(e);
    }
    
    String userName = info.getProperty( USER_PROPERTY );
    String userPass = info.getProperty( PASS_PROPERTY );
    
    info.remove( USER_PROPERTY );
    info.remove( PASS_PROPERTY );
    
    Object[] objects = db2Ds.getDB2TrustedPooledConnection( userName, userPass, info );
    DB2PooledConnection pooledCon = (DB2PooledConnection) objects[ 0 ];
    
    byte[] cookie = (byte[]) objects[ 1 ];
    return pooledCon.getDB2Connection( cookie, user, null, null, null, null, info );
    
  }

  private String getCurrentPentahoUser() {
    try {
      if ( PentahoSystem.getInitializedOK() ) {
        return PentahoSessionHolder.getSession().getName();
      } else {
        return "";
      }
    } catch ( Exception e ) {
      // log failure.
    }
    return "";
  }

  @Override
  public boolean acceptsURL( String url ) {
    return url.startsWith( JDBC_PREFIX );
  }

  public DriverPropertyInfo[] getPropertyInfo( String url, Properties info ) throws SQLException {
    return db2Driver.getPropertyInfo( url, info );
  }

  public int getMajorVersion() {
    return db2Driver.getMajorVersion();
  }

  public int getMinorVersion() {
    return db2Driver.getMinorVersion();
  }

  public boolean jdbcCompliant() {
    return db2Driver.jdbcCompliant();
  }

  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return db2Driver.getParentLogger();
  }
  
  
  
}
