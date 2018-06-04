package com.pentaho.bigsql;

import com.ibm.db2.jcc.DB2Driver;
import org.pentaho.platform.engine.core.system.PentahoSessionHolder;
import org.pentaho.platform.engine.core.system.PentahoSystem;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

public class PentahoBigSqlDriver implements Driver {

  // used just to delegate version methods.  Actual connection from .connect should use a DB2TrustedPooledConnection
  private final DB2Driver db2Driver = new DB2Driver();


  @Override public Connection connect( String url, Properties info ) {
    // Parse URL to pull out server/port/db
    String user = getCurrentPentahoUser();

//    DB2ConnectionPoolDataSource db2DataSource =
//      new DB2ConnectionPoolDataSource();
//    db2DataSource.setServerName( "ec2-54-146-247-143.compute-1.amazonaws.com" );
//    db2DataSource.setPortNumber( 32051 );
//    db2DataSource.setDatabaseName( "bigsql" );
//    db2DataSource.setDriverType( 4 );
//    Object[] objects = db2DataSource.getDB2TrustedPooledConnection( "bigsql", "password", new Properties() );
//    DB2PooledConnection pooledCon = (DB2PooledConnection) objects[ 0 ];
//    byte[] cookie = (byte[]) objects[ 1 ];
//    return pooledCon.getDB2Connection( cookie, user, null,
//      null, null, null, new Properties() );
    return null;
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

  @Override public boolean acceptsURL( String url ) {
    return url.startsWith( "jdbc:pen-bigsql" );
  }

  @Override public DriverPropertyInfo[] getPropertyInfo( String url, Properties info ) throws SQLException {
    return db2Driver.getPropertyInfo( url, info );
  }

  @Override public int getMajorVersion() {
    return db2Driver.getMajorVersion();
  }

  @Override public int getMinorVersion() {
    return db2Driver.getMinorVersion();
  }

  @Override public boolean jdbcCompliant() {
    return db2Driver.jdbcCompliant();
  }

  @Override public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return db2Driver.getParentLogger();
  }
}
