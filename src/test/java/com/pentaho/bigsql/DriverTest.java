package com.pentaho.bigsql;

import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.commons.beanutils.BeanUtils;
import org.junit.Ignore;
import org.junit.Test;

import com.ibm.db2.jcc.DB2ConnectionPoolDataSource;

public class DriverTest {

  @Test
  public void test() throws SQLException, InterruptedException {
    
    PentahoBigSqlDriver driver = new PentahoBigSqlDriver();
    Connection c = driver.connect( "jdbc:pen-bigsql://bigsql:password@ec2-54-146-247-143.compute-1.amazonaws.com:32051/bigsql", null );
    
    ResultSet r = c.prepareStatement( "SELECT TABSCHEMA, TABNAME FROM SYSCAT.TABLES" ).executeQuery();
    while(r.next()) {
      
      System.out.println( r.getString( 1 ).trim() + "." + r.getString( 2 ) );
      
    }
    
    r.close();
    
    ResultSet r2 = c.prepareStatement( "SELECT * FROM MYUSER2.MYUSER2TABLE" ).executeQuery();
    while(r2.next()) {
      System.out.println( r2.getString(1) );
    }
    r2.close();
    
    Thread.sleep( 20000 );
    c.close();
    
  }
  
  @Test @Ignore
  public void UriTest() throws Exception {
    
    Properties info = new Properties();
    
    URI uri = new URI( "db2://username:password@localhost:3000/bigsql?prop1=x;prop2=y;prop3=11000" );
    
    System.out.println( uri.getScheme() );
    System.out.println( uri.getHost() );
    System.out.println( uri.getUserInfo() );
    System.out.println( uri.getPath() );
    System.out.println( uri.getQuery() );
    

  }
  
  
  
}
