package data;


import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class OracelConnectFactory {
	String dburl;  
	String username;  
	String password;  
	Connection conn = null;  
	Statement stmt = null;  
	ResultSet rs = null;
	protected static Log log = LogFactory.getLog(OracelConnectFactory.class);
	//protected static final Log log = LogFactory.getLog(OracelConnectFactory.class);
	public void connectConfig(){
		InputStream in =ReadProperties.class.getClassLoader().getResourceAsStream("config.properties");
		Properties prop = new Properties(); 
		try {
			prop.load(in);
			dburl = prop.getProperty("dburl");
			username = prop.getProperty("username");
			password = prop.getProperty("password");
			log.info("读取ORACLE数据库配置文件完成");
			//System.out.println(prop.getProperty("filepath"));
		} catch (IOException e) {
			log.error("读取ORACLE数据库配置文件失败");
			e.printStackTrace();
		}
	}
	
	 public void jdbc() {  
	    try {  
	        Class.forName("oracle.jdbc.driver.OracleDriver").newInstance();  
	        conn = DriverManager.getConnection(dburl, username, password); 
	        stmt = conn.createStatement();
	    } catch (Exception e) {  
	        e.printStackTrace();  
	    }  
	 }  
	 public void close() {  
		 try {
			 stmt.close(); 
			 conn.close();  
		   	} catch (Exception e) {  
		     e.printStackTrace();  
		    }  
	 }  
	 public static void main(String[] args) {
		 OracelConnectFactory of = new OracelConnectFactory();
		 of.connectConfig();
	 }

}
