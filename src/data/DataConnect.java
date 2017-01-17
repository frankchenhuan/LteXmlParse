package data;

import java.io.InputStream;
import java.sql.*;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DataConnect {
	protected static final Log log = LogFactory.getLog(ReadProperties.class);
	public static void main(String[] args){
		String dburl ="";  
		String username = "";
		String password = "";
		try {
			InputStream in =ReadProperties.class.getClassLoader().getResourceAsStream("config.properties");
			Properties prop = new Properties(); 
			prop.load(in);
			
			dburl = prop.getProperty("dburl");
			username = prop.getProperty("username");
			password = prop.getProperty("password");
			System.out.println("dburl:"+dburl);
			System.out.println("username:"+username);
			System.out.println("password:"+password);
			//System.out.println(prop.getProperty("filepath"));
		} catch (Exception e) {
			//e.printStackTrace();
			log.error("���������ļ�����", e);
		}
//		try {
//			Class.forName("sun.jdbc.odbc.JdbcOdbcDriver");
//			Connection conn = DriverManager.getConnection(dburl, username, password);
//			//�޲Σ���Ӱ���
//		    //Statement stmt = conn.createStatement();
//		    //stmt.addBatch("insert into person(id, name) values(1,'shong')");
//		    //stmt.addBatch("insert into person(id, name) values(2,\"hacks\")");
//		    //stmt.executeBatch();
//		    //stmt.close();
//		    //conn.close();
//
//		   //����
//		   PreparedStatement ps = conn.prepareStatement("insert into person(id, name) values(?,?)");
//		   ps.setInt(1, 3);//���ò���
//		   ps.setString(2, "elise");
//		   ps.addBatch(); //�����������������
//		   ps.setInt(1, 4);
//		   ps.setString(2, "plegall");
//		   ps.addBatch();
//		   ps.setInt(1, 5);
//		   ps.setString(2, "mike");
//		   ps.addBatch();
//		   ps.executeBatch(); //ִ��������
//		   ps.close(); //���ر�
//		   conn.close();
//		} catch (SQLException e) {
//			e.printStackTrace();
//		} catch (ClassNotFoundException e) {
//			e.printStackTrace();
//		}
	}
}
