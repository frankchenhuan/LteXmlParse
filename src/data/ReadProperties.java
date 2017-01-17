package data;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Locale;
import java.util.Properties;
import java.util.ResourceBundle;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ReadProperties {
	private static Properties pro;
	protected static final Log log = LogFactory.getLog(ReadProperties.class);
	public static void main(String[] args) {
//		InputStream in = Thread.currentThread().getContextClassLoader()
//		.getResourceAsStream("config.properties");
//		pro = new Properties();
//		try {
//			pro.load(in);
//			System.out.println(pro.getProperty("server"));
//			System.out.println(pro.getProperty("username"));
//			System.out.println(pro.getProperty("password"));
//			in.close();
//		} catch (Exception e) {
//			log.error("加载ftp配置文件错误", e);
//		}
		//方法二
//		  System.out.println("------------testProperties-------------"); 
//          //将properties文件加载到输入字节流中 
//          InputStream is;
//          //创建一个Properties容器 
//          Properties prop = new Properties(); 
//		try {
//			is = new FileInputStream("D:\\Documents and Settings\\Administrator\\Workspaces\\MyEclipse\\XmlParse\\src\\config.properties");
//			//从流中加载properties文件信息 
//			prop.load(is);
//	        //循环输出配置信息 
//			for (Object key : prop.keySet()) { 
//                  System.out.println(key + "=" + prop.get(key)); 
//			} 
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} 
		//方法三
//		ResourceBundle resb2 = ResourceBundle.getBundle("config", Locale.getDefault()); 
//        System.out.println(resb2.getString("username")); 
		//方法四
		try {
			InputStream in =ReadProperties.class.getClassLoader().getResourceAsStream("config.properties");
			Properties prop = new Properties(); 
			prop.load(in);
			System.out.println(prop.getProperty("server"));
			System.out.println(prop.getProperty("username"));
			System.out.println(prop.getProperty("password"));
			System.out.println(prop.getProperty("filepath"));
		} catch (Exception e) {
			//e.printStackTrace();
			log.error("加载配置文件错误", e);
		}
	}
}

