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
//			log.error("����ftp�����ļ�����", e);
//		}
		//������
//		  System.out.println("------------testProperties-------------"); 
//          //��properties�ļ����ص������ֽ����� 
//          InputStream is;
//          //����һ��Properties���� 
//          Properties prop = new Properties(); 
//		try {
//			is = new FileInputStream("D:\\Documents and Settings\\Administrator\\Workspaces\\MyEclipse\\XmlParse\\src\\config.properties");
//			//�����м���properties�ļ���Ϣ 
//			prop.load(is);
//	        //ѭ�����������Ϣ 
//			for (Object key : prop.keySet()) { 
//                  System.out.println(key + "=" + prop.get(key)); 
//			} 
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} 
		//������
//		ResourceBundle resb2 = ResourceBundle.getBundle("config", Locale.getDefault()); 
//        System.out.println(resb2.getString("username")); 
		//������
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
			log.error("���������ļ�����", e);
		}
	}
}

