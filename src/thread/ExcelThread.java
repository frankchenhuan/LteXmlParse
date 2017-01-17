package thread;

import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import data.ReadProperties;

/****
 * ��Ҫ�����࣬ʹ�õ��߳�����
 * @author ��
 *
 */
public class ExcelThread implements Runnable{
	String dburl ="";  
	String username = "";
	String password = "";
	String filepath="";
	String antennaFunction="";
	Integer insertcounts=null;
	protected static final Log log = LogFactory.getLog(ExcelThread.class);
	/***
	 * ��ȡ���õ������ļ�
	 */
	public void readProper(){
		try {
			InputStream in =ReadProperties.class.getClassLoader().getResourceAsStream("config.properties");
			Properties prop = new Properties(); 
			prop.load(in);
			dburl=prop.getProperty("dburl");
			username=prop.getProperty("username");
			password=prop.getProperty("password");
			filepath=prop.getProperty("filepath");
			antennaFunction=prop.getProperty("AntennaFunction");
			insertcounts=Integer.valueOf(prop.getProperty("insertcounts"));
			System.out.println(Thread.currentThread().getName()+"���ݿ������ַ�����"+dburl);
			System.out.println(Thread.currentThread().getName()+"���ݿ��˻���"+username);
			System.out.println(Thread.currentThread().getName()+"���ݿ����룺"+password);
			System.out.println(Thread.currentThread().getName()+"��ȡ�����ļ���·����"+filepath);
			System.out.println(Thread.currentThread().getName()+"��¼�������������"+insertcounts);
			System.out.println(Thread.currentThread().getName()+"SQL��"+antennaFunction);
		} catch (Exception e) {
			//e.printStackTrace();
			log.error("���������ļ�����", e);
		}
	}
    public void run() {
    	this.readProper();
    }
    /***
     * ���������
     * @param args
     */
    public static void main(String[] args) {
    	ExcelThread ex = new ExcelThread();
    	Thread th1=new Thread(ex);
    	th1.start();
    	//new Thread(ex, "�߳�1").start();
    }
}
