package thread;

import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import data.ReadProperties;

/****
 * 主要启动类，使用的线程启动
 * @author 马健
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
	 * 读取配置的属性文件
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
			System.out.println(Thread.currentThread().getName()+"数据库连接字符串："+dburl);
			System.out.println(Thread.currentThread().getName()+"数据库账户："+username);
			System.out.println(Thread.currentThread().getName()+"数据库密码："+password);
			System.out.println(Thread.currentThread().getName()+"读取数据文件的路径："+filepath);
			System.out.println(Thread.currentThread().getName()+"记录插入的数量数："+insertcounts);
			System.out.println(Thread.currentThread().getName()+"SQL："+antennaFunction);
		} catch (Exception e) {
			//e.printStackTrace();
			log.error("加载配置文件错误", e);
		}
	}
    public void run() {
    	this.readProper();
    }
    /***
     * 主方法入口
     * @param args
     */
    public static void main(String[] args) {
    	ExcelThread ex = new ExcelThread();
    	Thread th1=new Thread(ex);
    	th1.start();
    	//new Thread(ex, "线程1").start();
    }
}
