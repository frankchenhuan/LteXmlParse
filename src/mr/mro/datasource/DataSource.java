package mr.mro.datasource;

import org.apache.commons.dbcp.BasicDataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class DataSource {
	private static BasicDataSource bds;

	private static String driverClass;

	private static String connectUrl;

	private static String userName;

	private static String password;

	private static String initSize;

	private static String maxIdel;

	private static String waitTime;

	private static boolean initflag = false;

	protected final static Log log = LogFactory.getLog(DataSource.class);

	public static void init() throws IOException {
		if (initflag) {
			return;
		}
		Properties p = new Properties();
		 InputStream in = Thread.currentThread().getContextClassLoader()
		 .getResourceAsStream("pool.properties");
		p.load(in);
		driverClass = p.getProperty("driverClass");
		connectUrl = p.getProperty("connectUrl");
		userName = p.getProperty("userName");
		password = p.getProperty("password");
		initSize = p.getProperty("initSize");
		maxIdel = p.getProperty("maxIdel");
		waitTime = p.getProperty("waitTime");
		log.info("driverClass=" + driverClass);
		log.info("connectUrl=" + connectUrl);
//		log.info("userName=" + userName);
//		log.info("password=" + password);
		log.info("initSize=" + initSize);
		log.info("maxIdel=" + maxIdel);
		log.info("waitTime=" + waitTime);
		bds = new BasicDataSource();
		bds.setDriverClassName(driverClass);
		bds.setUrl(connectUrl);
		bds.setUsername(userName);
		bds.setPassword(password);
		bds.setInitialSize(Integer.parseInt(initSize));
		bds.setMaxIdle(Integer.parseInt(maxIdel));
		initflag = true;
	}

	/***************************************************************************
	 * 得到连接
	 **************************************************************************/
	public static Connection getConnection() throws SQLException {
		// bds.close();
		if (bds == null)
			return null;
		return bds.getConnection();
	}

	/***************************************************************************
	 * 关闭连接池
	 **************************************************************************/
	public static void close() throws SQLException {
		bds.close();
	}

	// test
	public static void main(String a[]) {
		try {
			getConnection().close();
			System.out.println("test OK!");
			close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
