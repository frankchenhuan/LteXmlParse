package cfg;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Config {
	private static String commit_count;
	private static String dateformat = "yyyy-mm-dd";
	private static String[] filter_files;

	private static Properties pro;
	protected static final Log log = LogFactory.getLog(Config.class);

	public static void init(int type) {
		InputStream in = null;

		String s = null;
		switch (type) {
		case 1:
			s = "cfg.properties";
			break;
		case 2:
			s = "pmcfg.properties";
			break;
		case 3:
			s = "wpcfg.properties";
			break;
		case 4:
			s = "mrcfg.properties";
		default:
			break;
		}

		pro = new Properties();
		try {
			String path = System.getProperty("user.dir") + "/conf/" + s;
			in = new FileInputStream(path);
			pro.load(in);
			commit_count = pro.getProperty("commit_count");
			dateformat = pro.getProperty("dateformat");
			s = pro.getProperty("filter_file");
			if (s != null) {
				filter_files = s.split(",");
			}
			log.info("初始化配置文件完成" + path);
		} catch (IOException e) {
			log.error("加载配置文件错误", e);
		}
		try {
			in.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			// e.printStackTrace();
		}
	}
	
	public static void init(String name) {
		InputStream in = null;


		pro = new Properties();
		try {
			String path = System.getProperty("user.dir") + "/conf/" + name;
			in = new FileInputStream(path);
			pro.load(in);
			commit_count = pro.getProperty("commit_count");
			dateformat = pro.getProperty("dateformat");
			String s = pro.getProperty("filter_file");
			if (s != null) {
				filter_files = s.split(",");
			}
			log.info("初始化配置文件完成" + path);
		} catch (IOException e) {
			log.error("加载配置文件错误", e);
		}
		try {
			in.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			// e.printStackTrace();
		}
	}

	/**
	 * 根据名称得到参数值
	 */
	public static String getValue(String name) {
		return pro.getProperty(name);
	}

	public static String getDateFormat() {
		return Config.dateformat;
	}

	public static int getCommitCount() {
		return Integer.parseInt(Config.commit_count.trim());
	}

	public static String getConfType() {
		return pro.getProperty("confType");
	}

	public static String[] getFilter_files() {
		return filter_files;
	}

	public static boolean isFilter() {
		String s = pro.getProperty("filter");
		if (s != null && s.equalsIgnoreCase("y")) {
			return true;
		} else {
			return false;
		}
	}

	public static boolean isDelTempFile() {
		String s = pro.getProperty("delTempFile");
		if (s != null && s.equalsIgnoreCase("n")) {
			return false;
		} else {
			return true;
		}
	}

	public static String getCharSet() {
		String s = pro.getProperty("charset");
		if (s == null || s.trim().equals("")) {
			return "UTF-8";
		} else {
			return s;
		}
	}

	public static void main(String s[]) {
		Config.init(1);
		System.out.println(Config.getDateFormat());
	}
}
