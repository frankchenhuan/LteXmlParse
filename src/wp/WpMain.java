package wp;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


import cfg.Config;
import cfg.DataSource;

public class WpMain {

	protected static final Log log = LogFactory.getLog(WpMain.class);
	/**
	 * @param args
	 * @throws SQLException
	 */
	public static void main(String[] args) {
		Config.init(3);
		String factory = args[0];
		String filepath = args[1];
		String omc = args[2];
		String repdate;
		if (args.length >= 4) {
			repdate = args[3].trim();
		} else {
			SimpleDateFormat df = new SimpleDateFormat(Config.getDateFormat());
			Calendar cal = Calendar.getInstance();
			cal.add(Calendar.DATE, -1);
			repdate = df.format(cal.getTime());
		}
		//String path = "E:\\LTE数据\\HW\\WP\\HW_OMC06_20140112\\11";
		try {
			DataSource.init();
			WpParse p = new WpParse(factory, omc,repdate);
			/**
			 * 参数1：路径
			 * 参数2：是否删除临时文件
			 * */
			p.parseAndOut(filepath,Config.isDelTempFile());
			//p.parseAndOut(filepath, true);
			// p.parseXML(new File(
			// "E:\\LTE数据\\HW\\WP\\HW_OMC06_20140112\\ENB-NRM-ALLV2.1.0-HOMEASCOMM-20140112-0344.xml"));
		} catch (Exception e) {
			log.error("采集出错:",e);
		}
	}
}
