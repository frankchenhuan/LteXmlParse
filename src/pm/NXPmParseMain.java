package pm;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cfg.ConfObj;
import cfg.Config;
import cfg.DataSource;
import cfg.Field;
import cfg.KeyConstant;
import cfg.Tools;
import cm.CmParseException;

public class NXPmParseMain {

	/** 全局的计数变量,记录共处理多少个对象 */
	static int count = 0;

	/** 全局计数变量 记录共处理多少个文件 */
	static int fileCount = 0;

	/** 当前线程数 */
	static private int thread_current_num = 0;

	/** 存放线程 */
	//static private List<Thread> threadList = new ArrayList<Thread>();

	/**
	 * 保存配置文件中的对象
	 */
	static Map<String, ConfObj> objcfgs = new ConcurrentHashMap<String, ConfObj>();

	/**
	 * 保存常量
	 */
	static Map<String, String> constant = new ConcurrentHashMap<String, String>();

	/**
	 * 数据连接对象
	 */
	static Connection con;
	static Map<String, PreparedStatement> statementMap = new ConcurrentHashMap<String, PreparedStatement>();

	protected static final Log log = LogFactory.getLog(NXPmParseMain.class);

	/**
	 * 存放所有数据对象，以数据文件中dn作为key值
	 */
	static Map<String, PmDataObj> pmData = new HashMap<String, PmDataObj>();

	public static void main(String[] args) {
		if (args.length < 3) {
			log.error("输入参数数量不足");
			return;
		}
		String factory = args[0];// 参数1 厂家
		String filepath = args[1];// 参数2 文件路径
		String omc = args[2]; // 参数2 omc
		int flag;
		if (args.length >= 4) {
			// 参数4 是否是补采 1正常采集 2补采
			flag = Integer.parseInt(args[3].trim());
		} else {
			flag = 1;
		}
		int threadnum = 1;// 线程数
		if (args.length >= 5) {
			threadnum = Integer.parseInt(args[4].trim());
		} else {
			threadnum = 1;
		}

		constant.put(KeyConstant.KEY_FACTORY, factory);
		constant.put(KeyConstant.KEY_OMCID, omc);
		constant.put(KeyConstant.KEY_XMPPATH, filepath);
		try {
			Config.init(2);
			DataSource.init();
			String conftype = Config.getConfType();
			if (conftype.equalsIgnoreCase("db")) {
				objcfgs = Tools.initObjByDB(omc);
			} else if (conftype.equalsIgnoreCase("xml")) {
				objcfgs = Tools.initObjByXML(omc);
			}
			con = DataSource.getConnection();
			statementMap = Tools.initStatement(con, objcfgs, flag);
			parsePathAllXml(filepath, threadnum);
			insertObjData();
			wirteLog();
			Tools.closeCon(con, statementMap);
		} catch (Exception e) {
			log.error("", e);
			System.exit(1);
		}
	}

	public static void insertObjData() throws SQLException {
		log.info("开始入库");
		Iterator<PmDataObj> pdolist = pmData.values().iterator();
		PreparedStatement ps;
		while (pdolist.hasNext()) {

			PmDataObj pmd = pdolist.next();
			ConfObj pmc = pmd.getPmConfObj();
			ps = statementMap.get(pmc.getObjectType());
			List<Field> fs = pmc.getFields();
			for (int i = 0; i < fs.size(); i++) {
				Field f = fs.get(i);
				setPrepared(ps, f, pmd.getValues(), pmd);// 设置PreparedStatement参数
			}
			ps.addBatch();
			count++;
			pmc.addNum();

			if (count / Config.getCommitCount() >= 1
					&& count % Config.getCommitCount() == 0) {
				Tools.execute(con, statementMap);
			}

			if (!pmc.isExistsSub()) {
				continue;
			}

			/** 对含有分项的指标进行入库处理 */
			PmSubConfObj pmsc = pmc.getSubConf();
			Map<String, Map> pmsd = pmd.getSubTargetValues();
			List<Field> pmsclist = pmsc.getFields();
			ps = statementMap.get(pmc.getObjectType() + "-"
					+ pmsc.getTablename());
			Iterator<String> it = pmsd.keySet().iterator();
			while (it.hasNext()) {
				String name = it.next();
				Map<String, String> subtarget = pmsd.get(name);
				Iterator<String> is = subtarget.keySet().iterator();
				while (is.hasNext()) {
					String k = is.next();
					setPrepared(ps, pmsc.getTargetNameField(), pmc
							.getSubTarget(name), pmd);
					setPrepared(ps, pmsc.getSubTargetNameField(), k, pmd);
					setPrepared(ps, pmsc.getTargetValueField(), subtarget
							.get(k), pmd);
					for (int x = 0; x < pmsclist.size(); x++) {
						Field subf = pmsclist.get(x);
						setPrepared(ps, subf, pmd.getValues(), pmd);
					}
					ps.addBatch();
					pmsc.addNum();
					count++;
					if (count / Config.getCommitCount() >= 1
							&& count % Config.getCommitCount() == 0) {
						Tools.execute(con, statementMap);
					}
				}
			}
		}
		Tools.execute(con, statementMap);
	}

	public static void setPrepared(PreparedStatement ps, Field field,
			String value, PmDataObj pmd) throws SQLException {
		Map<String, String> values = pmd.getValues();
		String filename = values.get(KeyConstant.KEY_FILENAME);
		Tools.setPrepared(ps, field, value, pmd.getPmConfObj().getObjectType(),
				filename);
	}

	public static void setPrepared(PreparedStatement ps, Field field,
			Map<String, String> values, PmDataObj pmd) throws SQLException {
		String s = values.get(field.getName().toUpperCase());
		String objname = pmd.getPmConfObj().getObjectType();
		String filename = values.get(KeyConstant.KEY_FILENAME);

		Tools.setPrepared(ps, field, s, pmd.getPmConfObj().getObjectType(),
				filename);

	}

	/**
	 * 解析路径下的xml文件并入库
	 * 
	 * @throws XMLStreamException
	 * @throws SQLException
	 * 
	 * @throws CmParseException
	 * @throws SQLException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static void parsePathAllXml(String filepath, int threadnum)
			throws XMLStreamException, SQLException, IOException,
			InterruptedException {
		File f = new File(filepath);
		File fs[] = f.listFiles();
		
		ThreadPoolExecutor execute = (ThreadPoolExecutor)Executors.newFixedThreadPool(threadnum);
		
		for (int f_i = 0; f_i < fs.length; f_i++) {

			// 如果不是文件或不是xml文件则直接跳过
			if (!fs[f_i].isFile()
					|| !fs[f_i].getName().toLowerCase().endsWith("xml")) {
				continue;
			}

			// 过滤需要过滤的文件
			boolean isContinue = false;
			String[] filter_files = Config.getFilter_files();
			String name = fs[f_i].getName();
			if (filter_files != null) {

				for (int i = 0; i < filter_files.length; i++) {
					isContinue = false;
					if (name.indexOf(filter_files[i]) != -1) {
						isContinue = true;
						break;
					}
				}
			}
			if (isContinue) {
				continue;
			}

			// Thread.sleep(5);
			/* log.debug(fs[f_i].getName()); */
			/** 查看线程状态并移除结束的线程 */
			int num = 0;
			/*for (int threadsize = threadList.size(); threadsize >= threadnum; threadsize = threadList
					.size()) {
				List<Thread> removeThread = new ArrayList<Thread>();
				for (int x = 0; x < threadList.size(); x++) {
					Thread t = threadList.get(x);
					if (t.getState().equals(Thread.State.TERMINATED)) {
						removeThread.add(t);
					}
				}
				threadList.removeAll(removeThread);
				Thread.sleep(20);
				num++;
				if (num % 100 == 0) {
					log.debug("等待可用线程,当前线程数:" + threadList.size());
				}
			}*/

			NXPmParseThread nxpt = new NXPmParseThread();
			nxpt.setFile(fs[f_i]);

			nxpt.setFilter(Config.isFilter());
			nxpt.setDelTempfile(Config.isDelTempFile());
			execute.submit(nxpt);
			/*Thread t = new Thread(nxpt,"");
			threadList.add(t);
			//t.setPriority(Thread.MAX_PRIORITY);//设置优先
			t.start();*/
			//log.debug("当前线程数:" + threadList.size());
			//log.debug(fs[f_i].getName());
			//log.debug("当前线程数:" + execute.getPoolSize());
			// thread_current_num++;
			fileCount++;
		}

		/** 如果线程数大于0,则表示有线程未结束,确定所有线程结束后再继续 */

		/*while (threadList.size() != 0) {
			log.info("等待所有线程结束,当前线程数:" + threadList.size());
			Thread.sleep(5000);
			List<Thread> removeThread = new ArrayList<Thread>();
			for (int x = 0; x < threadList.size(); x++) {
				Thread t = threadList.get(x);
				if (t.getState().equals(Thread.State.TERMINATED)) {
					removeThread.add(t);
				}
				log.debug(t.getState().toString());
			}
			threadList.removeAll(removeThread);
		}*/
		execute.shutdown();
		
		while (!execute.isTerminated()) {
			log.debug("等待所有线程结束,当前线程数:" + execute.getPoolSize());
			Thread.sleep(5000);
		}
		log.info("解析完成，共解析文件" + fileCount);
	}

	/** 增加线程数 */
	synchronized static public void addThreadNum() {
		thread_current_num++;
	}

	/** 减少线程数* */
	synchronized static public void minusThreadNum() {
		thread_current_num--;
	}

	synchronized public static PmDataObj getPmDataObj(String key) {
		PmDataObj pmdataobj = pmData.get(key);
		if (pmdataobj == null) {
			pmdataobj = new PmDataObj();
			pmData.put(key, pmdataobj);
		}
		//log.debug(pmdataobj.toString());
		return pmdataobj;
	}

	public static void wirteLog() {
		Iterator it = objcfgs.values().iterator();
		while (it.hasNext()) {
			ConfObj o = (ConfObj) it.next();
			if (o.isExistsSub()) {
				log.info(o.getObjectType() + "对象数量:" + o.getNum() + " 子对象数量："
						+ o.getSubConf().getNum());
			} else {
				log.info(o.getObjectType() + "对象数量:" + o.getNum());
			}
		}
		log.info("处理对象总数:" + count);
		log.info("处理文件总数:" + fileCount);
	}
}
