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

	/** ȫ�ֵļ�������,��¼��������ٸ����� */
	static int count = 0;

	/** ȫ�ּ������� ��¼��������ٸ��ļ� */
	static int fileCount = 0;

	/** ��ǰ�߳��� */
	static private int thread_current_num = 0;

	/** ����߳� */
	//static private List<Thread> threadList = new ArrayList<Thread>();

	/**
	 * ���������ļ��еĶ���
	 */
	static Map<String, ConfObj> objcfgs = new ConcurrentHashMap<String, ConfObj>();

	/**
	 * ���泣��
	 */
	static Map<String, String> constant = new ConcurrentHashMap<String, String>();

	/**
	 * �������Ӷ���
	 */
	static Connection con;
	static Map<String, PreparedStatement> statementMap = new ConcurrentHashMap<String, PreparedStatement>();

	protected static final Log log = LogFactory.getLog(NXPmParseMain.class);

	/**
	 * ����������ݶ����������ļ���dn��Ϊkeyֵ
	 */
	static Map<String, PmDataObj> pmData = new HashMap<String, PmDataObj>();

	public static void main(String[] args) {
		if (args.length < 3) {
			log.error("���������������");
			return;
		}
		String factory = args[0];// ����1 ����
		String filepath = args[1];// ����2 �ļ�·��
		String omc = args[2]; // ����2 omc
		int flag;
		if (args.length >= 4) {
			// ����4 �Ƿ��ǲ��� 1�����ɼ� 2����
			flag = Integer.parseInt(args[3].trim());
		} else {
			flag = 1;
		}
		int threadnum = 1;// �߳���
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
		log.info("��ʼ���");
		Iterator<PmDataObj> pdolist = pmData.values().iterator();
		PreparedStatement ps;
		while (pdolist.hasNext()) {

			PmDataObj pmd = pdolist.next();
			ConfObj pmc = pmd.getPmConfObj();
			ps = statementMap.get(pmc.getObjectType());
			List<Field> fs = pmc.getFields();
			for (int i = 0; i < fs.size(); i++) {
				Field f = fs.get(i);
				setPrepared(ps, f, pmd.getValues(), pmd);// ����PreparedStatement����
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

			/** �Ժ��з����ָ�������⴦�� */
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
	 * ����·���µ�xml�ļ������
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

			// ��������ļ�����xml�ļ���ֱ������
			if (!fs[f_i].isFile()
					|| !fs[f_i].getName().toLowerCase().endsWith("xml")) {
				continue;
			}

			// ������Ҫ���˵��ļ�
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
			/** �鿴�߳�״̬���Ƴ��������߳� */
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
					log.debug("�ȴ������߳�,��ǰ�߳���:" + threadList.size());
				}
			}*/

			NXPmParseThread nxpt = new NXPmParseThread();
			nxpt.setFile(fs[f_i]);

			nxpt.setFilter(Config.isFilter());
			nxpt.setDelTempfile(Config.isDelTempFile());
			execute.submit(nxpt);
			/*Thread t = new Thread(nxpt,"");
			threadList.add(t);
			//t.setPriority(Thread.MAX_PRIORITY);//��������
			t.start();*/
			//log.debug("��ǰ�߳���:" + threadList.size());
			//log.debug(fs[f_i].getName());
			//log.debug("��ǰ�߳���:" + execute.getPoolSize());
			// thread_current_num++;
			fileCount++;
		}

		/** ����߳�������0,���ʾ���߳�δ����,ȷ�������߳̽������ټ��� */

		/*while (threadList.size() != 0) {
			log.info("�ȴ������߳̽���,��ǰ�߳���:" + threadList.size());
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
			log.debug("�ȴ������߳̽���,��ǰ�߳���:" + execute.getPoolSize());
			Thread.sleep(5000);
		}
		log.info("������ɣ��������ļ�" + fileCount);
	}

	/** �����߳��� */
	synchronized static public void addThreadNum() {
		thread_current_num++;
	}

	/** �����߳���* */
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
				log.info(o.getObjectType() + "��������:" + o.getNum() + " �Ӷ���������"
						+ o.getSubConf().getNum());
			} else {
				log.info(o.getObjectType() + "��������:" + o.getNum());
			}
		}
		log.info("�����������:" + count);
		log.info("�����ļ�����:" + fileCount);
	}
}
