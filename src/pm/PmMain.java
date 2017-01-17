package pm;

import java.io.File;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import sun.awt.windows.ThemeReader;

import cfg.ConfObj;
import cfg.Config;
import cfg.DataSource;
import cfg.KeyConstant;
import cfg.Tools;

public class PmMain {
	protected static final Log log = LogFactory.getLog(PmMain.class);

	/** ȫ�ּ������� ��¼��������ٸ��ļ� */
	private static int fileCount = 0;

	/**
	 * @param args
	 */
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

		PmParse parse = new PmParse();
		parse.putConstant(KeyConstant.KEY_FACTORY, factory);
		parse.putConstant(KeyConstant.KEY_OMCID, omc);
		parse.putConstant(KeyConstant.KEY_XMPPATH, filepath);

		try {
			Config.init(2);
			DataSource.init();
			String conftype = Config.getConfType();
			Map<String, ConfObj> confs = null;
			if (conftype.equalsIgnoreCase("db")) {
				confs = Tools.initObjByDB(omc);
			} else if (conftype.equalsIgnoreCase("xml")) {
				confs = Tools.initObjByXML(omc);
			}
			parse.setObjcfgs(confs);
			parse.setCon(DataSource.getConnection());
			parse.setStatementMap(Tools.initStatement(parse.getCon(), parse
					.getObjcfgs(), flag));
			parse.parsePathAllXml(filepath);
			parse.insertObjData();
			parse.wirteLog();
			parse.close();

			/*
			 * PmParseThread pmts[] = new PmParseThread[2]; for (int i = 0; i <
			 * pmts.length; i++) { pmts[i] = new PmParseThread();
			 * pmts[i].setObjcfgs(confs);
			 * pmts[i].setCon(DataSource.getConnection());
			 * pmts[i].setStatementMap(Tools.initStatement(pmts[i].getCon(),
			 * pmts[i].getObjcfgs(), flag));
			 * pmts[i].putConstant(KeyConstant.KEY_FACTORY, factory);
			 * pmts[i].putConstant(KeyConstant.KEY_OMCID, omc);
			 * pmts[i].putConstant(KeyConstant.KEY_XMPPATH, filepath); }
			 */
			// parsePathAllXml(filepath, pmts);
		} catch (Exception e) {
			log.error("", e);
		}
	}

	/*
	 * public static void parsePathAllXml(String filepath, PmParseThread pmts[]) {
	 * File f = new File(filepath); File fs[] = f.listFiles(); for (int f_i = 0;
	 * f_i < fs.length; f_i++) {
	 *  // ��������ļ�����xml�ļ���ֱ������ if (!fs[f_i].isFile() ||
	 * !fs[f_i].getName().toLowerCase().endsWith("xml")) { continue; }
	 *  // ������Ҫ���˵��ļ� boolean isContinue = false; String[] filter_files =
	 * Config.getFilter_files(); String name = fs[f_i].getName(); if
	 * (filter_files != null) {
	 * 
	 * for (int i = 0; i < filter_files.length; i++) { isContinue = false; if
	 * (name.indexOf(filter_files[i]) != -1) { isContinue = true; break; } } }
	 * if (isContinue) { continue; }
	 * 
	 * log.debug(fs[f_i].getName()); while (true) { for (int x = 0; x <
	 * pmts.length; x++) { if (pmts[x].getState() == Thread.State.TERMINATED) {
	 * pmts[x] = (PmParseThread) pmts[x].clone(); pmts[x].setFile(fs[f_i]);
	 * pmts[x].start(); isContinue = true; break; } else if (pmts[x].getState() ==
	 * Thread.State.NEW) { pmts[x].setFile(fs[f_i]); pmts[x].start(); isContinue =
	 * true; break; } } if (isContinue) { break; } else { try {
	 * Thread.sleep(1000); } catch (InterruptedException e) { // TODO
	 * Auto-generated catch block e.printStackTrace(); } } } fileCount++; } }
	 */

	public void test() {
		/*PmParseThread pmt = new PmParseThread();
		pmt.start();*/
	}

}
