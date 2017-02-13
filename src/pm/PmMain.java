package pm;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


import cfg.ConfObj;
import cfg.Config;
import cfg.DataSource;
import cfg.KeyConstant;
import cfg.Tools;

public class PmMain {
	protected static final Log log = LogFactory.getLog(PmMain.class);

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
		
		PmParse parse = new PmParseCsv();
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
			parse.writeLog();
			parse.close();
		} catch (Exception e) {
			log.error("", e);
		}
	}

	

	public void test() {
		/*PmParseThread pmt = new PmParseThread();
		pmt.start();*/
	}

}
