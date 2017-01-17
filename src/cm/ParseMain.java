package cm;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;

import cfg.ConfObj;
import cfg.Config;
import cfg.DataSource;
import cfg.Field;
import cfg.KeyConstant;
import cfg.Tools;

public class ParseMain {

	/** ȫ�ֵļ�������,��¼���������ٸ����� */
	static int count = 0;

	/** ȫ�ּ������� ��¼���������ٸ��ļ� */
	static int fileCount = 0;

	/**
	 * ���������ļ��еĶ���
	 */
	static Map<String, ConfObj> objcfgs = new HashMap<String, ConfObj>();

	/**
	 * ���泣��
	 */
	static Map<String, String> constant = new HashMap<String, String>();

	/**
	 * �������Ӷ���
	 */
	static Connection con;
	static Map<String, PreparedStatement> statementMap = new HashMap<String, PreparedStatement>();

	protected static final Log log = LogFactory.getLog(ParseMain.class);

	public static void main(String a[]) {

		if (a.length < 3) {
			log.error("���������������");
			return;
		}

		Config.init(1);// ��ʼ�������ļ�
		String factory = a[0];
		String filepath = a[1];
		String omc = a[2];
		String repdate = null;
		if (a.length >= 4) {
			repdate = a[3].trim();
		} else {
			SimpleDateFormat df = new SimpleDateFormat(Config.getDateFormat());
			Calendar cal = Calendar.getInstance();
			cal.add(Calendar.DATE, -1);
			repdate = df.format(cal.getTime());
		}

		int flag = 1;
		if (a.length >= 5) {
			flag = Integer.parseInt(a[4]);
		}

		constant.put(KeyConstant.KEY_FACTORY, factory);
		constant.put(KeyConstant.KEY_DTM_REPDATE, repdate);
		constant.put(KeyConstant.KEY_OMCID, omc);
		constant.put(KeyConstant.KEY_XMPPATH, filepath);

		log.info(KeyConstant.KEY_FACTORY + "=" + factory);
		log.info(KeyConstant.KEY_DTM_REPDATE + "=" + repdate);
		log.info(KeyConstant.KEY_OMCID + "=" + omc);
		log.info(KeyConstant.KEY_XMPPATH + "=" + filepath);
		log.info("Ĭ�ϱ��룺"+Charset.defaultCharset());

		try { // ��ʼ�����ݿ����ӳ�
			DataSource.init(); // ��ʼ���������Ӷ��󼰲�������
			// ��ʼ�����ö���Objcfgs����
			objcfgs = Tools.initObjByXML(omc);
			con = DataSource.getConnection();
			statementMap = Tools.initStatement(con, objcfgs, flag);
		} catch (Exception e) {
			log.error("��ʼ�����ݿ����ӳ���", e);
			return;
		}

		try {
			parsePathAllXml(filepath);
		} catch (Exception e) {
			log.error("�����ļ��쳣��", e);
		}

		/** �ر����ݿ����ӵ���Ϣ */
		Tools.closeCon(con, statementMap);

		/**
		 * �ļ�������ɺ������־
		 */
		wirteLog();

	}

	/**
	 * ����·���µ�xml�ļ������
	 * 
	 * @throws CmParseException
	 * @throws SQLException
	 */
	public static void parsePathAllXml(String filepath) throws JDOMException,
			IOException, SQLException {
		File f = new File(filepath);
		File fs[] = f.listFiles();
		for (int f_i = 0; f_i < fs.length; f_i++) {

			// ��������ļ�����xml�ļ���ֱ������
			if (!fs[f_i].isFile()
					|| !fs[f_i].getName().toLowerCase().endsWith("xml")) {
				continue;
			}
			
			//������Ҫ���˵��ļ�
			boolean isContinue = false;
			String[] filter_files = Config.getFilter_files();
			String name=fs[f_i].getName();
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
			
			if (Config.isFilter()) {
				File newXml = Tools.formatXML(fs[f_i]);
				parseXML(newXml);
				
				//ɾ����ʱ�ļ�
				if (Config.isDelTempFile()) {
					newXml.delete();
				}
			} else {
				parseXML(fs[f_i]);
			}
			log.debug(fs[f_i].getName());
			fileCount++;
		}
		/**
		 * ����������ʣ�ಿ��ִ�����
		 */
		Tools.execute(con, statementMap);
	}

	public static void parseXML(File f) throws SQLException, JDOMException,
			IOException {
		SAXBuilder builder = new SAXBuilder(); // ����һ��SAX������
		Document doc;

		/** �������ڽ����Ķ��� */
		ConfObj cfgxmlobj;

		/**
		 * ����������ƺ�������ϵ��map
		 */
		Map<String, String> field_index_map = new HashMap<String, String>();
		/** ���valueֵ��map */
		Map<String, String> value_map = new HashMap<String, String>();

		doc = builder.build(f);

		Element root = doc.getRootElement(); // ��ȡ��Ԫ��
		String filename = f.getName();

		/**
		 * ��ȡ�ļ�ͷʱ�䳣���������볣��map
		 */
		Element e_header = root.getChild("FileHeader");
		String dateTime = e_header.getChild("DateTime").getValue();
		dateTime = (dateTime != null ? dateTime.replaceAll("T", " ").substring(
				0, 19) : "");
		constant.put(KeyConstant.KEY_DATETIME, dateTime);

		/**
		 * ѭ��Objects
		 */
		List<Element> e_objects = root.getChildren("Objects");
		for (int i = 0; i < e_objects.size(); i++) {
			cfgxmlobj = null;
			Element el_Object = e_objects.get(i);
			cfgxmlobj = objcfgs.get(el_Object.getChildText("ObjectType")
					.toUpperCase());

			/** ���δ�ҵ�������֤������Ҫ���� */
			if (cfgxmlobj == null) {
				continue;
			}

			// ������ȡ���������
			List<Element> e_fieldname = el_Object.getChild("FieldName")
					.getChildren("N");

			/**
			 * �����������ƺ�������ϵ
			 */
			field_index_map.clear();// ʹ��ǰ�����
			for (int x = 0; x < e_fieldname.size(); x++) {
				field_index_map.put(e_fieldname.get(x).getAttributeValue("i"),
						e_fieldname.get(x).getValue());
			}

			// ������ȡ�����ֵ
			List<Element> e_fvs = el_Object.getChild("FieldValue").getChildren(
					"Cm");
			// �����ѭ�����ڱ������������

			for (int x = 0; x < e_fvs.size(); x++) {
				value_map.clear();// ��ֵǰ�����

				// �����г����������map
				value_map.putAll(constant);
				Element e_cm = e_fvs.get(x);
				String dn = e_cm.getAttributeValue("Dn");
				String userLabel = e_cm.getAttributeValue("UserLabel");
				value_map.put(KeyConstant.KEY_OBJDN, dn);
				value_map.put(KeyConstant.KEY_OBJUSERLABEL, userLabel);
				String dns[] = dn.split(",");
				for (int y = 0; y < dns.length; y++) {
					String ds[] = dns[y].split("=");
					if (ds.length > 1) {
						value_map.put("DN-" + ds[0].toUpperCase(), ds[1]);
					}
				}
				List<Element> e_fvalue = e_cm.getChildren("V");
				for (int y = 0; y < e_fvalue.size(); y++) {
					Element v = e_fvalue.get(y);

					/** ��������Ϊkey ֵ��Ϊvalue��� */
					value_map.put(field_index_map.get(v.getAttributeValue("i"))
							.toUpperCase(), v.getValue());

				}
				value_map.put(KeyConstant.KEY_FILENAME, filename);
				/** ������õ����ݶ���������ݻ����� */
				addBatch(cfgxmlobj, value_map);

				/** �������+1 */
				count++;
				/**
				 * �����������õ��ύ������ִ���ύ
				 */
				if (count / Config.getCommitCount() >= 1
						&& count % Config.getCommitCount() == 0) {
					Tools.execute(con, statementMap);
				}

				/** �������+1 */
				cfgxmlobj.addNum();
			}
		}
	}

	public static void addBatch(ConfObj cfgxmlobj, Map<String, String> value_map)
			throws SQLException {
		PreparedStatement pst = null;

		pst = statementMap.get(cfgxmlobj.getObjectType());

		/** ѭ�������ݿ⸳ֵ */
		for (int z = 0; z < cfgxmlobj.getFields().size(); z++) {
			Field field = cfgxmlobj.getFields().get(z);
			String s = null;
			String filename = value_map.get(KeyConstant.KEY_FILENAME);
			s = value_map.get(field.getName().toUpperCase());
			Tools.setPrepared(pst, field, s, cfgxmlobj.getObjectType(),
					filename);
		}

		/** �����úõ�sql���뻺���� */
		pst.addBatch();
	}

	/**
	 * ��¼��־
	 */
	public static void wirteLog() {
		Iterator it = objcfgs.values().iterator();
		while (it.hasNext()) {
			ConfObj o = (ConfObj) it.next();
			log.info(o.getObjectType() + "��������:" + o.getNum());
		}
		log.info("������������:" + count);
		log.info("�����ļ�����:" + fileCount);
	}
}