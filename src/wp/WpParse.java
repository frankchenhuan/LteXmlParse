package wp;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import java.util.List;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cfg.Config;
import cfg.CsvOutPrint;
import cfg.CsvWriter;
import cfg.DataSource;

import cfg.Tools;
import cm.CmParseException;

public class WpParse {

	protected final Log log = LogFactory.getLog(this.getClass());

	/** ȫ�ֵļ�������,��¼��������ٸ����� */
	private int count = 0;

	/** ȫ�ּ������� ��¼��������ٸ��ļ� */
	private int fileCount = 0;

	private List<String> eutranCellTddFilenames = new ArrayList<String>();

	private List<String> enbFunctionFilenames = new ArrayList<String>();

	// private List<String> eutranCellTdd_titles = new ArrayList<String>();
	private Map<String, Map> eutranCellTdd = new HashMap<String, Map>();
	private Map<String, Map> eutranCellTdd_key = new HashMap<String, Map>();

	private List<String> enbFunction_titles = new ArrayList<String>();
	private Map<String, Map> enbFunction = new HashMap<String, Map>();
	private Map<String, Map> enbFunction_key = new HashMap<String, Map>();

	/**
	 * ��Ŵ����ݶ���������
	 */
	private List<String> eutranCellTdd_db_types = new ArrayList<String>();
	private List<String> enbFunction_db_types = new ArrayList<String>();

	/**
	 * ��ȡ���ݱ�ͷ��sql����wpm_obj_nameת��Ϊ��д������ʵ�ֲ����ִ�Сд�Ĳ������ƶ�Ӧ
	 */
	private String titiles_sql = "select upper(t.wpm_obj_name) wpm_obj_name,t.wpm_clm ,t.wpm_dn_sort,t.wpm_factory,t.wpm_net_type,t.wpm_other_objs from wp_dic_table t where t.wpm_factory=? and t.wpm_net_type=? order by t.WPM_ORDER";

	/**
	 * ��ȡ���ݿ�types��sql
	 */
	private String types_sql = "select t.wpm_dn_sort,t.wpm_other_objs from wp_dic_table t where t.wpm_factory=?  and t.wpm_net_type=? group by t.wpm_dn_sort,t.wpm_other_objs ";

	/**
	 * С����ͷmap
	 */
	// private Map<String, WpType> eutranCellTdd_db_titles_map = new
	// HashMap<String, WpType>();
	private List<WpTargetName> eutranCellTdd_db_targetName = new ArrayList<WpTargetName>();

	/**
	 * ��վ��ͷmap
	 */
	// private Map<String, WpType> enbFunction_db_titles_map = new
	// HashMap<String, WpType>();
	private List<WpTargetName> enbFunction_db_targetName = new ArrayList<WpTargetName>();

	/**
	 * ���Ҵ���
	 */
	String factroy_id;

	/** omcid */
	String omc_id;

	/** �������� */
	String repdate;

	/** �ַ������ӷ� */
	private String ps = "__";

	int num = 0;

	/** �ֵ���� */
	private String wp_dic_table = null;

	public WpParse(String factory, String omc, String repdate) {
		this.factroy_id = factory;
		this.omc_id = omc;
		this.repdate = repdate;
		this.wp_dic_table = Config.getValue("wp_dic_table");
		if (this.wp_dic_table == null || this.wp_dic_table.trim().equals("")) {
			this.wp_dic_table = "t_lte_wp_dic_factory";
		}
		this.titiles_sql = this.titiles_sql.replaceAll("wp_dic_table", this.wp_dic_table);
		this.types_sql = this.types_sql.replaceAll("wp_dic_table", this.wp_dic_table);
	}

	/***************************************************************************
	 * ����������ļ� ����1����Ҫ�������ļ�·�� ����2���Ƿ�ɾ����ʱ�ļ�
	 * 
	 **************************************************************************/
	public void parseAndOut(String path)
			throws XMLStreamException, IOException, SQLException {
		this.initDbTitles();
		this.parsePathAllXml(path);
		/** �ϲ��������վ������ */
		this.mergeEnbFunction();
		this.outputEnbFunction(path);
		/** �ϲ������С�������� */
		this.mergeEutranCellTdd();
		this.outputEutranCellTdd(path);
		log.info("��������:" + num);
		/** ɾ����ʱ�ļ� */
		if (Config.isDelTempFile()) {
			this.delTempfile();
		}
	}

	/**
	 * ����·���µ�xml�ļ������
	 * 
	 * @throws XMLStreamException
	 * @throws SQLException
	 * 
	 */
	private void parsePathAllXml(String filepath) throws XMLStreamException, IOException {
		File f = new File(filepath);
		File fs[] = f.listFiles();
		boolean isUnGzip = false;
		List<File> currentFils = new ArrayList<File>();
		for (int f_i = 0; f_i < fs.length; f_i++) {
			File curFile = fs[f_i];
			// ��������ļ�����xml�ļ���ֱ������
			if (!curFile.isFile()
					|| (!curFile.getName().toLowerCase().endsWith("xml")
					&& !curFile.getName().toLowerCase().endsWith("gz")
					&& !curFile.getName().toLowerCase().endsWith("zip"))) {
				continue;
			}


			// ������Ҫ���˵��ļ�
			boolean isContinue = false;
			String[] filter_files = Config.getFilter_files();
			String name = curFile.getName();
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
			
			/**
			 * �ж��ļ���ѹ�����ѹ�ļ�
			 */
			currentFils.clear();
			boolean autoUnzip=Config.isAutoUnzip();
			if (autoUnzip
					&& curFile.getName().toLowerCase().endsWith("gz")) {
				curFile = Tools.unGZip(curFile);
				isUnGzip = true;
				currentFils.add(curFile);
			} else if (autoUnzip
					&& curFile.getName().toLowerCase().endsWith("zip")) {
				currentFils = Tools.unZip(curFile);
				isUnGzip = true;
			} else {
				isUnGzip = false;
				currentFils.add(curFile);
			}
			
			/**����Zip�ļ����п��ܴ��ڶ���ļ�������ͳһ������ļ�����*/
			for (int i = 0; i < currentFils.size(); i++) {
				curFile=currentFils.get(i);
				if (curFile.getName().toLowerCase().endsWith("xml")) {
					log.debug("��ʼ������"+curFile.getName());
					if (Config.isFilter()) {
						File newXml = Tools.formatXML(curFile);
						this.parseXml(curFile);
						// ɾ����ʱ�ļ�
						if (Config.isDelTempFile()) {
							newXml.delete();
						}
					} else {
						this.parseXml(curFile);
					}
					fileCount++;
				} 
				if (isUnGzip) {
					curFile.delete();
				}
				
			}

		}
	}
	
	/**�����ļ�
	 * @throws IOException 
	 * @throws XMLStreamException 
	 * @throws NullPointerException */
	private void parseXml(File curFile) throws IOException, NullPointerException, XMLStreamException
	{
		// ��վ����
		for (int x = 0; x < this.enbFunction_db_types.size(); x++) {
			String filename = curFile.getName().toUpperCase();
			String objectType=this.enbFunction_db_types.get(x).toUpperCase();
			String regex = ".+[^a-zA-Z]" + objectType + "[^a-zA-Z].+";
			
			if (filename.matches(regex)) {
				log.debug(filename);
				this.parseEnbFunctionXML(curFile,objectType);
				return;
			}
		}

		/**
		 * С������
		 */
		for (int x = 0; x < this.eutranCellTdd_db_types.size(); x++) {
			String filename = curFile.getName().toUpperCase();
			String objectType=this.eutranCellTdd_db_types.get(x).toUpperCase();
			String regex = ".+[^a-zA-Z]" + objectType + "[^a-zA-Z].+";
			log.debug(objectType);
			if (filename.matches(regex)) {
				log.debug(filename);
				this.parseEutranCellTddXML(curFile,objectType);
				return;
			}
		}
	}

	/**
	 * ����EutranCellTdd�����ļ��������csv��ʱ�ļ�
	 */
	private void parseEutranCellTddXML(File f,String titleName) throws XMLStreamException, NullPointerException, IOException {
		XMLInputFactory factory = XMLInputFactory.newInstance();
		XMLEventReader reader = factory
				.createXMLEventReader(new InputStreamReader(new FileInputStream(f), Config.getCharSet()));

		String dateTime = null;

		List<String> lines = null;
		CsvOutPrint cop = null;
		List<String> titles = null;
		boolean isContinue = false;// �Ƿ�������ȡ�ͱ�������
		boolean isFirstCmEnd = false;// ��ʾ�Ƿ��״�cm�ڵ����
		String userLabel = null;// cm��ǩ�е�����userLabel����Ϊȫ�֣���Ϊ��Ҫ��������Ϊ��ͷ���棬��Ҫ���׸�cm��ǩ����ʱʹ��
		//titlename = reader.getElementText();
		String filename = f.getParentFile().getAbsolutePath() + "/" + titleName + "_" + f.getName()
				+ ".csv";
		this.eutranCellTddFilenames.add(filename);
		cop = new CsvWriter(filename);
		titles = new ArrayList<String>();

		
		titles.add(KeyConstant.DN);
		titles.add(KeyConstant.RMUID);
		
		while (reader.hasNext()) {
			
			XMLEvent xe = reader.nextEvent();
			if (xe.isStartElement()) {
				StartElement se = xe.asStartElement();
				String name = se.getName().getLocalPart();
				
				if (name.equals("ObjectType")) {
				
				} else if (name.equals("FieldName")) {
				} else if (name.equals("N")) {
					String s = reader.getElementText();
					// ����ͷת��Ϊ��д��ʵ�ֲ����ִ�Сд�Ĳ�����ƥ��
					titles.add(s.trim().toUpperCase());
				} else if (name.equals("FieldValue")) {
				} else if (name.equals("Object")) {
					String rmuid=se.getAttributeByName(new QName("rmUID")).getValue();
					String dn = se.getAttributeByName(new QName("Dn")).getValue();
					userLabel = se.getAttributeByName(new QName("UserLabel")).getValue();
					isContinue = false;// ������ʶ����Ϊ������
					
					//log.debug(dn);
					String celltdd = "";
					String enb = "";
					String me = "";
					String subn = "";
					String dc = "";
					String dns[] = dn.split(",");

					/** �Ը�������Ľ�ȡ */
					for (int i = 0; i < dns.length; i++) {
						String s = dns[i];
						String ds[] = s.split("=");
						if (ds.length <= 1) {
							continue;
						}
						if (ds[0].equalsIgnoreCase("DC")) {
							dc = ds[1];
						} else if (ds[0].equalsIgnoreCase("SubNetwork")) {
							subn = ds[1];
						} else if (ds[0].equalsIgnoreCase("ManagedElement")) {
							me = (!me.equals("") ? me + ";" + ds[1] : ds[1]);
						} else if (ds[0].equalsIgnoreCase("EnbFunction")) {
							enb = ds[1];
						} else if (ds[0].equalsIgnoreCase("EutranCellTdd")) {
							celltdd = ds[1];
						}
					}

					lines = new ArrayList<String>();
					
					lines.add(dn);
					lines.add(rmuid);
					String keyDn = this.getNewDn("EutranCellTdd", dn);
					Map<String, String> key = this.eutranCellTdd_key.get(keyDn);
					if (key == null) {
						key = new HashMap<String, String>();
						key.put(KeyConstant.DN, keyDn);
						key.put(KeyConstant.DN_USERLABEL, userLabel);
						key.put(KeyConstant.HEAR_DATETIME, dateTime);
						key.put(KeyConstant.DN_DC, dc);
						key.put(KeyConstant.DN_SUBNETWORK, subn);
						key.put(KeyConstant.DN_MANAGEDELEMENT, me);
						key.put(KeyConstant.DN_ENBFUNCTION, enb);
						key.put(KeyConstant.DN_EUTRANCELLTDD, celltdd);
						this.eutranCellTdd_key.put(keyDn, key);
					}

				} else if (name.equals("V")) {
					String s = reader.getElementText();
					lines.add(s);

				} else if (name.equals("TimeStamp")) {
					String s = reader.getElementText();
					dateTime = (s != null ? s.replaceAll("T", " ").substring(0, 19) : "");
				}
			} else if (xe.isEndElement()) {
				EndElement se = xe.asEndElement();
				String name = se.getName().getLocalPart();
				if (name.equals("DataFile")) {

				} else if (name.equals("ObjectType")) {
				} else if (name.equals("FieldName")) {
					// cop.write(titles, true);
				} else if (name.equals("FieldValue")) {
					// cm��ǩ�����󽫱�ʾ��Ϊtrue
					if (!isFirstCmEnd)// ���fieldvalue����isFirstCmEnd��δfalse�����ʾ�ļ��в��������ݣ�ҲҪд���ͷ
					{
						cop.write(titles, true);
						isFirstCmEnd = true;
					}
					cop.flush();
					cop.close();
				} else if (name.equals("Object")) {

					/** 20160408���ӣ���Ҫ������userLabel��һЩֵ��Ϊ���� */
					if (userLabel != null) {
						String userLabel_vs[] = userLabel.split(":");
						if (userLabel_vs.length > 0) {
							for (int x = 0; x < userLabel_vs.length; x++) {
								String vs[] = userLabel_vs[x].split("-");
								if (vs.length == 2) {
									if (!isFirstCmEnd)// ������״�cm��ǩ����������Ҫ��ӱ�ͷ
									{
										titles.add(vs[0].toUpperCase());
									}
									lines.add(vs[1]);
								}
							}
						}
					}

					// cm��ǩ�����󽫱�ʾ��Ϊtrue
					if (!isFirstCmEnd)// ������״�cm��ǩ��������Ҫ����ͷд���ļ�
					{
						cop.write(titles, true);
						isFirstCmEnd = true;
					}
					if (!isContinue) {
						cop.write(lines, true);
						num++;
					}
				}
			}
		}

	}

	/**
	 * �����ļ��������csv��ʱ�ļ�
	 */
	private void parseEnbFunctionXML(File f,String titleName) throws XMLStreamException, NullPointerException, IOException {
		XMLInputFactory factory = XMLInputFactory.newInstance();
		XMLEventReader reader = factory
				.createXMLEventReader(new InputStreamReader(new FileInputStream(f), Config.getCharSet()));
		String dateTime = null;

		List<String> lines = null;
		CsvOutPrint cop = null;
		List<String> titles = null;
		boolean isFirstCmEnd = false;// ��ʾ�Ƿ��״�cm�ڵ����
		String userLabel = null;// cm��ǩ�е�����userLabel����Ϊȫ�֣���Ϊ��Ҫ��������Ϊ��ͷ���棬��Ҫ���׸�cm��ǩ����ʱʹ��
		//titlename = reader.getElementText();
		String filename = f.getParentFile().getAbsolutePath() + "/" + titleName + "_" + f.getName()
				+ ".csv";
		this.enbFunctionFilenames.add(filename);
		cop = new CsvWriter(filename);
		titles = new ArrayList<String>();
		titles.add(KeyConstant.DN);
		titles.add(KeyConstant.RMUID);
		
		while (reader.hasNext()) {
			XMLEvent xe = reader.nextEvent();
			if (xe.isStartElement()) {
				StartElement se = xe.asStartElement();
				String name = se.getName().getLocalPart();
				if (name.equals("ObjectType")) {
					isFirstCmEnd = false;// ��ʼ��Ϊfalse
				} else if (name.equals("FieldName")) {
				} else if (name.equals("N")) {
					String s = reader.getElementText();
					// ����ͷת��Ϊ��д��ʵ�ֲ����ִ�Сд�Ĳ�����ƥ��
					titles.add(s.trim().toUpperCase());
				} else if (name.equals("FieldValue")) {
				} else if (name.equals("Object")) {
					String rmuid=se.getAttributeByName(new QName("rmUID")).getValue();
					String dn = se.getAttributeByName(new QName("Dn")).getValue();
					userLabel = se.getAttributeByName(new QName("UserLabel")).getValue();

					String enb = "";
					String me = "";
					String subn = "";
					String dc = "";
					String dns[] = dn.split(",");

					/** �Ը�������Ľ�ȡ */
					for (int i = 0; i < dns.length; i++) {
						String s = dns[i];
						String ds[] = s.split("=");
						if (ds.length <= 1) {
							continue;
						}
						if (ds[0].equalsIgnoreCase("DC")) {
							dc = ds[1];
						} else if (ds[0].equalsIgnoreCase("SubNetwork")) {
							subn = ds[1];
						} else if (ds[0].equalsIgnoreCase("ManagedElement")) {
							me = (!me.equals("") ? me + ";" + ds[1] : ds[1]);
						} else if (ds[0].equalsIgnoreCase("EnbFunction")) {
							enb = ds[1];
						}
					}

					lines = new ArrayList<String>();
					lines.add(dn);
					lines.add(rmuid);
					String keyDn = this.getNewDn("EnbFunction", dn);
					Map<String, String> key = this.enbFunction_key.get(keyDn);
					if (key == null) {
						key = new HashMap<String, String>();
						key.put(KeyConstant.DN, keyDn);
						key.put(KeyConstant.DN_USERLABEL, userLabel);
						key.put(KeyConstant.HEAR_DATETIME, dateTime);
						key.put(KeyConstant.DN_DC, dc);
						key.put(KeyConstant.DN_SUBNETWORK, subn);
						key.put(KeyConstant.DN_MANAGEDELEMENT, me);
						key.put(KeyConstant.DN_ENBFUNCTION, enb);
						this.enbFunction_key.put(keyDn, key);
					}

				} else if (name.equals("V")) {
					String s = reader.getElementText();
					lines.add(s == null ? "" : s);
				} else if (name.equals("TimeStamp")) {
					String s = reader.getElementText();
					dateTime = (s != null ? s.replaceAll("T", " ").substring(0, 19) : "");
				}
			} else if (xe.isEndElement()) {
				EndElement se = xe.asEndElement();
				String name = se.getName().getLocalPart();
				if (name.equals("ObjectType")) {

				} else if (name.equals("FieldName")) {
					// cop.write(titles, true);
				} else if (name.equals("FieldValue")) {
					// cm��ǩ�����󽫱�ʾ��Ϊtrue
					if (!isFirstCmEnd)// ���fieldvalue����isFirstCmEnd��δfalse�����ʾ�ļ��в��������ݣ�ҲҪд���ͷ
					{
						cop.write(titles, true);
						isFirstCmEnd = true;
					}
					cop.flush();
					cop.close();

				} else if (name.equals("Object")) {
					/** 20160408���ӣ���Ҫ������userLabel��һЩֵ��Ϊ���� */
					if (userLabel != null) {
						String userLabel_vs[] = userLabel.split(":");
						if (userLabel_vs.length > 0) {
							for (int x = 0; x < userLabel_vs.length; x++) {
								String vs[] = userLabel_vs[x].split("-");
								if (vs.length == 2) {
									if (!isFirstCmEnd)// ������״�cm��ǩ����������Ҫ��ӱ�ͷ
									{
										titles.add(vs[0].toUpperCase());
									}
									lines.add(vs[1]);
								}
							}
						}
					}

					// cm��ǩ�����󽫱�ʾ��Ϊtrue
					if (!isFirstCmEnd)// ������״�cm��ǩ��������Ҫ����ͷд���ļ�
					{
						cop.write(titles, true);
						isFirstCmEnd = true;
					}
					cop.write(lines, true);
				}
			}
		}

	}

	/***************************************************************************
	 * ���ݲ��������ж��Ƿ�����������QCI�Ĳɼ���ʽ
	 * 
	 **************************************************************************/
	private boolean isQCIType(String type, String key) {
		String Qci_types = Config.getValue("QCI_" + key + "_" + this.factroy_id);
		// log.debug(Qci_types);
		if (Qci_types == null) {
			return false;
		} else {
			String s[] = Qci_types.split(",");
			for (int i = 0; i < s.length; i++) {
				if (s[i].equalsIgnoreCase(type)) {
					return true;
				}
			}
		}
		return false;
	}

	/***************************************************************************
	 * ���ݲ��������ж��Ƿ������˶�ֵ�ɼ���ʽ
	 * 
	 **************************************************************************/
	private String[] getMultipleTitle(String type, String key) {
		String types = Config.getValue("multiple_" + key + "_" + this.factroy_id);
		// log.debug("Multiple=" + types);
		if (types == null) {
			return null;
		} else {
			
			String s[] = types.split(",");
			for (int i = 0; i < s.length; i++) {
				String s1 = s[i].split("-")[0];
				String s2[] = s[i].split("-")[1].split(":");
				if (s1.equalsIgnoreCase(type)) {
					return s2;
				}
			}
		}
		return null;
	}

	/***************************************************************************
	 * ���ݲ��������ж��Ƿ������˶�ֵ�ɼ���ʽ
	 * 
	 **************************************************************************/
	private String[] getMergeTitle(String type, String key) {
		String types = Config.getValue("merge_" + key + "_" + this.factroy_id);
		// log.debug("Multiple=" + types);
		if (types == null) {
			return null;
		} else {
			String s[] = types.split(",");
			for (int i = 0; i < s.length; i++) {
				String[] sp = s[i].split("-");
				if (sp.length == 2) {
					String s1 = sp[0];

					String s2[] = sp[1].split(":");
					if (s1.equals(type)) {
						return s2;
					}
				} else {
					return new String[0];
				}
			}
		}
		return null;
	}

	/**
	 * �Զ���ļ����кϲ� �ϲ��ṹ˵�� ��һ�����Map dn��ΪKey���ÿ����Ԫ����valueΪMap�� �ڶ��㣺Map
	 * ����������Ϊkey��value��Ϊmap����Ų�������ֵ�ļ�ֵ�� �����㣺�������ļ�ֵ��
	 * 
	 */
	private void merge(String filename, Map<String, Map> values, String key) throws IOException {
		List<String> titles = null;
		File file = new File(filename);
		String typename = file.getName().substring(0, file.getName().indexOf("_")).toUpperCase();
		log.debug("read file:" + filename+","+typename);
		List<List<String>> data = Tools.readCSVFile(filename);

		/** �����Ҫ����QCI�Ĳ��� */
		// �Ƿ�����Ϊqci��������
		boolean isQci = isQCIType(typename, key);
		// log.debug("isQci="+isQci);
		int qci_index = -1;// qci���������������qci����������Ҫ�ҵ�qci����������

		titles = data.get(0);// ȡ�ñ�ͷ
		log.debug(data.size());
		for (int x = 1; x < data.size(); x++) {
			List<String> line = data.get(x);
			/* ��Ŷ��� */
			Map<String, Map> obj = null;
			String dn = line.get(0);
			String newdn = this.getNewDn(key, dn);
			obj = values.get(newdn);
			if (obj == null) {
				obj = new HashMap<String, Map>();
				values.put(newdn, obj);
			}
			Map<String, String> vs = obj.get(typename);
			if (vs == null) {
				vs = new HashMap<String, String>();
				obj.put(typename, vs);
			}
			// *�������Ҫ����qic�Ĳ���������Ҫȡ��QCI��������������ȡ��QCI����ֵ�������ж�QCI��ֵ*//
			if (isQci) {
				for (int y = 0; y < titles.size(); y++) {
					String title = titles.get(y);
					if (title.equalsIgnoreCase("QCI")) {
						qci_index = y;
						break;
					}
				}
			}
			for (int y = 0; y < titles.size(); y++) {
				String v = null;
				String title = titles.get(y);

				if (y < line.size()) {
					v = line.get(y);
				} else {
					v = "";
				}

				/** *��С�������������⴦�� */
				if (key.equals("EutranCellTdd")) {

					/** �����ŵ������,REDRT�������* */
					if (this.factroy_id.equals("01")) {

					}

					/**
					 * ����ǻ�Ϊ���Ҳ�����GERANNFREQGROUPARFCN�����µ�
					 * GeranArfcn��������Ҫ������ֵ��С���ϲ�
					 */
					if (this.factroy_id.equals("02")) {
						if (typename.equalsIgnoreCase("GERANNFREQGROUPARFCN") && title.equalsIgnoreCase("GeranArfcn")) {
							String v_GeranArfcn = vs.get(title);
							if (v_GeranArfcn != null && !v_GeranArfcn.equals("")) {
								v = v.equals("") ? v_GeranArfcn : v_GeranArfcn + "," + v;
							}
							log.debug(v);
						}
					}

					/**
					 * ��������˳��Ҳ�����GERANNFREQGROUPARFCN�����µ�
					 * GeranArfcn��������Ҫ������ֵ��С���ϲ�
					 */
					if (this.factroy_id.equals("03")) {
						if (typename.equalsIgnoreCase("EUtranCellMeasurementTDD")
								&& title.startsWith("GERANMEASPARAS_EXPLICITARFCN")) {
							title = "GERANMEASPARAS_EXPLICITARFCN";
							String ARFCN = vs.get(title);

							if (ARFCN != null && !ARFCN.equals("")) {
								v = (v.equals("") || v.equals("65535")) ? ARFCN : ARFCN + "," + v;
							}
							log.debug(v);
						}
					}

					/** �������� ���geranARFCNList�����ĺϲ�,gsm��1800�ĺϲ�* */
					if (this.factroy_id.equals("10")) {
						if (typename.equalsIgnoreCase("GeranNeighboringFreqsConf")
								&& title.equalsIgnoreCase("geranARFCNList")) {
							String ARFCN = vs.get(title);
							v = v.replaceAll("\\{", "");
							v = v.replaceAll("\\}", "");
							if (ARFCN != null && !ARFCN.equals("")) {
								v = v.equals("") ? ARFCN : ARFCN + "," + v;
							}
							log.debug(v);
						}
					}
				}

				else if (key.equals("EnbFunction")) {

				}

				/** �����qci���Ͳ���������Ҫ����ͬqci��ֵ���벻ͬ���ֶ�* */
				if (isQci) {
					// ����ȡ��qci��ֵ
					String qci = line.get(qci_index);
					// ��title����QCI��׺
					title += "_QCI" + qci.replaceAll("[^\\d]", "");
					log.debug("title_qci=" + title);
				}

				/** �ж��Ƿ��Ƕ�ֵ��������Ҫ����ֵ���� */
				// �õ���ֵ�����ı�־���������������ֶ�ֵ�Ĳ�����
				String multiple_ids[] = getMultipleTitle(typename, key);
				if (multiple_ids != null && multiple_ids.length != 0) {
					for (int k = 0; k < multiple_ids.length; k++) {
						String id = null;
						String multiple_id = multiple_ids[k];
						int id_index = -1;
						/** �ҵ�ID���ڵ�λ��* */
						for (int z = 0; z < titles.size(); z++) {
							String t = titles.get(z);

							if (t.equalsIgnoreCase(multiple_id)) {
								id_index = z;
								break;
							}
						}

						/** ȡ��ID��ֵ */
						if (id_index != -1) {
							id = line.get(id_index);
						}
						/** ��ID��ֵƴ�ӵ�title��� */
						title = title + "_" + id;
					}
					log.debug(typename + "_title=" + title);
				}

				/** �ж��Ƿ�����Ҫ�ϲ��Ĳ��� */
				String merge_ids[] = getMergeTitle(typename, key);
				if (merge_ids != null) {
					String v_ids = "";
					for (int k = 0; k < merge_ids.length; k++) {
						String id = null;
						String merge_id = merge_ids[k];
						int id_index = -1;
						/** �ҵ�ID���ڵ�λ��* */
						for (int z = 0; z < titles.size(); z++) {
							String t = titles.get(z);

							if (t.equalsIgnoreCase(merge_id)) {
								id_index = z;
								break;
							}
						}

						/** ȡ��ID��ֵ */
						if (id_index != -1) {
							id = line.get(id_index);
						}
						if (v_ids.length() == 0) {
							v_ids += id;
						} else {
							v_ids += ":" + id;
						}
					}
					/** �ϲ����ֵ */
					String merge_v = vs.get(title);
					if (v_ids.length() == 0) {
						v_ids = v;
					} else {
						v_ids = "(" + v_ids + "," + v + ")";
					}

					if (merge_v == null) {
						v = "{" + v_ids + "}";
					} else {
						merge_v = merge_v.substring(0, merge_v.length() - 1);
						v = merge_v + "," + v_ids + "}";
					}

					log.debug(typename + "_v=" + v);
				}

				// �����յĲ�������ֵ��Ϊ��ֵ�Դ���map��
				title = title.toUpperCase();
				vs.put(title, v);
			}
		}

	}

	/**
	 * �ϲ���վ��
	 */
	private void mergeEnbFunction() throws IOException {
		for (int i = 0; i < this.enbFunctionFilenames.size(); i++) {
			String filename = this.enbFunctionFilenames.get(i);
			log.debug(filename);
			this.merge(filename, this.enbFunction, "EnbFunction");
		}
	}

	/** �ϲ�С���� */
	private void mergeEutranCellTdd() throws IOException {
		for (int i = 0; i < this.eutranCellTddFilenames.size(); i++) {
			String filename = this.eutranCellTddFilenames.get(i);
			log.debug(filename);
			this.merge(filename, this.eutranCellTdd, "EutranCellTdd");
		}
		// log.debug("eutranCellTdd="+this.eutranCellTdd.keySet().size());

	}

	/***************************************************************************
	 * ɾ����ʱ�ļ�
	 **************************************************************************/
	private void delTempfile() {
		// ɾ���ļ�
		for (int i = 0; i < this.enbFunctionFilenames.size(); i++) {
			new File(this.enbFunctionFilenames.get(i)).delete();
		}

		for (int i = 0; i < this.eutranCellTddFilenames.size(); i++) {
			new File(this.eutranCellTddFilenames.get(i)).delete();
		}
	}

	/**
	 * �滻��ͷΪ���ݿ��е�����
	 * 
	 * @throws SQLException
	 */
	/*
	 * private void replaceTitles() throws SQLException { initDbTitles(); List
	 * newTitles = new ArrayList<String>(); // ��վ���� for (int i = 0; i <
	 * this.enbFunction_titles.size(); i++) { String title =
	 * this.enbFunction_titles.get(i); String dbTitle =
	 * this.enbFunction_db_titles_map.get(title); if (dbTitle != null &&
	 * !dbTitle.equals("")) { newTitles.add(dbTitle); } else {
	 * newTitles.add(title); } } this.enbFunction_titles = newTitles; // С������
	 * newTitles = new ArrayList<String>(); for (int i = 0; i <
	 * this.eutranCellTdd_titles.size(); i++) { String title =
	 * this.eutranCellTdd_titles.get(i); String dbTitle =
	 * this.eutranCellTdd_db_titles_map.get(title); if (dbTitle != null &&
	 * !dbTitle.equals("")) { newTitles.add(dbTitle); } else {
	 * newTitles.add(title); } } this.eutranCellTdd_titles = newTitles; }
	 */

	private void outputEnbFunction(String path) throws IOException, SQLException {
		File f = new File(path);
		// ��վ��
		CsvOutPrint cop = new CsvWriter(f.getAbsolutePath() + "/EnbFunction_" + System.currentTimeMillis() + ".csv");

		// ��վ��
		List<String> titles = new ArrayList<String>();
		titles.add(KeyConstant.DN);
		titles.add(KeyConstant.DN_DC);
		titles.add(KeyConstant.DN_SUBNETWORK);
		titles.add(KeyConstant.DN_MANAGEDELEMENT);
		titles.add(KeyConstant.DN_ENBFUNCTION);
		// titles.add(KeyConstant.DN_EUTRANCELLTDD);
		titles.add(KeyConstant.DN_USERLABEL);
		titles.add(KeyConstant.HEAR_DATETIME);
		titles.add(KeyConstant.FACTORY_ID);
		titles.add(KeyConstant.OMC_ID);
		titles.add(KeyConstant.DTM_REPDATE);

		/** ѭ���������ݿ��������ͷ */
		/*
		 * for (int i = 0; i < this.enbFunction_db_types.size(); i++) { String
		 * typename = this.enbFunction_db_types.get(i); WpType wt =
		 * this.enbFunction_db_titles_map.get(typename); List<String> cols =
		 * wt.getCols(); for (int j = 0; j < cols.size(); j++) {
		 * titles.add(cols.get(j)); } }
		 */

		for (int i = 0; i < this.enbFunction_db_targetName.size(); i++) {
			titles.add(this.enbFunction_db_targetName.get(i).getClmName());
		}

		cop.write(titles, true);

		Iterator<String> it = this.enbFunction_key.keySet().iterator();
		while (it.hasNext()) {
			String obj_dn = it.next();
			Map<String, String> obj_key = this.enbFunction_key.get(obj_dn);
			Map<String, Map> obj = this.enbFunction.get(obj_dn);
			if (obj == null) {
				continue;
			}
			List<String> vs = new ArrayList<String>();
			vs.add(obj_key.get(KeyConstant.DN));
			vs.add(obj_key.get(KeyConstant.DN_DC));
			vs.add(obj_key.get(KeyConstant.DN_SUBNETWORK));
			vs.add(obj_key.get(KeyConstant.DN_MANAGEDELEMENT));
			vs.add(obj_key.get(KeyConstant.DN_ENBFUNCTION));
			vs.add(obj_key.get(KeyConstant.DN_USERLABEL));
			vs.add(obj_key.get(KeyConstant.HEAR_DATETIME));
			vs.add(this.factroy_id);
			vs.add(this.omc_id);
			vs.add(this.repdate);

			for (int i = 0; i < this.enbFunction_db_targetName.size(); i++) {
				WpTargetName wptn = this.enbFunction_db_targetName.get(i);
				String typename = wptn.getTypeName();
				String targetname = wptn.getTargetName();
				Map<String, String> vm = obj.get(typename);
				String s = null;
				if (vm != null) {
					s = vm.get(targetname);
				}
				s = (s == null ? "" : s);

				// �������Ϊ������Ҫ�ж��Ƿ����������ȡֵ
				if (s.equals("") && wptn.getOtherObjs().size() > 0) {
					for (String otherobj : wptn.getOtherObjs()) {
						typename = otherobj.split("-")[0];
						targetname = otherobj.split("-")[1];
						vm = obj.get(typename);
						if (vm != null) {
							s = vm.get(targetname);
						}
						// ���ȡ�õĲ�����Ϊ�գ����˳�ѭ��
						if (s != null && !s.trim().equals("")) {
							break;
						}
					}
				}
				s = (s == null ? "" : s);
				vs.add(s);
			}

			/*
			 * for (int i = 0; i < this.enbFunction_db_types.size(); i++) {
			 * String typename = this.enbFunction_db_types.get(i); Map<String,
			 * String> vm = obj.get(typename); WpType wt =
			 * this.enbFunction_db_titles_map.get(typename); if (vm == null) {//
			 * ��������в����ڴ��������ݣ���ֱ������ for (int j = 0; j < wt.getCols().size(); j++)
			 * {// ���������͵������� vs.add(""); } continue; }
			 * 
			 * List targets = wt.getTargets(); for (int j = 0; j <
			 * targets.size(); j++) { String s = vm.get(targets.get(j)); if (s
			 * == null) { s = ""; } vs.add(s); } }
			 */
			cop.write(vs, true);
		}

		cop.flush();
		cop.close();
		// ������ݼ�
		this.enbFunction.clear();
	}

	private void outputEutranCellTdd(String path) throws IOException, SQLException {
		File f = new File(path);

		// ��վ��

		// С����
		CsvOutPrint cop = new CsvWriter(f.getAbsolutePath() + "/EutranCellTdd_" + System.currentTimeMillis() + ".csv");

		List<String> titles = new ArrayList<String>();
		titles.add(KeyConstant.DN);
		titles.add(KeyConstant.DN_DC);
		titles.add(KeyConstant.DN_SUBNETWORK);
		titles.add(KeyConstant.DN_MANAGEDELEMENT);
		titles.add(KeyConstant.DN_ENBFUNCTION);
		titles.add(KeyConstant.DN_EUTRANCELLTDD);
		titles.add(KeyConstant.DN_USERLABEL);
		titles.add(KeyConstant.HEAR_DATETIME);
		titles.add(KeyConstant.FACTORY_ID);
		titles.add(KeyConstant.OMC_ID);
		titles.add(KeyConstant.DTM_REPDATE);

		/** �����ͷ* */
		/*
		 * for (int i = 0; i < this.eutranCellTdd_db_titles_map.size(); i++) {
		 * String typename = this.eutranCellTdd_db_types.get(i); WpType wt =
		 * this.eutranCellTdd_db_titles_map.get(typename); List<String> cols =
		 * wt.getCols(); for (int j = 0; j < cols.size(); j++) {
		 * titles.add(cols.get(j)); } }
		 */

		for (int i = 0; i < this.eutranCellTdd_db_targetName.size(); i++) {
			titles.add(this.eutranCellTdd_db_targetName.get(i).getClmName());
		}

		cop.write(titles, true);

		Iterator<String> it = this.eutranCellTdd_key.keySet().iterator();
		while (it.hasNext()) {
			String obj_dn = it.next();
			log.debug(obj_dn);
			Map<String, String> obj_key = this.eutranCellTdd_key.get(obj_dn);
			Map<String, Map> obj = this.eutranCellTdd.get(obj_dn);
			if (obj == null) {
				log.debug(null);
				continue;
			}
			// System.out.println(obj_dn);
			List<String> vs = new ArrayList<String>();
			vs.add(obj_key.get(KeyConstant.DN));
			vs.add(obj_key.get(KeyConstant.DN_DC));
			vs.add(obj_key.get(KeyConstant.DN_SUBNETWORK));
			vs.add(obj_key.get(KeyConstant.DN_MANAGEDELEMENT));
			vs.add(obj_key.get(KeyConstant.DN_ENBFUNCTION));
			vs.add(obj_key.get(KeyConstant.DN_EUTRANCELLTDD));
			vs.add(obj_key.get(KeyConstant.DN_USERLABEL));
			vs.add(obj_key.get(KeyConstant.HEAR_DATETIME));
			vs.add(this.factroy_id);
			vs.add(this.omc_id);
			vs.add(this.repdate);

			for (int i = 0; i < this.eutranCellTdd_db_targetName.size(); i++) {
				WpTargetName wptn = this.eutranCellTdd_db_targetName.get(i);
				String typename = wptn.getTypeName().toUpperCase();
				String targetname = wptn.getTargetName();
				Map<String, String> vm = obj.get(typename);

				String s = null;
				if (vm != null) {
					s = vm.get(targetname);
				}
				s = (s == null ? "" : s);

				// �������Ϊ������Ҫ�ж��Ƿ����������ȡֵ
				if (s.equals("") && wptn.getOtherObjs().size() > 0) {
					for (String otherobj : wptn.getOtherObjs()) {
						typename = otherobj.split("-")[0];
						targetname = otherobj.split("-")[1];
						vm = obj.get(typename);
						if (vm != null) {
							s = vm.get(targetname);
						}
						// ���ȡ�õĲ�����Ϊ�գ����˳�ѭ��
						if (s != null && !s.trim().equals("")) {
							log.debug("other obj:" + typename + "  " + targetname + " " + s);
							break;
						}
					}
				}
				s = (s == null ? "" : s);
				vs.add(s);
			}
			cop.write(vs, true);
		}
		cop.flush();
		cop.close();
		// ������ݼ�
		this.eutranCellTdd.clear();
	}

	/**
	 * ��ʼ�����ݿ��ͷ select t.wpm_obj_name,t.wpm_clm
	 * ,t.wpm_dn_sort,t.wpm_factory,t.wpm_net_type from t_lte_wp_dic_factory t
	 * where t.wpm_factory=? and t.wpm_net_type=?
	 * 
	 * @throws SQLException
	 */
	private void initDbTitles() throws SQLException {
		this.initTypes();
		Connection con;
		con = DataSource.getConnection();
		PreparedStatement ps = con.prepareStatement(titiles_sql);
		// ��վ��
		ps.setString(1, this.factroy_id);
		ps.setInt(2, 1);
		ResultSet rs = ps.executeQuery();
		while (rs.next()) {
			String wpm_obj_name = rs.getString("wpm_obj_name");
			String wpm_clm = rs.getString("wpm_clm");
			String wpm_dn_sort = rs.getString("wpm_dn_sort");
			/*
			 * WpType wptype = this.enbFunction_db_titles_map.get(wpm_dn_sort);
			 * if (wptype == null) { wptype = new WpType();
			 * wptype.setTypeName(wpm_dn_sort);
			 * this.enbFunction_db_titles_map.put(wpm_dn_sort, wptype); }
			 * wptype.add(wpm_obj_name, wpm_clm);
			 */
			// this.enbFunction_db_types.add(wpm_dn_sort);
			WpTargetName wtn = new WpTargetName(wpm_obj_name, wpm_dn_sort, wpm_clm);
			this.enbFunction_db_targetName.add(wtn);
			String wpm_other_objs = rs.getString("wpm_other_objs");
			if (wpm_other_objs != null) {
				String otherobjs[] = wpm_other_objs.split(",");
				for (String obj : otherobjs) {
					wtn.addOtherObj(obj.toUpperCase());
				}
			}
		}
		rs.close();

		// С����
		ps.setString(1, this.factroy_id);
		ps.setInt(2, 2);
		rs = ps.executeQuery();
		while (rs.next()) {
			String wpm_obj_name = rs.getString("wpm_obj_name");
			String wpm_clm = rs.getString("wpm_clm");
			String wpm_dn_sort = rs.getString("wpm_dn_sort");
			/*
			 * WpType wptype =
			 * this.eutranCellTdd_db_titles_map.get(wpm_dn_sort); if (wptype ==
			 * null) { wptype = new WpType();
			 * this.eutranCellTdd_db_titles_map.put(wpm_dn_sort, wptype); }
			 * wptype.add(wpm_obj_name, wpm_clm);
			 */
			WpTargetName wtn = new WpTargetName(wpm_obj_name, wpm_dn_sort, wpm_clm);
			this.eutranCellTdd_db_targetName.add(wtn);
			String wpm_other_objs = rs.getString("wpm_other_objs");
			if (wpm_other_objs != null) {
				String otherobjs[] = wpm_other_objs.split(",");
				for (String obj : otherobjs) {
					wtn.addOtherObj(obj.toUpperCase());
				}
			}
		}
		rs.close();
		ps.close();
		con.close();
	}

	/**
	 * ��ʼ�����ݿ��ͷ select t.wpm_obj_name,t.wpm_clm
	 * ,t.wpm_dn_sort,t.wpm_factory,t.wpm_net_type from t_lte_wp_dic_factory t
	 * where t.wpm_factory=? and t.wpm_net_type=?
	 * 
	 * @throws SQLException
	 */
	private void initTypes() throws SQLException {
		Connection con;
		con = DataSource.getConnection();
		PreparedStatement ps = con.prepareStatement(this.types_sql);
		// ��վ��
		ps.setString(1, this.factroy_id);
		ps.setInt(2, 1);
		ResultSet rs = ps.executeQuery();
		while (rs.next()) {
			String wpm_dn_sort = rs.getString("wpm_dn_sort");
			this.enbFunction_db_types.add(wpm_dn_sort);
			String wpm_other_objs = rs.getString("wpm_other_objs");
			if (wpm_other_objs != null && !wpm_other_objs.trim().equals("")) {
				String otherobjs[] = wpm_other_objs.split(",");
				for (String obj : otherobjs) {
					this.enbFunction_db_types.add(obj.split("-")[0]);
				}
			}
		}
		rs.close();

		// С����
		ps.setString(1, this.factroy_id);
		ps.setInt(2, 2);
		rs = ps.executeQuery();
		while (rs.next()) {
			String wpm_dn_sort = rs.getString("wpm_dn_sort");
			this.eutranCellTdd_db_types.add(wpm_dn_sort);
			String wpm_other_objs = rs.getString("wpm_other_objs");
			if (wpm_other_objs != null && !wpm_other_objs.trim().equals("")) {
				String otherobjs[] = wpm_other_objs.split(",");
				for (String obj : otherobjs) {
					this.eutranCellTdd_db_types.add(obj.split("-")[0]);
				}
			}
		}
		rs.close();
		ps.close();
		con.close();
	}

	/**
	 * 
	 * ����dn�ַ������͹ؼ��֣�ȥ���ؼ��ֺ�ߵ����ݣ��õ��µ�dn
	 */
	public String getNewDn(String key, String redn) {
		int index = redn.indexOf(key);
		String newdn = null;
		if (index != -1) {
			index = redn.indexOf(",", index);
			if (index != -1) {
				newdn = redn.substring(0, index);
			} else {
				newdn = redn;
			}
		} else {
			newdn = redn;
		}
		return newdn;
	}

}
