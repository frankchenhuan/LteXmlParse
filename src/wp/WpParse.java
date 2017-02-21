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

	/** 全局的计数变量,记录共处理多少个对象 */
	private int count = 0;

	/** 全局计数变量 记录共处理多少个文件 */
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
	 * 存放从数据读出的类型
	 */
	private List<String> eutranCellTdd_db_types = new ArrayList<String>();
	private List<String> enbFunction_db_types = new ArrayList<String>();

	/**
	 * 读取数据表头的sql，将wpm_obj_name转换为大写，用来实现不区分大小写的参数名称对应
	 */
	private String titiles_sql = "select upper(t.wpm_obj_name) wpm_obj_name,t.wpm_clm ,t.wpm_dn_sort,t.wpm_factory,t.wpm_net_type,t.wpm_other_objs from wp_dic_table t where t.wpm_factory=? and t.wpm_net_type=? order by t.WPM_ORDER";

	/**
	 * 读取数据库types的sql
	 */
	private String types_sql = "select t.wpm_dn_sort,t.wpm_other_objs from wp_dic_table t where t.wpm_factory=?  and t.wpm_net_type=? group by t.wpm_dn_sort,t.wpm_other_objs ";

	/**
	 * 小区表头map
	 */
	// private Map<String, WpType> eutranCellTdd_db_titles_map = new
	// HashMap<String, WpType>();
	private List<WpTargetName> eutranCellTdd_db_targetName = new ArrayList<WpTargetName>();

	/**
	 * 基站表头map
	 */
	// private Map<String, WpType> enbFunction_db_titles_map = new
	// HashMap<String, WpType>();
	private List<WpTargetName> enbFunction_db_targetName = new ArrayList<WpTargetName>();

	/**
	 * 厂家代码
	 */
	String factroy_id;

	/** omcid */
	String omc_id;

	/** 数据日期 */
	String repdate;

	/** 字符串连接符 */
	private String ps = "__";

	int num = 0;

	/** 字典表名 */
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
	 * 解析并输出文件 参数1：需要解析的文件路径 参数2：是否删除临时文件
	 * 
	 **************************************************************************/
	public void parseAndOut(String path)
			throws XMLStreamException, IOException, SQLException {
		this.initDbTitles();
		this.parsePathAllXml(path);
		/** 合并并输出基站级数据 */
		this.mergeEnbFunction();
		this.outputEnbFunction(path);
		/** 合并并输出小区级数据 */
		this.mergeEutranCellTdd();
		this.outputEutranCellTdd(path);
		log.info("解析总数:" + num);
		/** 删除临时文件 */
		if (Config.isDelTempFile()) {
			this.delTempfile();
		}
	}

	/**
	 * 解析路径下的xml文件并入库
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
			// 如果不是文件或不是xml文件则直接跳过
			if (!curFile.isFile()
					|| (!curFile.getName().toLowerCase().endsWith("xml")
					&& !curFile.getName().toLowerCase().endsWith("gz")
					&& !curFile.getName().toLowerCase().endsWith("zip"))) {
				continue;
			}


			// 过滤需要过滤的文件
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
			 * 判断文件是压缩则解压文件
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
			
			/**由于Zip文件包中可能存在多个文件，所以统一按多个文件处理*/
			for (int i = 0; i < currentFils.size(); i++) {
				curFile=currentFils.get(i);
				if (curFile.getName().toLowerCase().endsWith("xml")) {
					log.debug("开始解析："+curFile.getName());
					if (Config.isFilter()) {
						File newXml = Tools.formatXML(curFile);
						this.parseXml(curFile);
						// 删除临时文件
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
	
	/**解析文件
	 * @throws IOException 
	 * @throws XMLStreamException 
	 * @throws NullPointerException */
	private void parseXml(File curFile) throws IOException, NullPointerException, XMLStreamException
	{
		// 基站级别
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
		 * 小区级别
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
	 * 解析EutranCellTdd级别文件，并输出csv临时文件
	 */
	private void parseEutranCellTddXML(File f,String titleName) throws XMLStreamException, NullPointerException, IOException {
		XMLInputFactory factory = XMLInputFactory.newInstance();
		XMLEventReader reader = factory
				.createXMLEventReader(new InputStreamReader(new FileInputStream(f), Config.getCharSet()));

		String dateTime = null;

		List<String> lines = null;
		CsvOutPrint cop = null;
		List<String> titles = null;
		boolean isContinue = false;// 是否跳过读取和保存数据
		boolean isFirstCmEnd = false;// 表示是否首次cm节点结束
		String userLabel = null;// cm标签中的属性userLabel，设为全局，因为需要将内容作为表头保存，需要在首个cm标签结束时使用
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
					// 将表头转换为大写，实现不区分大小写的参数名匹配
					titles.add(s.trim().toUpperCase());
				} else if (name.equals("FieldValue")) {
				} else if (name.equals("Object")) {
					String rmuid=se.getAttributeByName(new QName("rmUID")).getValue();
					String dn = se.getAttributeByName(new QName("Dn")).getValue();
					userLabel = se.getAttributeByName(new QName("UserLabel")).getValue();
					isContinue = false;// 跳过标识设置为不跳过
					
					//log.debug(dn);
					String celltdd = "";
					String enb = "";
					String me = "";
					String subn = "";
					String dc = "";
					String dns[] = dn.split(",");

					/** 对各个对象的截取 */
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
					// cm标签结束后将标示置为true
					if (!isFirstCmEnd)// 如果fieldvalue结束isFirstCmEnd仍未false，则表示文件中不存在数据，也要写入表头
					{
						cop.write(titles, true);
						isFirstCmEnd = true;
					}
					cop.flush();
					cop.close();
				} else if (name.equals("Object")) {

					/** 20160408增加，需要解析出userLabel中一些值作为参数 */
					if (userLabel != null) {
						String userLabel_vs[] = userLabel.split(":");
						if (userLabel_vs.length > 0) {
							for (int x = 0; x < userLabel_vs.length; x++) {
								String vs[] = userLabel_vs[x].split("-");
								if (vs.length == 2) {
									if (!isFirstCmEnd)// 如果是首次cm标签结束，则需要添加表头
									{
										titles.add(vs[0].toUpperCase());
									}
									lines.add(vs[1]);
								}
							}
						}
					}

					// cm标签结束后将标示置为true
					if (!isFirstCmEnd)// 如果是首次cm标签结束，需要将表头写入文件
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
	 * 解析文件，并输出csv临时文件
	 */
	private void parseEnbFunctionXML(File f,String titleName) throws XMLStreamException, NullPointerException, IOException {
		XMLInputFactory factory = XMLInputFactory.newInstance();
		XMLEventReader reader = factory
				.createXMLEventReader(new InputStreamReader(new FileInputStream(f), Config.getCharSet()));
		String dateTime = null;

		List<String> lines = null;
		CsvOutPrint cop = null;
		List<String> titles = null;
		boolean isFirstCmEnd = false;// 表示是否首次cm节点结束
		String userLabel = null;// cm标签中的属性userLabel，设为全局，因为需要将内容作为表头保存，需要在首个cm标签结束时使用
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
					isFirstCmEnd = false;// 初始化为false
				} else if (name.equals("FieldName")) {
				} else if (name.equals("N")) {
					String s = reader.getElementText();
					// 将表头转换为大写，实现不区分大小写的参数名匹配
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

					/** 对各个对象的截取 */
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
					// cm标签结束后将标示置为true
					if (!isFirstCmEnd)// 如果fieldvalue结束isFirstCmEnd仍未false，则表示文件中不存在数据，也要写入表头
					{
						cop.write(titles, true);
						isFirstCmEnd = true;
					}
					cop.flush();
					cop.close();

				} else if (name.equals("Object")) {
					/** 20160408增加，需要解析出userLabel中一些值作为参数 */
					if (userLabel != null) {
						String userLabel_vs[] = userLabel.split(":");
						if (userLabel_vs.length > 0) {
							for (int x = 0; x < userLabel_vs.length; x++) {
								String vs[] = userLabel_vs[x].split("-");
								if (vs.length == 2) {
									if (!isFirstCmEnd)// 如果是首次cm标签结束，则需要添加表头
									{
										titles.add(vs[0].toUpperCase());
									}
									lines.add(vs[1]);
								}
							}
						}
					}

					// cm标签结束后将标示置为true
					if (!isFirstCmEnd)// 如果是首次cm标签结束，需要将表头写入文件
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
	 * 根据参数类型判断是否配置了区分QCI的采集方式
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
	 * 根据参数类型判断是否配置了多值采集方式
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
	 * 根据参数类型判断是否配置了多值采集方式
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
	 * 对多个文件进行合并 合并结构说明 第一层对象Map dn作为Key存放每个网元对象，value为Map。 第二层：Map
	 * 参数类型作为key，value仍为map，存放参数名和值的键值对 第三层：参数名的键值对
	 * 
	 */
	private void merge(String filename, Map<String, Map> values, String key) throws IOException {
		List<String> titles = null;
		File file = new File(filename);
		String typename = file.getName().substring(0, file.getName().indexOf("_")).toUpperCase();
		log.debug("read file:" + filename+","+typename);
		List<List<String>> data = Tools.readCSVFile(filename);

		/** 存放需要区分QCI的参数 */
		// 是否配置为qci参数分类
		boolean isQci = isQCIType(typename, key);
		// log.debug("isQci="+isQci);
		int qci_index = -1;// qci参数索引，如果是qci参数，则需要找到qci参数的索引

		titles = data.get(0);// 取得表头
		log.debug(data.size());
		for (int x = 1; x < data.size(); x++) {
			List<String> line = data.get(x);
			/* 存放对象 */
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
			// *如果是需要区分qic的参数，则需要取得QCI参数索引，用于取得QCI参数值，用于判断QCI的值*//
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

				/** *对小区级参数的特殊处理 */
				if (key.equals("EutranCellTdd")) {

					/** 如果是诺西厂家,REDRT参数类别* */
					if (this.factroy_id.equals("01")) {

					}

					/**
					 * 如果是华为厂家并且是GERANNFREQGROUPARFCN分类下的
					 * GeranArfcn参数，需要将参数值按小区合并
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
					 * 如果是中兴厂家并且是GERANNFREQGROUPARFCN分类下的
					 * GeranArfcn参数，需要将参数值按小区合并
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

					/** 贝尔厂家 针对geranARFCNList参数的合并,gsm和1800的合并* */
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

				/** 如果是qci类型参数，则需要将不同qci的值放入不同的字段* */
				if (isQci) {
					// 首先取得qci的值
					String qci = line.get(qci_index);
					// 将title加上QCI后缀
					title += "_QCI" + qci.replaceAll("[^\\d]", "");
					log.debug("title_qci=" + title);
				}

				/** 判断是否是多值参数，需要按多值处理 */
				// 得到多值参数的标志参数，即用于区分多值的参数名
				String multiple_ids[] = getMultipleTitle(typename, key);
				if (multiple_ids != null && multiple_ids.length != 0) {
					for (int k = 0; k < multiple_ids.length; k++) {
						String id = null;
						String multiple_id = multiple_ids[k];
						int id_index = -1;
						/** 找到ID所在的位置* */
						for (int z = 0; z < titles.size(); z++) {
							String t = titles.get(z);

							if (t.equalsIgnoreCase(multiple_id)) {
								id_index = z;
								break;
							}
						}

						/** 取得ID的值 */
						if (id_index != -1) {
							id = line.get(id_index);
						}
						/** 将ID的值拼接到title后边 */
						title = title + "_" + id;
					}
					log.debug(typename + "_title=" + title);
				}

				/** 判断是否是需要合并的参数 */
				String merge_ids[] = getMergeTitle(typename, key);
				if (merge_ids != null) {
					String v_ids = "";
					for (int k = 0; k < merge_ids.length; k++) {
						String id = null;
						String merge_id = merge_ids[k];
						int id_index = -1;
						/** 找到ID所在的位置* */
						for (int z = 0; z < titles.size(); z++) {
							String t = titles.get(z);

							if (t.equalsIgnoreCase(merge_id)) {
								id_index = z;
								break;
							}
						}

						/** 取得ID的值 */
						if (id_index != -1) {
							id = line.get(id_index);
						}
						if (v_ids.length() == 0) {
							v_ids += id;
						} else {
							v_ids += ":" + id;
						}
					}
					/** 合并后的值 */
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

				// 将最终的参数名和值作为键值对存入map中
				title = title.toUpperCase();
				vs.put(title, v);
			}
		}

	}

	/**
	 * 合并基站级
	 */
	private void mergeEnbFunction() throws IOException {
		for (int i = 0; i < this.enbFunctionFilenames.size(); i++) {
			String filename = this.enbFunctionFilenames.get(i);
			log.debug(filename);
			this.merge(filename, this.enbFunction, "EnbFunction");
		}
	}

	/** 合并小区级 */
	private void mergeEutranCellTdd() throws IOException {
		for (int i = 0; i < this.eutranCellTddFilenames.size(); i++) {
			String filename = this.eutranCellTddFilenames.get(i);
			log.debug(filename);
			this.merge(filename, this.eutranCellTdd, "EutranCellTdd");
		}
		// log.debug("eutranCellTdd="+this.eutranCellTdd.keySet().size());

	}

	/***************************************************************************
	 * 删除临时文件
	 **************************************************************************/
	private void delTempfile() {
		// 删除文件
		for (int i = 0; i < this.enbFunctionFilenames.size(); i++) {
			new File(this.enbFunctionFilenames.get(i)).delete();
		}

		for (int i = 0; i < this.eutranCellTddFilenames.size(); i++) {
			new File(this.eutranCellTddFilenames.get(i)).delete();
		}
	}

	/**
	 * 替换表头为数据库中的列名
	 * 
	 * @throws SQLException
	 */
	/*
	 * private void replaceTitles() throws SQLException { initDbTitles(); List
	 * newTitles = new ArrayList<String>(); // 基站级别 for (int i = 0; i <
	 * this.enbFunction_titles.size(); i++) { String title =
	 * this.enbFunction_titles.get(i); String dbTitle =
	 * this.enbFunction_db_titles_map.get(title); if (dbTitle != null &&
	 * !dbTitle.equals("")) { newTitles.add(dbTitle); } else {
	 * newTitles.add(title); } } this.enbFunction_titles = newTitles; // 小区级别
	 * newTitles = new ArrayList<String>(); for (int i = 0; i <
	 * this.eutranCellTdd_titles.size(); i++) { String title =
	 * this.eutranCellTdd_titles.get(i); String dbTitle =
	 * this.eutranCellTdd_db_titles_map.get(title); if (dbTitle != null &&
	 * !dbTitle.equals("")) { newTitles.add(dbTitle); } else {
	 * newTitles.add(title); } } this.eutranCellTdd_titles = newTitles; }
	 */

	private void outputEnbFunction(String path) throws IOException, SQLException {
		File f = new File(path);
		// 基站级
		CsvOutPrint cop = new CsvWriter(f.getAbsolutePath() + "/EnbFunction_" + System.currentTimeMillis() + ".csv");

		// 基站级
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

		/** 循环按照数据库列输出表头 */
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

				// 如果参数为空则需要判断是否从其他参数取值
				if (s.equals("") && wptn.getOtherObjs().size() > 0) {
					for (String otherobj : wptn.getOtherObjs()) {
						typename = otherobj.split("-")[0];
						targetname = otherobj.split("-")[1];
						vm = obj.get(typename);
						if (vm != null) {
							s = vm.get(targetname);
						}
						// 如果取得的参数不为空，则退出循环
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
			 * 如果数据中不存在此类型数据，则直接跳过 for (int j = 0; j < wt.getCols().size(); j++)
			 * {// 跳过改类型的所有列 vs.add(""); } continue; }
			 * 
			 * List targets = wt.getTargets(); for (int j = 0; j <
			 * targets.size(); j++) { String s = vm.get(targets.get(j)); if (s
			 * == null) { s = ""; } vs.add(s); } }
			 */
			cop.write(vs, true);
		}

		cop.flush();
		cop.close();
		// 清空数据集
		this.enbFunction.clear();
	}

	private void outputEutranCellTdd(String path) throws IOException, SQLException {
		File f = new File(path);

		// 基站级

		// 小区级
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

		/** 输出表头* */
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

				// 如果参数为空则需要判断是否从其他参数取值
				if (s.equals("") && wptn.getOtherObjs().size() > 0) {
					for (String otherobj : wptn.getOtherObjs()) {
						typename = otherobj.split("-")[0];
						targetname = otherobj.split("-")[1];
						vm = obj.get(typename);
						if (vm != null) {
							s = vm.get(targetname);
						}
						// 如果取得的参数不为空，则退出循环
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
		// 清空数据集
		this.eutranCellTdd.clear();
	}

	/**
	 * 初始化数据库表头 select t.wpm_obj_name,t.wpm_clm
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
		// 基站级
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

		// 小区级
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
	 * 初始化数据库表头 select t.wpm_obj_name,t.wpm_clm
	 * ,t.wpm_dn_sort,t.wpm_factory,t.wpm_net_type from t_lte_wp_dic_factory t
	 * where t.wpm_factory=? and t.wpm_net_type=?
	 * 
	 * @throws SQLException
	 */
	private void initTypes() throws SQLException {
		Connection con;
		con = DataSource.getConnection();
		PreparedStatement ps = con.prepareStatement(this.types_sql);
		// 基站级
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

		// 小区级
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
	 * 根据dn字符串，和关键字，去掉关键字后边的内容，得到新的dn
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
