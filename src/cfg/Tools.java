package cfg;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;

import pm.PmSubConfObj;

public class Tools {

	protected static final Log log = LogFactory.getLog(Tools.class);

	/**
	 * 设置sql参数
	 */
	public static void setPrepared(PreparedStatement ps, Field field,
			String value, String objname, String filename) throws SQLException {
		String s = value;
		int index = field.getIndex();
		String type = field.getType();
		int length = field.getLength();
		String regex = field.getRegex();
		if (regex != null) {
			s = (s != null ? s.replaceAll(regex, "") : null);
		}
		/** 判断入库类型 */
		if (type.equalsIgnoreCase("string")) {
			if (s == null || s.equals("")) {
				ps.setNull(index, java.sql.Types.VARCHAR);
			} else {
				if (length > 0 && s.length() > length) {
					log.warn("文件：" + filename + " 对象：" + objname + " 属性："
							+ field.getName() + "值：" + s + " 长度：" + s.length()
							+ " 超过规定长度" + length + " 已被截断");
					s = s.substring(0, length);
				}
				ps.setString(index, s);
			}
		} else if (type.equalsIgnoreCase("int")) {
			if (s == null || s.equals("")) {
				ps.setNull(index, java.sql.Types.INTEGER);
			} else {
				try {
					ps.setInt(index, Integer.parseInt(s));
				} catch (NumberFormatException e) {
					log.warn("文件：" + filename + " 对象：" + objname + "   字段："
							+ field.getName() + " 值：" + s, e);
					ps.setNull(index, java.sql.Types.INTEGER);
				}
			}
		} else if (type.equalsIgnoreCase("number")) {
			if (s == null || s.equals("")) {
				ps.setNull(index, java.sql.Types.NUMERIC);
			} else {
				try {
					// System.out.println(index);
					ps.setDouble(index, Float.parseFloat(s));
				} catch (NumberFormatException e) {
					log.warn("文件：" + filename + " 对象：" + objname + "   字段："
							+ field.getName() + " 值：" + s, e);
					ps.setNull(index, java.sql.Types.NUMERIC);
				}
			}
		} else if (type.equalsIgnoreCase("date")) {
			if (s == null || s.equals("")) {
				ps.setNull(index, java.sql.Types.DATE);
			} else {
				SimpleDateFormat df = new SimpleDateFormat(field.getFormat());
				try {
					ps.setDate(index, new Date(df.parse(s).getTime()));
				} catch (ParseException e) {
					log.warn("文件：" + filename + " 对象：" + objname + "   字段："
							+ field.getName() + " 值：" + s, e);
					ps.setNull(index, java.sql.Types.DATE);
				}
			}
		} else if (type.equalsIgnoreCase("float")) {
			if (s == null || s.equals("")) {
				ps.setNull(index, java.sql.Types.NUMERIC);
			} else {
				try {
					ps.setDouble(index, Float.parseFloat(s));
				} catch (NumberFormatException e) {
					log.warn("文件：" + filename + " 对象：" + objname + "   字段："
							+ field.getName() + " 值：" + s, e);
					ps.setNull(index, java.sql.Types.NUMERIC);
				}
			}
		} else if (type.equalsIgnoreCase("datetime")) {
			if (s == null || s.equals("")) {
				ps.setNull(index, java.sql.Types.DATE);
			} else {
				SimpleDateFormat df = new SimpleDateFormat(field.getFormat());
				try {
					ps
							.setTimestamp(index, new Timestamp(df.parse(s)
									.getTime()));
				} catch (ParseException e) {
					log.warn("文件：" + filename + " 对象：" + objname + "   字段："
							+ field.getName() + " 值：" + s, e);
					ps.setNull(index, java.sql.Types.DATE);
				}
			}
		}
	}

	/**
	 * 替换xml文件中不规范的字符，并重新生成xml文件
	 */
	public static File formatXML(File xml) throws IOException {
		StringBuffer sb = new StringBuffer();
		char[] fbyte = new char[1024 * 1024];
		FileInputStream fins = new FileInputStream(xml);
		InputStreamReader fr = new InputStreamReader(fins, Config.getCharSet());
		int i = 0;
		while ((i = fr.read(fbyte)) != -1) {
			sb.append(new String(fbyte, 0, i));
			fbyte = new char[1024 * 1024];
		}
		fr.close();
		int andIndex = 0;
		int fenIndex = 0;
		andIndex = sb.indexOf("&");
		while (andIndex != -1) {
			fenIndex = sb.indexOf(";", andIndex);
			if (fenIndex == -1 || fenIndex - andIndex > 5) {
				sb = sb.replace(andIndex, andIndex + 1, "&amp;");
			}
			andIndex = sb.indexOf("&", andIndex + 4);
		}

		String newfilepath = xml.getAbsolutePath() + "_tmp";
		FileOutputStream fout = new FileOutputStream(newfilepath);
		// 过滤非法Unicode字符
		String outStr = sb.toString().trim().replaceAll(
				"[\\x00-\\x08\\x0b-\\x0c\\x0e-\\x1f]", "");
		int xmlstart = outStr.indexOf("<?xml");
		outStr = outStr.substring(xmlstart);
		fout.write(outStr.getBytes(Config.getCharSet()));
		fout.close();
		return new File(newfilepath);
	}

	/***************************************************************************
	 * 初始化配置对象
	 **************************************************************************/
	public static Map<String, ConfObj> initObjByXML(String objconfigname) {
		String path = Config.getValue(objconfigname);
		SAXBuilder builder = new SAXBuilder(); // 创建一个SAX构造器
		Document doc;
		Map<String, ConfObj> objcfgs = new HashMap<String, ConfObj>();
		log.info("初始化对象配置开始");
		try {
			log.info(path);
			// doc = builder.build(ParseMain.class.getClassLoader()
			// .getResourceAsStream(path));
			doc = builder.build(new FileInputStream(System
					.getProperty("user.dir")
					+ "/" + path));
			Element root = doc.getRootElement(); // 获取根元素
			List<Element> li = root.getChildren("Object");
			for (int i = 0; i < li.size(); i++) {
				Element el = li.get(i);
				ConfObj obj = new ConfObj();
				obj.setObjectType(el.getChildText("ObjectType"));
				obj.setTablename(el.getChildText("tablename"));
				obj.setTemp_tablename(el.getChildText("temp_tablename"));
				List fields = new ArrayList<Field>();
				Element fieldName = el.getChild("FieldName");
				List ln = fieldName.getChildren("N");
				for (int j = 0; j < ln.size(); j++) {
					Element en = (Element) ln.get(j);
					Field f = new Field();
					String s = en.getAttributeValue("flag");
					if (s == null || s.trim().equals("")) {
						f.setFlag(0);
					} else {
						f.setFlag(Integer.parseInt(s.trim()));
					}
					f.setIndex(j + 1);
					f.setName(en.getValue());
					s = en.getAttributeValue("format");
					f.setFormat(s != null ? s : Config.getDateFormat());
					f.setType(en.getAttributeValue("type"));
					f.setTablecolumn(en.getAttributeValue("tablecolumn"));
					f.setRegex(en.getAttributeValue("regex"));
					s = en.getAttributeValue("length");
					if (s == null || s.trim().equals("")) {
						f.setLength(0);
					} else {
						f.setLength(Integer.parseInt(s.trim()));
					}
					// 设置忽略大小写，默认为true，忽略
					s = en.getAttributeValue("IgnoreCase");
					if (s == null || s.trim().equals("")) {
						f.setIgnoreCase(true);
					} else if (s.equals("N")) {
						f.setIgnoreCase(false);
					} else if (s.equals("Y")) {
						f.setIgnoreCase(true);
					}

					fields.add(f);
				}
				obj.setFileds(fields);

				/** SubConfObj */
				Element subTargets = el.getChild("subTargets");
				if (subTargets != null) {

					List<Element> targets = subTargets.getChildren("target");
					for (int x = 0; x < targets.size(); x++) {
						obj.addSubTarget(targets.get(x).getValue()
								.toUpperCase(), targets.get(x).getValue()
								.toUpperCase());
					}

					Element e_sub = subTargets.getChild("subTable");
					PmSubConfObj subconf = new PmSubConfObj();
					// N属性
					List<Field> subfields = new ArrayList<Field>();
					List<Element> subN = e_sub.getChildren("N");
					for (int j = 0; j < subN.size(); j++) {
						Element en = subN.get(j);
						Field f = new Field();
						String s = en.getAttributeValue("flag");
						if (s == null || s.trim().equals("")) {
							f.setFlag(0);
						} else {
							f.setFlag(Integer.parseInt(s.trim()));
						}
						f.setIndex(j + 1);
						f.setName(en.getValue());
						s = en.getAttributeValue("format");
						f.setFormat(s != null ? s : Config.getDateFormat());
						f.setType(en.getAttributeValue("type"));
						f.setTablecolumn(en.getAttributeValue("tablecolumn"));
						f.setRegex(en.getAttributeValue("regex"));
						s = en.getAttributeValue("length");
						if (s == null || s.trim().equals("")) {
							f.setLength(0);
						} else {
							f.setLength(Integer.parseInt(s.trim()));
						}
						subfields.add(f);
					}
					subconf.setFields(subfields);

					/** targetNameField */
					subconf.setTablename(e_sub.getChildText("tablename"));
					subconf.setTemp_tablename(e_sub
							.getChildText("temp_tablename"));

					Element e_target = e_sub.getChild("targetNameField");
					Field f_target = new Field();
					f_target.setFlag(0);
					f_target.setIndex(1);
					f_target.setType(e_target.getAttributeValue("type"));
					f_target.setTablecolumn(e_target
							.getAttributeValue("tablecolumn"));
					String s = e_target.getAttributeValue("length");
					if (s == null || s.trim().equals("")) {
						f_target.setLength(0);
					} else {
						f_target.setLength(Integer.parseInt(s.trim()));
					}
					s = e_target.getAttributeValue("format");
					f_target.setFormat(s != null ? s : Config.getDateFormat());
					f_target.setType(e_target.getAttributeValue("type"));
					f_target.setRegex(e_target.getAttributeValue("regex"));
					subconf.setTargetNameField(f_target);

					/** subTargetNameField */
					e_target = e_sub.getChild("subTargetNameField");
					f_target = new Field();
					f_target.setFlag(0);
					f_target.setIndex(2);
					f_target.setType(e_target.getAttributeValue("type"));
					f_target.setTablecolumn(e_target
							.getAttributeValue("tablecolumn"));
					s = e_target.getAttributeValue("length");
					if (s == null || s.trim().equals("")) {
						f_target.setLength(0);
					} else {
						f_target.setLength(Integer.parseInt(s.trim()));
					}
					s = e_target.getAttributeValue("format");
					f_target.setFormat(s != null ? s : Config.getDateFormat());
					f_target.setType(e_target.getAttributeValue("type"));
					f_target.setRegex(e_target.getAttributeValue("regex"));
					subconf.setSubTargetNameField(f_target);

					/** targetValueField */
					e_target = e_sub.getChild("targetValueField");
					f_target = new Field();
					f_target.setFlag(0);
					f_target.setIndex(3);
					f_target.setType(e_target.getAttributeValue("type"));
					f_target.setTablecolumn(e_target
							.getAttributeValue("tablecolumn"));
					s = e_target.getAttributeValue("length");
					if (s == null || s.trim().equals("")) {
						f_target.setLength(0);
					} else {
						f_target.setLength(Integer.parseInt(s.trim()));
					}
					s = e_target.getAttributeValue("format");
					f_target.setFormat(s != null ? s : Config.getDateFormat());
					f_target.setType(e_target.getAttributeValue("type"));
					f_target.setRegex(e_target.getAttributeValue("regex"));
					subconf.setTargetValueField(f_target);

					// 根据具体列情况重置索引
					subconf.initIndex();

					// /
					obj.setSubConf(subconf);
				}

				objcfgs.put(obj.getObjectType().toUpperCase(), obj);
			}
			log.info("初始化对象配置完成，共初始化对象配置:" + objcfgs.size());
		} catch (JDOMException e) {
			e.printStackTrace();
			log.error("JDOM解析对象XML出错", e);
		} catch (IOException e) {
			e.printStackTrace();
			log.error("读取对象XML文件出错", e);
		}

		return objcfgs;
	}

	/***************************************************************************
	 * 初始化配置对象
	 * 
	 * @throws SQLException
	 * @throws NumberFormatException
	 **************************************************************************/
	public static Map<String, ConfObj> initObjByDB(String objconfigname)
			throws NumberFormatException, SQLException {
		String confname = Config.getValue(objconfigname);
		Map<String, ConfObj> objcfgs = new HashMap<String, ConfObj>();
		log.info("初始化对象配置开始");
		ConfObjDbLoad codl = new ConfObjDbLoad();
		String s = null;
		List<Map> objects = codl.getObjects(confname);
		for (int i = 0; i < objects.size(); i++) {
			Map<String, String> om = objects.get(i);
			ConfObj obj = new ConfObj();
			obj.setObjectType(om.get("OBJECT_TYPE"));
			obj.setTablename(om.get("TABLENAME"));
			obj.setTemp_tablename(om.get("TEMP_TABLENAME"));
			List fields = new ArrayList<Field>();
			List<Map> fieldMaps = codl
					.getFields(Integer.parseInt(om.get("ID")));

			for (int j = 0; j < fieldMaps.size(); j++) {
				Map<String, String> fieldvalue = fieldMaps.get(j);
				Field f = new Field();
				f.setName(fieldvalue.get("NAME"));
				f.setIndex(j + 1);
				s = fieldvalue.get("FORMAT");
				f.setFormat(s != null ? s : Config.getDateFormat());
				f.setType(fieldvalue.get("DATATYPE"));
				f.setTablecolumn(fieldvalue.get("TABLECOLUMN"));
				s = fieldvalue.get("LENGTH");
				if (s == null || s.trim().equals("")) {
					f.setLength(0);
				} else {
					f.setLength(Integer.parseInt(s.trim()));
				}
				s = fieldvalue.get("REGEX");
				f.setRegex(s);
				fields.add(f);
			}
			obj.setFileds(fields);

			/** SubConfObj */
			s = om.get("SUB_TABLENAME");
			if (s != null && !s.trim().equals("")) {
				PmSubConfObj subconf = new PmSubConfObj();

				subconf.setTablename(om.get("SUB_TABLENAME"));
				subconf.setTemp_tablename(om.get("TEMP_SUB_TABLENAME"));

				Field f_target = new Field();
				f_target.setFlag(0);
				f_target.setIndex(1);
				f_target.setTablecolumn(om.get("TARGETNAMEFIELD_TABLECOLUMN"));
				s = om.get("TARGETNAMEFIELD_LENGTH");
				if (s == null || s.trim().equals("")) {
					f_target.setLength(0);
				} else {
					f_target.setLength(Integer.parseInt(s.trim()));
				}
				s = om.get("TARGETNAMEFIELD_FORMAT");
				f_target.setFormat(s != null ? s : Config.getDateFormat());
				f_target.setType(om.get("TARGETNAMEFIELD_DATATYPE"));
				f_target.setRegex(om.get("TARGETNAMEFIELD_REGEX"));
				subconf.setTargetNameField(f_target);

				/** subTargetNameField */
				f_target = new Field();
				f_target.setFlag(0);
				f_target.setIndex(2);
				f_target.setTablecolumn(om
						.get("SUBTARGETNAMEFIELD_TABLECOLUMN"));
				s = om.get("SUBTARGETNAMEFIELD_LENGTH");
				if (s == null || s.trim().equals("")) {
					f_target.setLength(0);
				} else {
					f_target.setLength(Integer.parseInt(s.trim()));
				}
				s = om.get("SUBTARGETNAMEFIELD_FORMAT");
				f_target.setFormat(s != null ? s : Config.getDateFormat());
				f_target.setType(om.get("SUBTARGETNAMEFIELD_DATATYPE"));
				f_target.setRegex(om.get("SUBTARGETNAMEFIELD_REGEX"));
				subconf.setSubTargetNameField(f_target);

				/** targetValueField */
				f_target = new Field();
				f_target.setFlag(0);
				f_target.setIndex(3);
				f_target.setTablecolumn(om.get("TARGETVALUEFIELD_TABLECOLUMN"));
				s = om.get("TARGETVALUEFIELD_LENGTH");
				if (s == null || s.trim().equals("")) {
					f_target.setLength(0);
				} else {
					f_target.setLength(Integer.parseInt(s.trim()));
				}
				s = om.get("TARGETVALUEFIELD_FORMAT");
				f_target.setFormat(s != null ? s : Config.getDateFormat());
				f_target.setType(om.get("TARGETVALUEFIELD_DATATYPE"));
				f_target.setRegex(om.get("TARGETVALUEFIELD_REGEX"));
				subconf.setTargetValueField(f_target);

				List<Map> targets = codl.getSubTargets(Integer.parseInt(om
						.get("ID")));
				for (int x = 0; x < targets.size(); x++) {
					s = (String) targets.get(x).get("NAME");
					obj.addSubTarget(s.toUpperCase(), s.toUpperCase());
				}

				// N属性
				List<Map> subfieldsMap = codl.getSubFields(Integer.parseInt(om
						.get("ID")));
				List<Field> subfields = new ArrayList<Field>();
				for (int j = 0; j < subfieldsMap.size(); j++) {
					Map<String, String> en = subfieldsMap.get(j);
					Field f = new Field();
					f.setIndex(j + 1);
					f.setName(en.get("NAME"));
					s = en.get("FORMAT");
					f.setFormat(s != null ? s : Config.getDateFormat());
					f.setType(en.get("DATATYPE"));
					f.setTablecolumn(en.get("TABLECOLUMN"));
					s = en.get("LENGTH");
					if (s == null || s.trim().equals("")) {
						f.setLength(0);
					} else {
						f.setLength(Integer.parseInt(s.trim()));
					}
					f.setRegex(en.get("REGEX"));
					subfields.add(f);
				}
				subconf.setFields(subfields);

				// 根据具体列情况重置索引
				subconf.initIndex();

				obj.setSubConf(subconf);
			}

			objcfgs.put(obj.getObjectType().toUpperCase(), obj);
		}
		log.info("初始化对象配置完成，共初始化对象配置:" + objcfgs.size());
		return objcfgs;
	}

	/***************************************************************************
	 * 初始化数据连接对象及操作对象
	 * 
	 * @throws SQLException
	 **************************************************************************/
	public static Map<String, PreparedStatement> initStatement(Connection con,
			Map<String, ConfObj> objcfgs) throws SQLException {
		Map<String, PreparedStatement> statementMap = new HashMap<String, PreparedStatement>();
		con.setAutoCommit(false);
		Iterator it = objcfgs.keySet().iterator();
		while (it.hasNext()) {
			ConfObj obj = objcfgs.get(it.next());
			PreparedStatement ps = con.prepareStatement(obj.getSql());
			statementMap.put(obj.getObjectType(), ps);
			if (obj.isExistsSub()) {
				PmSubConfObj so = obj.getSubConf();
				ps = con.prepareStatement(so.getSql());
				statementMap.put(obj.getObjectType() + "-" + so.getTablename(),
						ps);
			}
		}
		return statementMap;

	}

	/***************************************************************************
	 * 初始化数据连接对象及操作对象
	 * 
	 * @throws SQLException
	 **************************************************************************/
	public static Map<String, PreparedStatement> initStatement(Connection con,
			Map<String, ConfObj> objcfgs, int type) throws SQLException {
		Map<String, PreparedStatement> statementMap = new HashMap<String, PreparedStatement>();
		con.setAutoCommit(false);
		Iterator it = objcfgs.keySet().iterator();
		while (it.hasNext()) {
			ConfObj obj = objcfgs.get(it.next());

			PreparedStatement ps = con.prepareStatement(obj.getSql(type));
			statementMap.put(obj.getObjectType(), ps);
			if (obj.isExistsSub()) {
				PmSubConfObj so = obj.getSubConf();
				ps = con.prepareStatement(so.getSql(type));
				statementMap.put(obj.getObjectType() + "-" + so.getTablename(),
						ps);
			}
		}
		return statementMap;

	}

	/**
	 * 执行sql
	 * 
	 * @throws SQLException
	 */
	public static void execute(Connection con,
			Map<String, PreparedStatement> statementMap) throws SQLException {
		Iterator it = statementMap.values().iterator();
		while (it.hasNext()) {
			PreparedStatement pst = (PreparedStatement) it.next();
			pst.executeBatch();
			pst.clearBatch();
		}
		con.commit();
	}

	/** 关闭数据库相关对象 */
	public static void closeCon(Connection con,
			Map<String, PreparedStatement> statementMap) {
		Iterator it = statementMap.values().iterator();
		while (it.hasNext()) {
			PreparedStatement pst = (PreparedStatement) it.next();
			try {
				pst.close();
			} catch (Exception e) {
				log.error("关闭PreparedStatement 对象出错", e);
			}
		}
		try {
			con.close();
		} catch (Exception e) {
			log.error("关闭connection 对象出错", e);
		}

		try {
			DataSource.close();
		} catch (Exception e) {
			log.error("关闭DataSource对象出错", e);
		}
	}

	/**
	 * 解析csv文件 到一个list中 每个单元个为一个String类型记录，每一行为一个list。 再将所有的行放到一个总list中
	 */
	public static List<List<String>> readCSVFile(String me) throws IOException {
		InputStreamReader fr = new InputStreamReader(new FileInputStream(me));
		BufferedReader br = new BufferedReader(fr);
		String rec = null;// 一行
		String str;// 一个单元格
		List<List<String>> listFile = new ArrayList<List<String>>();
		try {
			// 读取一行
			while ((rec = br.readLine()) != null) {
				Pattern pCells = Pattern
						.compile("(\"[^\"]*(\"{2})*[^\"]*\")*[^,]*,");
				if (!rec.endsWith(",")) {
					rec = rec + ",";
				}
				Matcher mCells = pCells.matcher(rec);
				List<String> cells = new ArrayList<String>();// 每行记录一个list
				// 读取每个单元格
				while (mCells.find()) {
					str = mCells.group();
					str = str.replaceAll(
							"(?sm)\"?([^\"]*(\"{2})*[^\"]*)\"?.*,", "$1");
					str = str.replaceAll("(?sm)(\"(\"))", "$2");
					cells.add(str);
				}
				listFile.add(cells);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (fr != null) {
				fr.close();
			}
			if (br != null) {
				br.close();
			}
		}
		return listFile;
	}

	/**
	 * 根据4G小区的objectid得到小区号 factory :厂家
	 */
	static public String getCiByObjectId(String objectid, String factory) {
		/** 诺西、贝尔厂家、中兴 */
		/*
		 * if (factory.equals("01")||factory.equals("10")||factory.equals("03")) {
		 * String binary = Integer.toBinaryString(Integer.parseInt(objectid));
		 * binary = binary.substring(binary.length() - 8);
		 * //System.out.println(binary); BigInteger bi = new BigInteger(binary,
		 * 2); return bi.toString(10); /**华为
		 */
		/*
		 * } else if (factory.equals("02")) { return
		 * objectid.split(":")[0].split("-")[1]; } else { return objectid; }
		 */

		/** 由于个别厂家存在多个情况，所以根据内容判断如何处理基站号 */
		if (objectid.indexOf("-") > 0) {
			return objectid.split(":")[0].split("-")[1];
		} else {
			String binary = Integer.toBinaryString(Integer.parseInt(objectid));
			binary = binary.substring(binary.length() - 8);
			// System.out.println(binary);
			BigInteger bi = new BigInteger(binary, 2);
			return bi.toString(10);
		}
	}

	/**
	 * 解压gz文件，并返回解压后的文件
	 */
	public static File unGZip(File file) throws IOException {
		int buffer = 2048;

		String savepath = null;
		boolean flag = false;
		String path = file.getAbsolutePath();
		String name = file.getName();
		String outname = name.substring(0, name.lastIndexOf("."));
		savepath = path.substring(0, path.lastIndexOf("\\")) + "\\";
		// System.out.println(savepath);

		BufferedOutputStream bos = null;
		ZipEntry entry = null;
		FileInputStream fis = new FileInputStream(file);
		GZIPInputStream gzi = new GZIPInputStream(fis);
		FileOutputStream out = new FileOutputStream(savepath + outname);
		byte data[] = new byte[buffer];
		int len = 0;
		while ((len = gzi.read(data)) > 0) {
			out.write(data, 0, len);
		}
		gzi.close();
		out.close();
		return new File(savepath + outname);
	}

	/**
	 * 解压Zip文件，并返回文件列表
	 * 
	 * @throws IOException
	 */
	public static List<File> unZip(File file) throws IOException {
		int count = -1;
		int index = -1;
		int buffer = 2048;

		String savepath = "";
		boolean flag = false;
		List<File> outFiles = new ArrayList<File>();
		String path = file.getAbsolutePath();

		savepath = path.substring(0, path.lastIndexOf("\\")) + "\\";

		BufferedOutputStream bos = null;
		ZipEntry entry = null;
		FileInputStream fis = new FileInputStream(path);
		ZipInputStream zis = new ZipInputStream(new BufferedInputStream(fis));
		while ((entry = zis.getNextEntry()) != null) {
			byte data[] = new byte[buffer];

			String temp = entry.getName();

			// flag = isPics(temp);
			// if(!flag)
			// continue;

			index = temp.lastIndexOf("/");
			if (index > -1)
				temp = temp.substring(index + 1);
			temp = savepath + temp;

			File f = new File(temp);
			f.createNewFile();

			FileOutputStream fos = new FileOutputStream(f);
			bos = new BufferedOutputStream(fos, buffer);

			while ((count = zis.read(data, 0, buffer)) != -1) {
				bos.write(data, 0, count);
			}

			bos.flush();
			bos.close();
			outFiles.add(f);
		}

		zis.close();
		return outFiles;
	}

	/**
	 * 根据经纬度换算距离
	 * 
	 * @param lat1
	 *            维度1
	 * @param lat2
	 *            维度2
	 * @param lon1
	 *            经度1
	 * @param lon2
	 *            经度2
	 * @return
	 */
	public static double getDistatce(double lat1, double lat2, double lon1,
			double lon2) {
		double PI = Math.PI; // 圆周率
		double R = 6371004.0; // 地球的半径
		double x, y, distance;
		x = Math.abs(lon2 - lon1) * PI * R
				* Math.cos(((lat1 + lat2) / 2) * PI / 180) / 180;
		y = Math.abs(lat2 - lat1) * PI * R / 180;
		distance = Math.hypot(x, y);
		return distance;
	}

	/***************************************************************************
	 * 根据sql，在指定连接上返回结果
	 * 返回格式为List中存放map对象，每个map对象对应一样数据，key为sql中的字段名称，value为对应字段的查询结果
	 * 
	 **************************************************************************/
	public static List<Map> getObjects(Connection con, String sql)
			throws SQLException {
		PreparedStatement ps = con.prepareStatement(sql);
		List<Map> os = null;

		ResultSet rs = ps.executeQuery();
		os = getValues(rs);
		rs.close();
		ps.close();
		con.close();
		return os;
	}

	/***************************************************************************
	 * 遍历ResultSet对象中的数据，将每行数据存入一个map 并将每个map对象放入list返回
	 **************************************************************************/
	public static List<Map> getValues(ResultSet rs) throws SQLException {
		List vs = new ArrayList<Map>();
		ResultSetMetaData rsmd = rs.getMetaData();
		int cn = rsmd.getColumnCount();
		while (rs.next()) {
			Map<String, String> values = new HashMap<String, String>();
			for (int i = 0; i < cn; i++) {
				String name = rsmd.getColumnName(i + 1);
				values.put(name, rs.getString(name));
			}
			vs.add(values);
		}
		return vs;
	}

	/**
	 * 计算两个角度的差值
	 */
	public static int calculateAngle(int angle1, int angle2) {
		/** 将两个角度换算为0~+360之间 */
		angle1 = angle1 - angle1 / 360 * 360;

		if (angle1 < 0) {
			angle1 += 360;
		}
		//System.out.println(angle1);
		angle2 = angle2 - angle2 / 360 * 360;

		if (angle2 < 0) {
			angle2 += 360;
		}
		//System.out.println(angle2);
		int angle = Math.abs(angle1 - angle2);
		//System.out.println(angle);
		if (angle > 180) {
			angle = 360 - angle;
		}
		return angle;
	}

	public static void main(String s[]) {
		//System.out.println(calculateAngle(-530, 350));
		
		System.out.println(Tools.getDistatce(34.02662, 34.744,113.84988,113.643));
	}

}
