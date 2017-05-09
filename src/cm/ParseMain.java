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

import javax.xml.stream.XMLStreamException;

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

	/** 全局的计数变量,记录共处理多少个对象 */
	static int count = 0;

	/** 全局计数变量 记录共处理多少个文件 */
	static int fileCount = 0;
	
	
	
	/**
	 * 保存配置文件中的对象
	 */
	static Map<String, ConfObj> objcfgs = new HashMap<String, ConfObj>();

	/**
	 * 保存常量
	 */
	static Map<String, String> constant = new HashMap<String, String>();

	/**
	 * 数据连接对象
	 */
	static Connection con;
	static Map<String, PreparedStatement> statementMap = new HashMap<String, PreparedStatement>();

	protected static final Log log = LogFactory.getLog(ParseMain.class);

	public static void main(String a[]) {

		if (a.length < 3) {
			log.error("输入参数数量不足");
			return;
		}

		Config.init(1);// 初始化配置文件
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
		log.info("默认编码："+Charset.defaultCharset());

		try { // 初始化数据库连接池
			DataSource.init(); // 初始化数据连接对象及操作对象
			// 初始化配置对象到Objcfgs变量
			objcfgs = Tools.initObjByXML(omc);
			con = DataSource.getConnection();
			statementMap = Tools.initStatement(con, objcfgs, flag);
		} catch (Exception e) {
			log.error("初始化数据库连接出错", e);
			return;
		}

		try {
			parsePathAllXml(filepath);
		} catch (Exception e) {
			log.error("解析文件异常：", e);
		}

		/** 关闭数据库连接等信息 */
		Tools.closeCon(con, statementMap);

		/**
		 * 文件处理完成后输出日志
		 */
		writeLog();

	}

	/**
	 * 解析路径下的xml文件并入库
	 * 
	 * @throws CmParseException
	 * @throws SQLException
	 */
	public static void parsePathAllXml(String filepath) throws JDOMException,
			IOException, SQLException {
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
			//log.debug(curFile.getName());
			//过滤需要过滤的文件
			boolean isContinue = false;
			String[] filter_files = Config.getFilter_files();
			String name=curFile.getName();
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
				log.debug("开始解析："+curFile.getName());
				if (curFile.getName().toLowerCase().endsWith("xml")) {
					if (Config.isFilter()) {
						File newXml = Tools.formatXML(curFile);
						parseXML(curFile);
						// 删除临时文件
						if (Config.isDelTempFile()) {
							newXml.delete();
						}
					} else {
						parseXML(curFile);
					}
					fileCount++;
				}
				if (isUnGzip) {
					curFile.delete();
				}
				
			}
		}
		/**
		 * 解析结束后将剩余部分执行入库
		 */
		Tools.execute(con, statementMap);
	}

	public static void parseXML(File f) throws SQLException, JDOMException,
			IOException {
		SAXBuilder builder = new SAXBuilder(); // 创建一个SAX构造器
		Document doc;

		/** 保存正在解析的对象 */
		ConfObj cfgxmlobj;

		/**
		 * 存放属性名称和索引关系的map
		 */
		Map<String, String> field_index_map = new HashMap<String, String>();
		/** 存放value值的map */
		Map<String, String> value_map = new HashMap<String, String>();
		/**存放忽略key值大小写的map*/
		Map<String,String> value_map1=new HashMap<String, String>(); 

		doc = builder.build(f);

		Element root = doc.getRootElement(); // 获取根元素
		String filename = f.getName();

		/**
		 * 读取文件头时间常量，并存入常量map
		 */
		Element e_header = root.getChild("FileHeader");
		String dateTime = e_header.getChild("TimeStamp").getValue();
		dateTime = (dateTime != null ? dateTime.replaceAll("T", " ").substring(
				0, 19) : "");
		constant.put(KeyConstant.KEY_DATETIME, dateTime);

		/**
		 * 循环Objects
		 */
		List<Element> e_objects = root.getChildren("Objects");
		
		for (int i = 0; i < e_objects.size(); i++) {
			cfgxmlobj = null;
			Element el_Object = e_objects.get(i);
			/**由于京信厂家格式不标准，多一层objects标签，增加以下代码解决一下问题*/
			if(el_Object.getChild("Objects")!=null)
			{
				el_Object=el_Object.getChild("Objects");
			}
			
			String objectTypes[]=f.getName().split("-");
			for(int j=0;j<objectTypes.length;j++)
			{
				/*cfgxmlobj = objcfgs.get(el_Object.getChildText("ObjectType")
						.toUpperCase());*/
				cfgxmlobj = objcfgs.get(objectTypes[j]
						.toUpperCase());
				if(cfgxmlobj!=null)
					break;
			}
			

			/** 如果未找到对象，则证明不需要解析 */
			if (cfgxmlobj == null) {
				continue;
			}

			// 用于提取对象的属性
			List<Element> e_fieldname = el_Object.getChild("FieldName")
					.getChildren("N");

			/**
			 * 保存属性名称和索引关系
			 */
			field_index_map.clear();// 使用前先清空
			for (int x = 0; x < e_fieldname.size(); x++) {
				field_index_map.put(e_fieldname.get(x).getAttributeValue("i"),
						e_fieldname.get(x).getValue());
			}

			// 用于提取对象的值
			List<Element> e_fvs = el_Object.getChild("FieldValue").getChildren(
					"Object");
			// 下面的循环用于遍历对象的属性

			for (int x = 0; x < e_fvs.size(); x++) {
				value_map.clear();// 赋值前先清空
				value_map1.clear();

				// 将所有常量存入对象map
				value_map.putAll(constant);
				value_map1.putAll(constant);
				Element e_cm = e_fvs.get(x);
				String dn = e_cm.getAttributeValue("Dn");
				String userLabel = e_cm.getAttributeValue("UserLabel");
				String rmUID=e_cm.getAttributeValue("rmUID");
				value_map.put(KeyConstant.KEY_OBJDN, dn);
				value_map.put(KeyConstant.KEY_OBJUSERLABEL, userLabel);
				value_map.put(KeyConstant.KEY_RMUID, rmUID);
				
				value_map1.put(KeyConstant.KEY_OBJDN, dn);
				value_map1.put(KeyConstant.KEY_OBJUSERLABEL, userLabel);
				value_map1.put(KeyConstant.KEY_RMUID, rmUID);
				
				String dns[] = dn.split(",");
				for (int y = 0; y < dns.length; y++) {
					String ds[] = dns[y].split("=");
					if (ds.length > 1) {
						value_map.put("DN-" + ds[0].toUpperCase(), ds[1]);
						value_map1.put("DN-" + ds[0].toUpperCase(), ds[1]);
					}
				}
				List<Element> e_fvalue = e_cm.getChildren("V");
				for (int y = 0; y < e_fvalue.size(); y++) {
					Element v = e_fvalue.get(y);

					/** 将属性作为key 值作为value存放 */
					value_map.put(field_index_map.get(v.getAttributeValue("i")), v.getValue());
					value_map1.put(field_index_map.get(v.getAttributeValue("i"))
							.toUpperCase(), v.getValue());
				}
				value_map.put(KeyConstant.KEY_FILENAME, filename);
				value_map1.put(KeyConstant.KEY_FILENAME, filename);
				/** 将保存好的数据对象放入数据缓冲区 */
				addBatch(cfgxmlobj, value_map,value_map1);

				/** 入库条数+1 */
				count++;
				/**
				 * 计数超过设置的提交条数则执行提交
				 */
				if (count / Config.getCommitCount() >= 1
						&& count % Config.getCommitCount() == 0) {
					Tools.execute(con, statementMap);
				}

				/** 对象计数+1 */
				cfgxmlobj.addNum();
			}
		}
	}

	public static void addBatch(ConfObj cfgxmlobj, Map<String, String> value_map,Map<String, String> value_map1 )
			throws SQLException {
		PreparedStatement pst = null;

		pst = statementMap.get(cfgxmlobj.getObjectType());

		/** 循环对数据库赋值 */
		for (int z = 0; z < cfgxmlobj.getFields().size(); z++) {
			Field field = cfgxmlobj.getFields().get(z);
			String s = null;
			String filename = value_map.get(KeyConstant.KEY_FILENAME);
			if(field.isIgnoreCase())//忽略大小写
			{
				s = value_map1.get(field.getName().toUpperCase());
			}else //不忽略大小写
			{
				s = value_map.get(field.getName());
			}
			
			Tools.setPrepared(pst, field, s, cfgxmlobj.getObjectType(),
					filename);
		}

		/** 将设置好的sql加入缓冲区 */
		pst.addBatch();
	}

	/**
	 * 记录日志
	 */
	public static void writeLog() {
		Iterator it = objcfgs.values().iterator();
		while (it.hasNext()) {
			ConfObj o = (ConfObj) it.next();
			log.info(o.getObjectType() + "对象数量:" + o.getNum());
		}
		log.info("处理对象总数:" + count);
		log.info("处理文件总数:" + fileCount);
	}
}
