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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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

public class PmParse {

	/** 全局的计数变量,记录共处理多少个对象 */
	private int count = 0;

	/** 全局计数变量 记录共处理多少个文件 */
	private int fileCount = 0;

	/**
	 * 保存配置文件中的对象
	 */
	private Map<String, ConfObj> objcfgs = new HashMap<String, ConfObj>();

	/**
	 * 保存常量
	 */
	private Map<String, String> constant = new HashMap<String, String>();

	/**
	 * 数据连接对象
	 */
	private Connection con;
	private Map<String, PreparedStatement> statementMap = new HashMap<String, PreparedStatement>();

	protected final Log log = LogFactory.getLog(PmParse.class);

	/**
	 * 存放所有数据对象，以数据文件中dn作为key值
	 */
	private Map<String, PmDataObj> pmData = new HashMap<String, PmDataObj>();

	

	public void insertObjData() throws SQLException {
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

	public void setPrepared(PreparedStatement ps, Field field, String value,
			PmDataObj pmd) throws SQLException {
		Map<String, String> values = pmd.getValues();
		String filename = values.get(KeyConstant.KEY_FILENAME);
		Tools.setPrepared(ps, field, value, pmd.getPmConfObj().getObjectType(),
				filename);
	}

	public void setPrepared(PreparedStatement ps, Field field,
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
	 */
	public void parsePathAllXml(String filepath) throws XMLStreamException,
			SQLException, IOException {
		File f = new File(filepath);
		File fs[] = f.listFiles();
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

			log.debug(fs[f_i].getName());
			if (Config.isFilter()) {
				File newXml = Tools.formatXML(fs[f_i]);
				parseXML(newXml);
				// 删除临时文件
				if (Config.isDelTempFile()) {
					newXml.delete();
				}
			} else {
				parseXML(fs[f_i]);
			}

			fileCount++;
		}
	}

	public void parseXML(File f) throws FileNotFoundException,
			XMLStreamException, UnsupportedEncodingException, SQLException {
		XMLInputFactory factory = XMLInputFactory.newInstance();
		XMLEventReader reader = factory
				.createXMLEventReader(new InputStreamReader(
						new FileInputStream(f), Config.getCharSet()));

		ConfObj confobj = null;// 当前正在解析的对象类型
		Map<String, String> pmNames = null;
		PmDataObj pmdataobj = null;
		boolean isSubTarge = false;// 判断当前分项指标是否配置
		String cv_name = null;
		String cv_sn = null;
		String cv_sv = null;
		String begintime = null;
		String endtime = null;
		boolean flag = false;// 判断是否配置有解析的对象，如果没有则不解析
		while (reader.hasNext()) {
			XMLEvent xe = reader.nextEvent();

			if (xe.isStartElement()) {
				StartElement se = xe.asStartElement();
				String name = se.getName().getLocalPart();

				if (name.equals("ObjectType")) {
					// 设置当前解析对象类型
					confobj = objcfgs
							.get(reader.getElementText().toUpperCase());
					if (confobj == null) {
						flag = false;
					} else {
						flag = true;
					}
				} else if (flag && name.equals("PmName")) {
					pmNames = new HashMap<String, String>();
				} else if (flag && name.equals("N")) {
					pmNames.put(se.getAttributeByName(new QName("i"))
							.getValue(), reader.getElementText().toUpperCase());
				} else if (flag && name.equals("PmData")) {
				} else if (flag && name.equals("Pm")) {
					String dn = se.getAttributeByName(new QName("Dn"))
							.getValue();
					String userLabel = se.getAttributeByName(
							new QName("UserLabel")).getValue();
					// synchronized (dn) {
					pmdataobj = pmData
							.get(begintime + "_" + dn + "_" + endtime);
					if (pmdataobj == null) {
						pmdataobj = new PmDataObj();
						pmData.put(begintime + "_" + dn + "_" + endtime,
								pmdataobj);
					}
					// }
					pmdataobj.setPmConfObj(confobj);
					pmdataobj.addValue(KeyConstant.KEY_OBJDN, dn);
					pmdataobj.addValue(KeyConstant.KEY_OBJUSERLABEL, userLabel);
					pmdataobj.addValue(KeyConstant.KEY_FILENAME, f.getName());
					pmdataobj.addValue(KeyConstant.KEY_BEGINTIME, begintime);
					pmdataobj.addValue(KeyConstant.KEY_ENDTIME, endtime);
					Iterator<String> it = constant.keySet().iterator();
					while (it.hasNext()) {
						String key = it.next();
						pmdataobj.addValue(key, constant.get(key));
					}
					String dns[] = dn.split(",");
					for (int y = 0; y < dns.length; y++) {
						String ds[] = dns[y].split("=");
						if (ds.length > 1) {
							pmdataobj.addValue("DN-" + ds[0].toUpperCase(),
									ds[1]);
						}
					}
				} else if (flag && name.equals("V")) {
					String i = se.getAttributeByName(new QName("i")).getValue();
					String pmname = pmNames.get(i);
					String value = reader.getElementText();
					pmdataobj.addValue(pmname, value);
				} else if (flag && name.equals("CV")) {
					String i = se.getAttributeByName(new QName("i")).getValue();
					cv_name = pmNames.get(i);
					isSubTarge = confobj.isExistsSubTarget(cv_name);
				} else if (flag && name.equals("SN")) {
					cv_sn = reader.getElementText();
				} else if (flag && name.equals("SV")) {
					cv_sv = reader.getElementText();
					if (isSubTarge) {
						Map cv_map = pmdataobj.getSubTargetValues(cv_name);
						if (cv_map == null) {
							cv_map = new HashMap<String, String>();
							pmdataobj.addSubTargetValue(cv_name, cv_map);
						}
						cv_map.put(cv_sn.toUpperCase(), cv_sv);
					}
					//pmdataobj.addValue(cv_sn.toUpperCase(), cv_sv);
					pmdataobj.addValue("<"+cv_name+">"+cv_sn.toUpperCase(), cv_sv);

				} else if (name.equals("BeginTime")) {
					String s = reader.getElementText();
					begintime = (s != null ? s.replaceAll("T", " ").substring(
							0, 19) : "");
				} else if (name.equals("EndTime")) {
					String s = reader.getElementText();
					endtime = (s != null ? s.replaceAll("T", " ").substring(0,
							19) : "");
				}
			} else if (xe.isEndElement()) {
				EndElement se = xe.asEndElement();
				String name = se.getName().getLocalPart();
				if (name.equals("ObjectType")) {

				} else if (name.equals("PmData")) {

				} else if (name.equals("Pm")) {
					pmdataobj = null;
				} else if (name.equals("V")) {
				} else if (name.equals("CV")) {
					isSubTarge = false;
				} else if (name.equals("SN")) {
				} else if (name.equals("SV")) {
				}
			}

			if (pmData.size() >= Config.getCommitCount()) {
				insertObjData();
				pmData.clear();
			}
		}

	}

	public void wirteLog() {
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

	public void putConstant(String key, String value) {
		this.constant.put(key, value);
	}

	public void close() {
		Tools.closeCon(con, statementMap);
	}

	public Map<String, ConfObj> getObjcfgs() {
		return objcfgs;
	}

	public void setObjcfgs(Map<String, ConfObj> objcfgs) {
		this.objcfgs = objcfgs;
	}

	public Map<String, PreparedStatement> getStatementMap() {
		return statementMap;
	}

	public void setStatementMap(Map<String, PreparedStatement> statementMap) {
		this.statementMap = statementMap;
	}

	public Connection getCon() {
		return con;
	}

	public void setCon(Connection con) {
		this.con = con;
	}

	public Map<String, String> getConstant() {
		return constant;
	}

	public void setConstant(Map<String, String> constant) {
		this.constant = constant;
	}
}
