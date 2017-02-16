package pm;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Iterator;
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
import cfg.KeyConstant;
import cfg.Tools;

public class NXPmParseThread implements Runnable {

	File file;

	// boolean tempfile = false;

	boolean filter = false;

	boolean delTempfile = false;

	long startTime;// 线程开始执行时间
	
	/** 是否是解压缩后的文件*/
	boolean isUnzip=false;

	public boolean isUnzip() {
		return isUnzip;
	}

	public void setUnzip(boolean isUnzip) {
		this.isUnzip = isUnzip;
	}

	public boolean isDelTempfile() {
		return delTempfile;
	}

	public void setDelTempfile(boolean delTempfile) {
		this.delTempfile = delTempfile;
	}

	public boolean isFilter() {
		return filter;
	}

	public void setFilter(boolean filter) {
		this.filter = filter;
	}

	public NXPmParseThread(File f) {
		this.file = f;
	}

	public NXPmParseThread() {
		// TODO Auto-generated constructor stub
	}

	protected final Log log = LogFactory.getLog(this.getClass());

	protected void parseXML(File f) throws FileNotFoundException,
			XMLStreamException, UnsupportedEncodingException, IOException {
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
					String s = reader.getElementText().toUpperCase();
					confobj = NXPmParseMain.objcfgs.get(s);
					// log.debug(s);
					if (confobj == null) {
						flag = false;
						// return;
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
					pmdataobj = NXPmParseMain.getPmDataObj(begintime + "_" + dn
							+ "_" + endtime);
					//pmdataobj = new PmDataObj();
					// if (pmdataobj == null) {
					// pmdataobj = new PmDataObj();
					// pmData.put(begintime + "_" + dn + "_" + endtime,
					// pmdataobj);
					// }
					// }
					pmdataobj.setPmConfObj(confobj);
					pmdataobj.addValue(KeyConstant.KEY_OBJDN, dn);
					pmdataobj.addValue(KeyConstant.KEY_OBJUSERLABEL, userLabel);
					pmdataobj.addValue(KeyConstant.KEY_FILENAME, f.getName());
					pmdataobj.addValue(KeyConstant.KEY_BEGINTIME, begintime);
					pmdataobj.addValue(KeyConstant.KEY_ENDTIME, endtime);
					Iterator<String> it = NXPmParseMain.constant.keySet()
							.iterator();
					while (it.hasNext()) {
						String key = it.next();
						pmdataobj
								.addValue(key, NXPmParseMain.constant.get(key));
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

			// if (pmData.size() >= Config.getCommitCount()) {
			// insertObjData();
			// pmData.clear();
			// }
		}
	}

	/** 返回线程执行时间 */
	public long getRunTime() {
		return System.currentTimeMillis() - startTime;
	}

	public void run() {
		//startTime = System.currentTimeMillis();
		// log.debug("线程开始");
		/** 线程数加一 */
		// NXPmParseMain.addThreadNum();
		// 过滤特殊字符,并生成临时文件
		log.debug(this.file.getName());
		File newfile=null;
		if (this.isFilter()) {
			try {
				newfile = Tools.formatXML(this.file);
			} catch (Exception e) {
				log.error("创建临时文件错误", e);
			}
		}else
		{
			newfile=this.file;
		}

		try {
			parseXML(newfile);
		} catch (Exception e) {
			log.error("解析文件出错" + this.file.getName(), e);
		} finally {
			// 删除临时文件
			try {
				if (this.isFilter() && this.isDelTempfile()) {
					newfile.delete();
				}
				if(this.isUnzip)
				{
					this.file.delete();
				}
			} catch (Exception e) {
				log.error("删除临时文件错误", e);
			}
		}
		/** 线程结束，线程数减一 */
		// NXPmParseMain.minusThreadNum();
		// log.debug(this.file.getName());
		// log.debug("线程结束,用时:"+((System.currentTimeMillis()-time)/1000)+"秒");
	}

	public File getFile() {
		return file;
	}

	public void setFile(File file) {
		this.file = file;
	}

}
