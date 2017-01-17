package mr.mrs;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import pm.PmParse;

import cfg.Config;
import cfg.CsvOutPrint;
import cfg.CsvWriter;
import cfg.Tools;

public class MrsParse {

	/**
	 * �ж��Ƿ������ֵ�����
	 */
	String reg = "^\\d+(.\\d+)?$";

	protected final Log log = LogFactory.getLog(MrsParse.class);
	/**
	 * ��mrNameΪkey���洢mr����ָ������
	 */
	Map<String, List> mrsTitles = new HashMap<String, List>();

	/**
	 * ���mrName��Ӧ������
	 */
	Map<String, Map<String, List>> mrsData = new HashMap<String, Map<String, List>>();

	/** ��¼�����ļ������� */
	private int fileCount = 0;

	/** �豸���� */
	private String factory;

	private boolean autoUnGZip = false;

	public int getFileCount() {
		return this.fileCount;
	}

	/** �����ļ������浽�ڴ������ */
	private void parse(File f) throws XMLStreamException, IOException {
		XMLInputFactory factory = XMLInputFactory.newInstance();
		XMLEventReader reader = factory
				.createXMLEventReader(new InputStreamReader(
						new FileInputStream(f), Config.getCharSet()));

		String startTime = null;
		String endTime = null;
		String reportTime = null;
		List<String> lines = new ArrayList<String>();
		List<String> titles = null;
		List<String> titlesX = null;
		String eNBID = null;// ��Ӧ�����ļ��е�enb id
		String cellid = null;// С����
		String userLabel = null;// ��Ӧ�����ļ��е�userLabel
		String objID = null;// ��Ӧ�����ļ���object id
		String qci = null;
		/**
		 * ��¼ָ�������������ж�ֵ��ָ���Ƿ�����һ��
		 */
		int titlesLength = 0;

		/**
		 * �����ļ��е�mrName
		 */
		String mrName = null;

		/**
		 * ����������ַ����mrName��ͳһ����,���ںϲ�
		 */
		String mrNameX = null;

		/**
		 * �ڵ�һ�ν�������ͬ��mrNameʱ�˱���Ϊtrue �ж��Ƿ���Ҫtitles������ָ���ͷ
		 */
		boolean addTitles = false;

		while (reader.hasNext()) {
			XMLEvent xe = reader.nextEvent();
			if (xe.isStartElement()) {
				StartElement se = xe.asStartElement();
				String name = se.getName().getLocalPart();
				if (name.equals("fileHeader")) {
					String s = se.getAttributeByName(new QName("reportTime"))
							.getValue();
					reportTime = (s != null ? s.replaceAll("T", " ").substring(
							0, 19) : "");
					s = se.getAttributeByName(new QName("startTime"))
							.getValue();
					startTime = (s != null ? s.replaceAll("T", " ").substring(
							0, 19) : "");
					s = se.getAttributeByName(new QName("endTime")).getValue();
					endTime = (s != null ? s.replaceAll("T", " ").substring(0,
							19) : "");
				} else if (name.equalsIgnoreCase("eNB")) {
					eNBID = se.getAttributeByName(new QName("id")).getValue();
					Attribute ul = se
							.getAttributeByName(new QName("userLabel"));
					userLabel = ul == null ? "" : ul.getValue();
				} else if (name.equals("measurement")) {
					mrName = se.getAttributeByName(new QName("mrName"))
							.getValue();
					/**
					 * ����ǲ��������ȡ�÷�������֣���Ϊqci
					 */
					if (mrName.matches(".*\\d.*")) {
						String s[] = mrName.split("");
						for (int i = 0; i < s.length; i++) {
							if (s[i].matches("\\d")) {
								qci = s[i];
								continue;
							}
						}
					} else {
						qci = "";
					}
					mrNameX = mrName.replaceFirst("\\d", "X");
					titles = mrsTitles.get(mrName);
					if (titles == null) {
						titles = new ArrayList<String>();
						titles.add("reportTime");
						titles.add("startTime");
						titles.add("endTime");
						titles.add("eNBID");
						titles.add("cellid");
						titles.add("userLabel");
						titles.add("objectid");
						titles.add("factory");
						titles.add("QCI");
						mrsTitles.put(mrName, titles);
						if (!mrNameX.equals(mrName)) {
							titlesX = new ArrayList<String>();
							titlesX.add("reportTime");
							titlesX.add("startTime");
							titlesX.add("endTime");
							titlesX.add("eNBID");
							titlesX.add("cellid");
							titlesX.add("userLabel");
							titlesX.add("objectid");
							titlesX.add("factory");
							titlesX.add("QCI");
							mrsTitles.put(mrNameX, titlesX);
						}
						addTitles = true;
					} else {
						addTitles = false;
					}
				} else if (name.equals("smr")) {
					String s = reader.getElementText().trim();
					String sp[] = s.split(" ");
					titlesLength = sp.length;
					if (addTitles) {
						for (int i = 0; i < titlesLength; i++) {
							titles.add(sp[i]);
							if (!mrNameX.equals(mrName)) {
								titlesX.add(sp[i].replaceFirst("\\d", "X"));
							}
						}
					}

				} else if (name.equals("object")) {// ȡ��С����
					objID = se.getAttributeByName(new QName("id")).getValue();
					objID = objID.split(":")[0];
					cellid = Tools.getCiByObjectId(objID, this.factory);
				} else if (name.equals("v")) {
					String s = reader.getElementText().trim();
					String sp[] = s.split(" ");

					// ���ݺͱ�ͷһ�£��򱣴�����
					if (titlesLength == sp.length) {
						lines = new ArrayList<String>();
						lines.add(reportTime);
						lines.add(startTime);
						lines.add(endTime);
						lines.add(eNBID);
						lines.add(cellid);
						lines.add(userLabel);
						lines.add(objID);
						lines.add(this.factory);
						lines.add(qci);
						for (int i = 0; i < sp.length; i++) {
							lines.add(sp[i]);
						}
						this.addObjectList(mrName, lines, objID, 9);
						if (!mrNameX.equals(mrName)) {// ��������ֵķ���
							this.addObjectList(mrNameX, lines, objID + "-"
									+ qci, 9);
						}
					} else {
						log.info("����ֵ�������㣺" + sp.length + " type:" + mrName
								+ " enbID=" + eNBID + " objectid=" + objID);
					}
				}
			} else if (xe.isEndElement()) {
				EndElement se = xe.asEndElement();
				String name = se.getName().getLocalPart();
				if (name.equals("fileHeader")) {
				} else if (name.equals("bulkPmMrDataFile")) {
					/*
					 * Iterator<CsvOutPrint> it = mrOuts.values().iterator();
					 * while (it.hasNext()) { it.next().close(); }
					 */
				}
			}
		}
	}

	/**
	 * ������������Ϊ�ļ� path:ָ�����·��
	 * 
	 * @throws IOException
	 */
	private void output(String path) throws IOException {
		Iterator<String> mrsNames = this.mrsTitles.keySet().iterator();
		// long time = System.currentTimeMillis();
		Calendar ca = Calendar.getInstance();

		String stime = "" + ca.get(Calendar.YEAR) + ca.get(Calendar.MONTH) + 1
				+ ca.get(Calendar.DAY_OF_MONTH) + ca.get(Calendar.HOUR_OF_DAY)
				+ ca.get(Calendar.MINUTE) + ca.get(Calendar.SECOND);
		while (mrsNames.hasNext()) {
			String mrsName = mrsNames.next();
			List mrsTitle = this.mrsTitles.get(mrsName);
			CsvOutPrint cop = new CsvWriter(path + "/" + mrsName + "-" + stime
					+ ".csv");
			cop.write(mrsTitle, true);
			Map<String, List> mrsvmap = this.mrsData.get(mrsName);
			if (mrsvmap == null) {
				continue;
			}
			Iterator<List> mrsVs = mrsvmap.values().iterator();
			while (mrsVs.hasNext()) {
				cop.write(mrsVs.next(), true);
			}
			cop.flush();
			cop.close();
		}
	}

	/**
	 * �����ļ���������mrs�ļ�
	 * 
	 * @throws IOException
	 * @throws XMLStreamException
	 */
	private void parseAll(String path) throws IOException, XMLStreamException {
		File f = new File(path);
		File fs[] = f.listFiles();
		boolean isUnGzip = false;
		List<File> currentFils = new ArrayList<File>();
		for (int f_i = 0; f_i < fs.length; f_i++) {
			File curFile = fs[f_i];
			// ��������ļ�����mrs��xml�ļ���ֱ������
			if (!curFile.isFile() || curFile.getName().indexOf("MRS") < 0) {
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
			if (this.autoUnGZip
					&& curFile.getName().toLowerCase().endsWith("gz")) {
				curFile = Tools.unGZip(curFile);
				isUnGzip = true;
				currentFils.add(curFile);
			} else if (this.autoUnGZip
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
				/** ��xml�ļ������� */
				if (!curFile.getName().toLowerCase().endsWith("xml")) {
					if (isUnGzip) {
						curFile.delete();
					}
					continue;
				}

				log.debug(curFile.getName());
				try {
					if (Config.isFilter()) {
						File newXml = Tools.formatXML(curFile);
						parse(newXml);
						// ɾ����ʱ�ļ�
						if (Config.isDelTempFile()) {
							newXml.delete();
						}
					} else {
						parse(curFile);
					}
				} catch (XMLStreamException e) {
					log.error("�ļ����������������ļ�:" + curFile.getName(), e);
				}

				if (isUnGzip) {
					curFile.delete();
				}
				fileCount++;
			}
		}
	}

	/**
	 * ��ָ��mrName�����ݼ���洢list
	 */
	private void addObjectList(String mrName, List<String> lines, String objID,
			int startIndex) {
		Map<String, List> mrValues = this.mrsData.get(mrName);
		if (mrValues == null) {
			mrValues = new HashMap<String, List>();
			this.mrsData.put(mrName, mrValues);
			mrValues.put(objID, lines);
		} else {
			List<String> v = mrValues.get(objID);
			if (v != null) {
				/**
				 * ���������ݼӺͺϲ�
				 */
				List newV = new ArrayList<String>();

				// reportTime,ȡ����ֵ
				newV.add(lines.get(0).compareTo(v.get(0)) > 0 ? lines.get(0)
						: v.get(0));
				// startTime,ȡ��С��ֵ
				newV.add(lines.get(1).compareTo(v.get(1)) > 0 ? v.get(1)
						: lines.get(1));
				// endTime,ȡ����ֵ
				newV.add(lines.get(2).compareTo(v.get(2)) > 0 ? lines.get(2)
						: v.get(2));
				for (int i = 3; i < startIndex; i++) {
					newV.add(lines.get(i) + "");
					// newV.add(lines.get(4));
					// newV.add(lines.get(5));
					// newV.add(lines.get(6));
					// newV.add(lines.get(7));
					// newV.add(lines.get(8));
				}
				for (int i = startIndex; i < v.size(); i++) {
					String v1 = v.get(i);
					String v2 = lines.get(i);
					if (v1.matches(reg) && v2.matches(reg)) {
						float fv = Float.parseFloat(v.get(i))
								+ Float.parseFloat(lines.get(i));
						newV.add(fv + "");
					} else if (v1.matches(reg)) {
						float fv = Float.parseFloat(v1);
						newV.add(fv + "");
					} else if (v2.matches(reg)) {
						float fv = Float.parseFloat(v2);
						newV.add(fv + "");
					} else {
						newV.add("");
					}
				}
				mrValues.put(objID, newV);
			} else {
				mrValues.put(objID, lines);
			}

		}
	}

	/**
	 * ����������ļ� path:��Ҫ�������ļ�·�� outpath:�������·�� factory:�豸���ң����ݲ�ͬ���ң�������ͬ�ĳ����������
	 * 
	 * @throws XMLStreamException
	 * @throws IOException
	 */
	public void parseAndOut(String path, String outpath, String factory,
			boolean autoUnGzip) {
		this.factory = factory;
		this.autoUnGZip = autoUnGzip;
		try {
			this.parseAll(path);
			this.output(outpath);
			log.info("�������ļ�" + this.fileCount + "��");
		} catch (Exception e) {
			log.error("��������", e);
		}
	}

}
