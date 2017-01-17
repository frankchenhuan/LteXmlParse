package wp;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

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

import cfg.Config;
import cfg.CsvOutPrint;
import cfg.CsvWriter;
import cfg.Tools;
import cm.CmParseException;

public class WpParseNomal {

	protected final Log log = LogFactory.getLog(this.getClass());

	private boolean autoUnGZip = false;

	/** 全局的计数变量,记录共处理多少个对象 */
	private int count = 0;

	/** 全局计数变量 记录共处理多少个文件 */
	private int fileCount = 0;

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
	public void parsePathAllXml(String filepath,String outpath) throws XMLStreamException,
			IOException {
		File f = new File(filepath);
		
		File fs[] = f.listFiles();
		boolean isUnGzip = false;
		for (int f_i = 0; f_i < fs.length; f_i++) {
			File curFile = fs[f_i];
			// 如果不是文件或不是mrs的xml文件则直接跳过
			if (!curFile.isFile()) {
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
			if (this.autoUnGZip
					&& curFile.getName().toLowerCase().endsWith("gz")) {
				curFile = Tools.unGZip(curFile);
				isUnGzip = true;
			} else {
				isUnGzip = false;
			}
			/** 非xml文件则跳过 */
			if (!curFile.getName().toLowerCase().endsWith("xml")) {
				if (isUnGzip) {
					curFile.delete();
				}
				continue;
			}

			log.debug(curFile.getName());
			if (Config.isFilter()) {
				File newXml = Tools.formatXML(curFile);
				parse(newXml,outpath);
				// 删除临时文件
				if (Config.isDelTempFile()) {
					newXml.delete();
				}
			} else {
				parse(curFile,outpath);
			}

			if (isUnGzip) {
				curFile.delete();
			}
			fileCount++;
		}
	}

	/** 解析文件并保存到内存对象中 */
	private void parse(File f,String outpath) throws XMLStreamException, IOException {
		XMLInputFactory factory = XMLInputFactory.newInstance();
		XMLEventReader reader = factory
				.createXMLEventReader(new InputStreamReader(
						new FileInputStream(f), Config.getCharSet()));

		String datetime = null;
		List<String> line = null;
		List<String> title = null;
		// String userLabel = null;// 对应数据文件中的userLabel

		CsvOutPrint cop = null;
		String fname=f.getName().substring(1,f.getName().lastIndexOf("."));

		while (reader.hasNext()) {
			XMLEvent xe = reader.nextEvent();
			if (xe.isStartElement()) {
				StartElement se = xe.asStartElement();
				String name = se.getName().getLocalPart();
				if (name.equalsIgnoreCase("FileHeader")) {

				} else if (name.equalsIgnoreCase("DateTime")) {
					String s=reader.getElementText();
					datetime=(s != null ? s.replaceAll("T", " ").substring(0,
							19) : "");
				}  else if (name.equalsIgnoreCase("ObjectType")) {
					String titlename = reader.getElementText();
					
					String filename = outpath + "/"
							+ titlename + "_" + fname + ".csv";

					cop = new CsvWriter(filename);

				} else if (name.equalsIgnoreCase("FieldName")) {
					title = new ArrayList<String>();
					title.add("DateTime");
					title.add("Dn");
					title.add("UserLabel");
				} else if (name.equalsIgnoreCase("N")) {
					title.add(reader.getElementText());
				} else if (name.equalsIgnoreCase("FieldValue")) {

				} else if (name.equalsIgnoreCase("Cm")) {
					line = new ArrayList<String>();
					String dn = se.getAttributeByName(new QName("Dn"))
							.getValue();
					String userlabel = se.getAttributeByName(
							new QName("UserLabel")).getValue();
					line.add(datetime);
					line.add(dn);
					line.add(userlabel);
				} else if (name.equalsIgnoreCase("v")) {
					line.add(reader.getElementText());
				}
			} else if (xe.isEndElement()) {
				EndElement se = xe.asEndElement();
				String name = se.getName().getLocalPart();
				if (name.equalsIgnoreCase("fileHeader")) {
				} else if (name.equalsIgnoreCase("ObjectType")) {

				} else if (name.equalsIgnoreCase("FieldName")) {
					cop.write(title, true);
				} else if (name.equalsIgnoreCase("Cm")) {
					cop.write(line, true);
				} else if (name.equalsIgnoreCase("FieldValue")) {
					cop.flush();
					cop.close();
				}
			}
		}
	}
}
