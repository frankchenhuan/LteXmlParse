package Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;

//import com.csvreader.CsvReader;

import cfg.Config;
import cfg.Tools;

public class FileTest {

	public static void main(String[] args) {
		test5();
	}

	public static void test2() {
		String name = "E:\\公司\\项目\\网优平台\\采集\\20170208\\PM-ENB-EUTRANCELLTDD-2A-V2.7.0-20170208130000-15.csv";
		try {
			List<List<String>> data = Tools.readCSVFile(name);
			int i = 0;
			for (List<String> line : data) {
				for (String v : line) {
					System.out.println(v);
				}
				i++;
				if (i > 10)
					break;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/*public static void test3(){
		String name = "E:\\公司\\项目\\网优平台\\采集\\20170208\\PM-ENB-EUTRANCELLTDD-2A-V2.7.0-20170208130000-15.csv";
		try {
			System.out.println(new File(name).getAbsolutePath());
			CsvReader  cr=new CsvReader(name, '|', Charset.forName("GBK"));
			cr.readRecord();
			System.out.println(cr.getRawRecord());
			boolean flag=cr.readHeaders();
			System.out.println();
			flag=cr.readHeaders();
			System.out.println(flag);
			if(flag)
			{
				int count=cr.getHeaderCount();
				System.out.println(count);
				for(int i=0;i<count;i++)
				{
					System.out.println(cr.getHeader(i));
				}
			}
			//cr.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}*/

	public static void test1() {
		String dn = "ALU-CMHA-ZZ,SubNetwork=1,ManagedElement=B_RNORUNANBENDIDIANDC_268739,EnbFunction=460-00-268739,EutranCellTdd=128";
		int index = dn.indexOf("EutranCellTdd");
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
	}
	
	public static void test4()
	{
		SAXBuilder builder = new SAXBuilder(); // 创建一个SAX构造器
		String name="E:\\公司\\项目\\网优平台\\采集\\CMCC-ENB-NRM-ALLV2.7.0-CellComponent-20170216-0320_185.xml";
		Config.init(1);
		try {
			File newfile=Tools.formatXML(new File (name));
			builder.build(new FileInputStream(newfile));
		} catch (JDOMException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void test5()
	{
		SAXBuilder builder = new SAXBuilder(); // 创建一个SAX构造器
		String name="E:\\公司\\项目\\网优平台\\采集\\20170509\\ENB-PM-V2.6.0-EutranCellTdd-20170426-1645P03_185.xml";
		Config.init(1);
		try {
			File newfile=Tools.formatXML(new File (name));
			builder.build(new FileInputStream(newfile));
		} catch (JDOMException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
