package mr.mro;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * LTE-MRO解析工具类
 * @author Administrator
 */
public class Tool {
	/**
	 * 计算站间距
	 * @param lat1
	 * @param lat2
	 * @param lon1
	 * @param lon2
	 * @return 单位，米(m)
	 */
	public static double getDistatce(double lat1, double lat2, double lon1, double lon2) {
		double PI = 3.141592653589793; // 圆周率
	    double R = 6371004.0; // 地球的半径
		double x=0, y=0, distance=0;
		if(lat1>0 && lat2>0 && lon1>0 && lon2>0){
			x = Math.abs(lon2 - lon1) * PI * R* Math.cos(((lat1 + lat2) / 2) * PI / 180) / 180;
	        y = Math.abs(lat2 - lat1) * PI * R / 180;
	        distance = Math.hypot(x, y);
		}else{
			distance = -1;
		}
        return distance;
    }
	
	public static void main(String[] args){
		System.out.println(Tool.getCiByObjectId("66683394", "03", 27));
//		Map<String,String> map = new HashMap<String,String>();
//		map.put("", "ccc");
//		if(map.containsKey("")){
//			System.out.println("aaa");
//		}else{
//			System.out.println("bbb");
//		}
	}
	
	/**
	 * 根据格式字符串，以及小时偏移量，格式化当前日期
	 * @param formate
	 * @return
	 */
	public static String initDate(String formate,int add){
		Calendar c = Calendar.getInstance();
		c.setTime(new Date());
		c.add(Calendar.HOUR_OF_DAY, add);
		Date date = c.getTime();
		SimpleDateFormat format = new SimpleDateFormat(formate);
		return format.format(date);
	}
	
	/**
	 * 根据4G小区的objectid得到小区号 factory :厂家
	 */
	static public String getCiByObjectId(String objectid, String factory, int arrayLength) {
		/** 诺西厂家 */
		if (factory.equals("01") || factory.equals("04") || (factory.equals("03") && arrayLength==ZTE2.NSN_SMR_LENGTH)) {
			String binary = Integer.toBinaryString(Integer.parseInt(objectid));
			binary = binary.substring(binary.length() - 8);
			BigInteger bi = new BigInteger(binary, 2);
			return bi.toString(10);
		} else if (factory.equals("02") || (factory.equals("03") && arrayLength==ZTE.NSN_SMR_LENGTH)) {
			return objectid.split(":")[0].split("-")[1];
		} else {
			return "";
		}
	}
	
	/**
	 * 解压gz文件，并返回解压后的文件
	 */
	public static File unGZip(File file) throws IOException {
		int buffer = 2048;

		String savepath = null;
		String path = file.getAbsolutePath();
		String name = file.getName();
		String outname = name.substring(0, name.lastIndexOf("."));
		savepath = path.substring(0, path.lastIndexOf("\\")) + "\\";
		
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
	 * @throws IOException 
	 */
	public static List<File> unZip(File file) throws IOException {
		int count = -1;
		int index = -1;
		int buffer = 2048;

		String savepath = "";
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
	 * 替换xml文件中不规范的字符，并重新生成xml文件
	 */
	public static File formatXML(File xml) throws IOException {
		StringBuffer sb = new StringBuffer();
		char[] fbyte = new char[1024 * 1024];
		FileInputStream fins = new FileInputStream(xml);
		InputStreamReader fr = new InputStreamReader(fins, "UTF-8");
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
		//过滤非法Unicode字符
		String outStr=sb.toString().trim().replaceAll("[\\x00-\\x08\\x0b-\\x0c\\x0e-\\x1f]", "");

		int xmlstart=outStr.indexOf("<?xml");
		outStr=outStr.substring(xmlstart);
		fout.write(outStr.getBytes("UTF-8"));
		fout.close();
		return new File(newfilepath);
	}
}
