package cfg;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * 导出csv格式的数据到流或文件中
 * 
 * @author 陈焕
 * 
 */
public class CsvWriter extends CsvOutPrint {

	private OutputStream out = null;

	/**
	 * 构造方法
	 * 
	 * @param out 输出流
	 */
	public CsvWriter(OutputStream out) {
		this.out = out;
	}

	/**
	 * 构造方法
	 * 
	 * @param path 输出的文件路径
	 */
	public CsvWriter(String path) {
		try {
			out = new FileOutputStream(path);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			out = null;
		}
	}

	/**
	 * 构造方法
	 * 
	 * @param file 输出的文件
	 */
	public CsvWriter(File file) {
		try {
			out = new FileOutputStream(file);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			out = null;
		}
	}

	public void flush() {
		try {
			out.flush();
		} catch (Exception e) {
		}
	}

	public void close() {
		try {
			out.close();
		} catch (Exception e) {
		}
		out = null;
	}

	/***************************************************************************
	 * 写入一行数据
	 * 
	 * @param data 要写入的数据字符串(csv格式的字符串)
	 * @throws IOException,NullPointerException
	 **************************************************************************/
	public void write(String data) throws IOException, NullPointerException {
		if (out == null)
			throw new NullPointerException("OutputStream is null!");
		out.write((data + "\r\n").getBytes());
	}

}
