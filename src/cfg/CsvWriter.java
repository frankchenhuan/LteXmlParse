package cfg;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * ����csv��ʽ�����ݵ������ļ���
 * 
 * @author �»�
 * 
 */
public class CsvWriter extends CsvOutPrint {

	private OutputStream out = null;

	/**
	 * ���췽��
	 * 
	 * @param out �����
	 */
	public CsvWriter(OutputStream out) {
		this.out = out;
	}

	/**
	 * ���췽��
	 * 
	 * @param path ������ļ�·��
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
	 * ���췽��
	 * 
	 * @param file ������ļ�
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
	 * д��һ������
	 * 
	 * @param data Ҫд��������ַ���(csv��ʽ���ַ���)
	 * @throws IOException,NullPointerException
	 **************************************************************************/
	public void write(String data) throws IOException, NullPointerException {
		if (out == null)
			throw new NullPointerException("OutputStream is null!");
		out.write((data + "\r\n").getBytes());
	}

}
