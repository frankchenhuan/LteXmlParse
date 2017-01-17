package cfg;

import java.io.IOException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * csv��ʽ�����
 * 
 * @author �»�
 * 
 */
public abstract class CsvOutPrint {

	private static Pattern p1 = Pattern.compile("[\r\n]");

	private static Pattern p2 = Pattern.compile("[\"]");

	private static Pattern p3 = Pattern.compile("[,]");

	private static Pattern p4 = Pattern.compile("^\".*\"$");
	private static Pattern p5 = Pattern.compile(".*,.*");

	public abstract void flush();

	public abstract void close();

	/**
	 * д��������
	 * 
	 * @param data
	 *            ��Ҫ��д������ݣ���һάΪ�У��ڶ�άΪ��
	 * @param flush
	 *            д����ɺ��Ƿ�ˢ�»�����
	 * @throws IOException
	 * @throws NullPointerException
	 */
	public void write(String[][] data, boolean flush)
			throws NullPointerException, IOException {
		if (data == null)
			throw new NullPointerException("String[][] data is null!");
		for (int i = 0; i < data.length; i++) {
			String wdata = format(data[i]);
			write(wdata);
		}
		if (flush)
			flush();
	}

	/***************************************************************************
	 * д��һ������
	 * 
	 * @param data
	 *            ��Ҫд������ݣ�ûһ������λ�����һ��Ԫ��
	 * @param flush
	 *            д����ɺ��Ƿ�ˢ�»�����
	 * @throws IOException
	 * @throws NullPointerException
	 **************************************************************************/
	public void write(String[] data, boolean flush)
			throws NullPointerException, IOException {
		if (data == null)
			throw new NullPointerException("String[] data is null!");
		String wdata = format(data);
		write(wdata);
		if (flush)
			flush();
	}

	/**
	 * д��������
	 * 
	 * @param data
	 *            ����,List������ΪString[]����,String[]�д��ÿһ�е�����
	 * @param flush
	 *            д����ɺ��Ƿ�ˢ�»�����
	 * @throws IOException
	 * @throws NullPointerException
	 */
	public void write(List<String> data, boolean flush) throws IOException {
		if (data == null)
			return;
		String wdata = format(data);
		write(wdata);
		if (flush)
			flush();
	}

	/***************************************************************************
	 * ���Զ���ת������ת��csv��ʽ
	 * 
	 * @param data
	 *            ��Ҫ���������List
	 * @param formater
	 *            �Զ���CsvFormater��������ת������csv��ʽ
	 * @param flush
	 *            д����ɺ��Ƿ�ˢ�»�����
	 * @throws IOException
	 * @throws NullPointerException
	 **************************************************************************/
	public void write(List data, CsvFormater formater, boolean flush)
			throws NullPointerException, IOException {
		if (data == null)
			throw new NullPointerException("List data is null!");
		for (int i = 0; i < data.size(); i++) {
			String wdata = formater.toCsvString(data.get(i));
			write(wdata);
		}
		if (flush)
			flush();
	}

	/***************************************************************************
	 * ���ַ�����ת����csv��ʽ
	 * 
	 * @param data
	 *            Ҫת��������
	 * @return String
	 **************************************************************************/
	public String format(String[] data) throws NullPointerException {
		if (data == null)
			throw new NullPointerException("String[] data is null!");
		StringBuffer sb = new StringBuffer("");
		for (int i = 0; i < data.length; i++) {
			String s = data[i];
			if (s != null) {
				/** �ж�һ���ַ����Ƿ�������Ų����Ƿ������ſ�ͷ�ͽ�β */
				Matcher m1 = p4.matcher(s);
				Matcher m2 = p5.matcher(s);

				// ����ַ����������ſ�ͷ�ͽ�β�����Ұ�������
				if (!m1.find() && m2.find()) {
					s = "\"" + s + "\"";
					//System.out.println(s);
				}
			}
			sb.append(s + ",");
		}
		/*
		 * if (sb.length() != 0) sb.setLength(sb.length() - 1);
		 */
		return sb.toString();
	}

	/***************************************************************************
	 * ��Listת����csv��ʽ
	 * 
	 * @param data
	 *            Ҫת��������
	 * @return String
	 **************************************************************************/
	public String format(List<String> data) throws NullPointerException {
		if (data == null)
			throw new NullPointerException("Data list is null!");
		StringBuffer sb = new StringBuffer("");
		for (int i = 0; i < data.size(); i++) {

			String s = data.get(i);
			if (s != null) {
				/** �ж�һ���ַ����Ƿ�������Ų����Ƿ������ſ�ͷ�ͽ�β */
				Matcher m1 = p4.matcher(s);
				Matcher m2 = p5.matcher(s);

				// ����ַ����������ſ�ͷ�ͽ�β�����Ұ�������
				if (!m1.find() && m2.find()) {
					s = "\"" + s + "\"";
					//System.out.println(s);
				}
			}

			sb.append(s + ",");
		}
		/*
		 * if (sb.length() != 0) sb.setLength(sb.length() - 1);
		 */
		return sb.toString();
	}

	/***************************************************************************
	 * �滻csv��ʽ�����ַ�
	 * 
	 * @param src
	 *            ���滻���ַ���
	 * @return String
	 **************************************************************************/
	public static String replaceAll(String src) {
		if (src == null)
			return null;
		Matcher m = p1.matcher(src);
		m = p2.matcher(m.replaceAll(" "));
		// m = p3.matcher(m.replaceAll("'"));
		return m.replaceAll("");
	}

	/***************************************************************************
	 * д��һ������
	 * 
	 * @param data
	 *            Ҫд��������ַ���(��Ҫд���csv�ַ���)
	 * @throws IOException,NullPointerException
	 **************************************************************************/
	abstract public void write(String data) throws IOException,
			NullPointerException;

	/***************************************************************************
	 * ����ָ���ķ�ҳPage�����ѯ������
	 * 
	 * @param page
	 *            Ҫд���page����
	 * @param formater
	 *            csv��ʽת����
	 * @param rows
	 *            ÿ�ε���������
	 * @param startpage
	 *            ��ָ��ҳ����
	 * @param flash
	 *            ÿ�ε���ָ���������Ƿ��Զ�ˢ�»���
	 * @throws IOException
	 * @throws NullPointerException
	 */
	public void writePage(Page page, CsvFormater formater, int rows,
			int startpage, boolean flush) throws NullPointerException,
			IOException {
		page.setPageRow(rows);
		page.setCurPage(startpage - 1);
		while (page.hasNext()) {
			List data = page.next();
			this.write(data, formater, flush);
		}
	}
}
