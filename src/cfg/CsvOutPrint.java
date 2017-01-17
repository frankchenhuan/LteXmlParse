package cfg;

import java.io.IOException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * csv格式的输出
 * 
 * @author 陈焕
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
	 * 写多行数据
	 * 
	 * @param data
	 *            需要第写入的数据，第一维为行，第二维为列
	 * @param flush
	 *            写入完成后是否刷新缓冲区
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
	 * 写入一行数据
	 * 
	 * @param data
	 *            需要写入的数据，没一列数据位数组的一个元素
	 * @param flush
	 *            写入完成后是否刷新缓冲区
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
	 * 写多行数据
	 * 
	 * @param data
	 *            数据,List中类型为String[]类型,String[]中存放每一列的数据
	 * @param flush
	 *            写入完成后是否刷新缓冲区
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
	 * 用自定义转器进行转换csv格式
	 * 
	 * @param data
	 *            需要导入的数据List
	 * @param formater
	 *            自定义CsvFormater对象，用于转换对象到csv格式
	 * @param flush
	 *            写入完成后是否刷新缓冲区
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
	 * 将字符数组转换成csv格式
	 * 
	 * @param data
	 *            要转换的数据
	 * @return String
	 **************************************************************************/
	public String format(String[] data) throws NullPointerException {
		if (data == null)
			throw new NullPointerException("String[] data is null!");
		StringBuffer sb = new StringBuffer("");
		for (int i = 0; i < data.length; i++) {
			String s = data[i];
			if (s != null) {
				/** 判断一个字符串是否包含逗号并且是否以引号开头和结尾 */
				Matcher m1 = p4.matcher(s);
				Matcher m2 = p5.matcher(s);

				// 如果字符串不以引号开头和结尾，并且包含逗号
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
	 * 将List转换成csv格式
	 * 
	 * @param data
	 *            要转换的数据
	 * @return String
	 **************************************************************************/
	public String format(List<String> data) throws NullPointerException {
		if (data == null)
			throw new NullPointerException("Data list is null!");
		StringBuffer sb = new StringBuffer("");
		for (int i = 0; i < data.size(); i++) {

			String s = data.get(i);
			if (s != null) {
				/** 判断一个字符串是否包含逗号并且是否以引号开头和结尾 */
				Matcher m1 = p4.matcher(s);
				Matcher m2 = p5.matcher(s);

				// 如果字符串不以引号开头和结尾，并且包含逗号
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
	 * 替换csv格式特殊字符
	 * 
	 * @param src
	 *            被替换的字符串
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
	 * 写入一行数据
	 * 
	 * @param data
	 *            要写入的数据字符串(需要写入的csv字符串)
	 * @throws IOException,NullPointerException
	 **************************************************************************/
	abstract public void write(String data) throws IOException,
			NullPointerException;

	/***************************************************************************
	 * 导出指定的分页Page对象查询的数据
	 * 
	 * @param page
	 *            要写入的page对象
	 * @param formater
	 *            csv格式转换器
	 * @param rows
	 *            每次导出的行数
	 * @param startpage
	 *            从指定页导出
	 * @param flash
	 *            每次导出指定行数后是否自动刷新缓存
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
