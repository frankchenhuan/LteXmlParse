package cfg;

/**
 * 实现自定义对象到csv字符串的转换
 * **/
public interface CsvFormater {
	
	/**
	 * 将目标对象转换成csv字符串(用逗号分隔的字符串)
	 * @param o 目标对象
	 * @return
	 */
	String toCsvString(Object o);
}
