package cfg;

/**
 * ʵ���Զ������csv�ַ�����ת��
 * **/
public interface CsvFormater {
	
	/**
	 * ��Ŀ�����ת����csv�ַ���(�ö��ŷָ����ַ���)
	 * @param o Ŀ�����
	 * @return
	 */
	String toCsvString(Object o);
}
