package cfg;

import java.util.List;

public interface Page {
	/**
	 * �õ����ҳ��
	 * 
	 * @return
	 */
	int getMaxPage();// �õ����ҳ��

	/**
	 * �õ���ǰҳ��
	 * 
	 * @return
	 */
	int getCurPage();// �õ���ǰҳ��

	/**
	 * ���õ�ǰҳ
	 * 
	 * @param page
	 */
	void setCurPage(int page);// ���õ�ǰҳ

	/**
	 * �õ���������
	 * 
	 * @return
	 */
	int getMaxRow();// �õ���������

	/**
	 * �����������
	 * 
	 * @param rows
	 */
	void setMaxRow(int rows);// �����������

	/**
	 * �õ�ÿҳ����
	 * 
	 * @return
	 */
	int getPageRow();// �õ�ÿҳ����

	/**
	 * ����ÿҳ����
	 * 
	 * @param rows
	 */
	void setPageRow(int rows);// ����ÿҳ����

	/**
	 * �õ�ָ��ҳ����
	 * 
	 * @param page
	 * @return
	 */
	List getPageData(int page);// �õ�ָ��ҳ����

	/**
	 * �õ���ǰҳ����
	 * 
	 * @return
	 */
	List current();// �õ���ǰҳ����

	/**
	 * �Ƿ������һҳ
	 * 
	 * @return
	 */
	boolean hasNext();// �Ƿ������һҳ

	/**
	 * ��һҳ����
	 * 
	 * @return
	 */
	List next();// ��һҳ����

	/**
	 * ��һҳ����
	 * 
	 * @return
	 */
	List previous();// ��һҳ����
}
