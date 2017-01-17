package cfg;

import java.util.List;

public interface Page {
	/**
	 * 得到最大页数
	 * 
	 * @return
	 */
	int getMaxPage();// 得到最大页数

	/**
	 * 得到当前页数
	 * 
	 * @return
	 */
	int getCurPage();// 得到当前页数

	/**
	 * 设置当前页
	 * 
	 * @param page
	 */
	void setCurPage(int page);// 设置当前页

	/**
	 * 得到数据行数
	 * 
	 * @return
	 */
	int getMaxRow();// 得到数据行数

	/**
	 * 设置最大行数
	 * 
	 * @param rows
	 */
	void setMaxRow(int rows);// 设置最大行数

	/**
	 * 得到每页行数
	 * 
	 * @return
	 */
	int getPageRow();// 得到每页行数

	/**
	 * 设置每页行数
	 * 
	 * @param rows
	 */
	void setPageRow(int rows);// 设置每页行数

	/**
	 * 得到指定页数据
	 * 
	 * @param page
	 * @return
	 */
	List getPageData(int page);// 得到指定页数据

	/**
	 * 得到当前页数据
	 * 
	 * @return
	 */
	List current();// 得到当前页数据

	/**
	 * 是否存在下一页
	 * 
	 * @return
	 */
	boolean hasNext();// 是否存在下一页

	/**
	 * 下一页数据
	 * 
	 * @return
	 */
	List next();// 下一页数据

	/**
	 * 上一页数据
	 * 
	 * @return
	 */
	List previous();// 上一页数据
}
