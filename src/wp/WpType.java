package wp;

import java.util.ArrayList;
import java.util.List;


public class WpType {

	String typeName; 

	/** 数据库列名 */
	List<String> cols = new ArrayList<String>();

	/** 指标名 */
	List<String> targets = new ArrayList<String>();

	/**
	 * 加入指标
	 * */
	public void add(String target,String col) {
		cols.add(col);
		targets.add(target);
	}

	public String getTypeName() {
		return typeName;
	}

	public void setTypeName(String typeName) {
		this.typeName = typeName;
	}

	public List<String> getCols() {
		return cols;
	}

	public List<String> getTargets() {
		return targets;
	}
	
	
}
