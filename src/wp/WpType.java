package wp;

import java.util.ArrayList;
import java.util.List;


public class WpType {

	String typeName; 

	/** ���ݿ����� */
	List<String> cols = new ArrayList<String>();

	/** ָ���� */
	List<String> targets = new ArrayList<String>();

	/**
	 * ����ָ��
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
