package cfg;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import pm.PmSubConfObj;

public class ConfObj {
	protected String objectType;
	protected String tablename;

	protected String temp_tablename;
	// private String sql;
	protected List<Field> fields;

	protected Map<String, String> targetsName;
	protected int num = 0;

	protected PmSubConfObj subConf;

	protected Map<String, String> subTargets = new HashMap<String, String>();

	public String getSql() {
		String sql;
		sql = "insert into " + this.tablename + "(";
		int size = fields.size();
		for (int i = 0; i < size; i++) {
			Field f = fields.get(i);
			sql += f.getTablecolumn();
			if (i < size - 1) {
				sql += ",";
			} else {
				sql += ")";
			}
		}
		sql += " values(";
		for (int i = 0; i < size; i++) {
			if (i < size - 1) {
				sql += "?,";
			} else {
				sql += "?)";
			}
		}
		return sql;
	}

	public String getSql(int type) {

		String sql;
		String tb;
		if (type == 1) {
			tb = this.tablename;
		} else if (type == 2) {
			tb = this.temp_tablename;
		}else
		{
			tb=this.tablename;
		}
		sql = "insert into " + tb + "(";
		int size = fields.size();
		for (int i = 0; i < size; i++) {
			Field f = fields.get(i);
			sql += f.getTablecolumn();
			if (i < size - 1) {
				sql += ",";
			} else {
				sql += ")";
			}
		}
		sql += " values(";
		for (int i = 0; i < size; i++) {
			if (i < size - 1) {
				sql += "?,";
			} else {
				sql += "?)";
			}
		}
		return sql;
	}

	public PmSubConfObj getSubConf() {
		return subConf;
	}

	public void setSubConf(PmSubConfObj subConf) {
		this.subConf = subConf;
	}

	public Map<String, String> getSubTargets() {
		return subTargets;
	}

	public void setSubTargets(Map<String, String> subTargets) {
		this.subTargets = subTargets;
	}

	public void addSubTarget(String targetKey, String targetValue) {
		subTargets.put(targetKey, targetValue);
	}

	public boolean isExistsSubTarget(String key) {
		return subTargets.containsKey(key);
	}

	public boolean isExistsTarget(String key) {
		return targetsName.containsKey(key);
	}

	public String getSubTarget(String key) {
		return this.subTargets.get(key);
	}

	/**
	 * 判断是否存在子指标
	 */
	public boolean isExistsSub() {
		if (subConf != null) {
			return true;
		} else {
			return false;
		}
	}

	public void addNum() {
		num++;
	}

	public int getNum() {
		return num;
	}

	public String getObjectType() {
		return objectType;
	}

	public void setObjectType(String objectType) {
		this.objectType = objectType;
	}

	public List<Field> getFields() {
		return fields;
	}

	public void setFileds(List<Field> fileds) {
		this.fields = fileds;
	}

	public String getTablename() {
		return tablename;
	}

	public void setTablename(String tablename) {
		this.tablename = tablename;
	}
	

	// public abstract String getSql();

	public Map<String, String> getTargetsName() {
		return targetsName;
	}

	public void setTargetsName(Map<String, String> targetsName) {
		this.targetsName = targetsName;
	}

	public void setFields(List<Field> fields) {
		this.fields = fields;
	}

	public String getTemp_tablename() {
		return temp_tablename;
	}

	public void setTemp_tablename(String temp_tablename) {
		this.temp_tablename = temp_tablename;
	}
}
