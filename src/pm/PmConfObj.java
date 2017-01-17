package pm;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import cfg.ConfObj;
import cfg.Config;
import cfg.Field;

public class PmConfObj extends ConfObj {

	protected PmSubConfObj subConf;

	protected Map<String, String> subTargets = new ConcurrentHashMap<String, String>();

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
}
