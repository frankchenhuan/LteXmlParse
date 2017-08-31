package pm;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import cfg.ConfObj;

public class PmDataObj {

	private ConfObj pmConfObj;

	private Map<String, Map> subTargetValues = new ConcurrentHashMap<String, Map>();

	private Map<String, String> values = new ConcurrentHashMap<String, String>();

	public synchronized void addValue(String key, String value) {
		String v = values.get(key);
		if (v == null || v.trim().equals("")) {
			values.put(key, value);
		}
	}

	public Map<String, Map> getSubTargetValues() {
		return subTargetValues;
	}

	public Map<String, String> getValues() {
		return values;
	}

	public synchronized void addSubTargetValue(String key, Map vmap) {
		Map v = subTargetValues.get(key);
		if (v == null) {
			subTargetValues.put(key, vmap);
		}
	}

	public Map getSubTargetValues(String key) {
		return subTargetValues.get(key);
	}

	@Override
	public String toString() {
		return this.pmConfObj.getObjectType();
	}

	public ConfObj getPmConfObj() {
		return pmConfObj;
	}

	public void setPmConfObj(ConfObj pmConfObj) {
		this.pmConfObj = pmConfObj;
	}
}
