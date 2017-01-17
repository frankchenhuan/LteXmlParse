package pm;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import cfg.ConfObj;

public class PmDataObj {

	private ConfObj pmConfObj;

	private Map<String, Map> subTargetValues = new ConcurrentHashMap<String, Map>();

	private Map<String, String> values = new ConcurrentHashMap<String, String>();

	public void addValue(String key, String value) {
		values.put(key, value);
	}

	public Map<String, Map> getSubTargetValues() {
		return subTargetValues;
	}

	public Map<String, String> getValues() {
		return values;
	}

	public void addSubTargetValue(String key, Map values) {
		subTargetValues.put(key, values);
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
