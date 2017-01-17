package pm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cfg.Field;

public class PmSubConfObj {
	private String tablename;
	private Field targetNameField;
	private Field subTargetNameField;
	private Field targetValueField;
	private String temp_tablename;

	private int num = 0;

	public void addNum() {
		num++;
	}

	public int getNum() {
		return num;
	}

	private List<Field> field = new ArrayList<Field>();

	public String getTablename() {
		return tablename;
	}

	public void setTablename(String tablename) {
		this.tablename = tablename;
	}

	public String getSql() {
		String sql;
		sql = "insert into " + this.tablename + "(";
		int size = field != null ? field.size() : 0;
		for (int i = 0; i < size; i++) {
			Field f = field.get(i);
			sql += f.getTablecolumn();
			sql += ",";
		}
		sql += targetNameField.getTablecolumn() + ","
				+ subTargetNameField.getTablecolumn() + ","
				+ targetValueField.getTablecolumn() + ")";
		sql += " values(";
		for (int i = 0; i < size; i++) {
			sql += "?,";
		}
		sql += "?,?,?)";

		// 从新设置索引
		this.initIndex();

		return sql;
	}

	public String getSql(int type) {

		String tb = null;
		if (type == 1) {
			tb = this.tablename;
		} else if (type == 2) {
			tb = this.temp_tablename;
		} else {
			tb = this.tablename;
		}
		String sql;
		sql = "insert into " + tb + "(";
		int size = field != null ? field.size() : 0;
		for (int i = 0; i < size; i++) {
			Field f = field.get(i);
			sql += f.getTablecolumn();
			sql += ",";
		}
		sql += targetNameField.getTablecolumn() + ","
				+ subTargetNameField.getTablecolumn() + ","
				+ targetValueField.getTablecolumn() + ")";
		sql += " values(";
		for (int i = 0; i < size; i++) {
			sql += "?,";
		}
		sql += "?,?,?)";

		// 从新设置索引
		this.initIndex();

		return sql;
	}

	public void initIndex() {
		/** 重设三个列对象的索引 */
		if (field != null) {
			targetNameField.setIndex(field.size() + 1);
			subTargetNameField.setIndex(field.size() + 2);
			targetValueField.setIndex(field.size() + 3);
		} else {
			targetNameField.setIndex(1);
			subTargetNameField.setIndex(2);
			targetValueField.setIndex(3);
		}
	}

	public Field getTargetNameField() {
		return targetNameField;
	}

	public void setTargetNameField(Field targetNameField) {
		this.targetNameField = targetNameField;
	}

	public Field getSubTargetNameField() {
		return subTargetNameField;
	}

	public void setSubTargetNameField(Field subTargetNameField) {
		this.subTargetNameField = subTargetNameField;
	}

	public List<Field> getFields() {
		return field;
	}

	public void setFields(List<Field> field) {
		this.field = field;
	}

	public Field getTargetValueField() {
		return targetValueField;
	}

	public void setTargetValueField(Field targetValueField) {
		this.targetValueField = targetValueField;
	}

	public String getTemp_tablename() {
		return temp_tablename;
	}

	public void setTemp_tablename(String temp_tablename) {
		this.temp_tablename = temp_tablename;
	}

}
