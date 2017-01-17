package cm;

import java.util.List;

import cfg.ConfObj;
import cfg.Field;

public class CfgXmlObj extends ConfObj {


	

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

	public String getTablename() {
		return tablename;
	}

	public void setTablename(String tablename) {
		this.tablename = tablename;
	}
}
