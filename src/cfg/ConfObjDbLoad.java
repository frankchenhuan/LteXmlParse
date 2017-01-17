package cfg;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ConfObjDbLoad {
	String object_table;
	String field_table;
	String subtable_field;
	String subtable_target;
	String object_table_sql;
	String field_table_sql;
	String subtable_field_sql;
	String subtable_target_sql;

	Connection con;
	
	protected final Log log = LogFactory.getLog(this.getClass());

	public ConfObjDbLoad() {
		object_table = Config.getValue("object_table");
		field_table = Config.getValue("field_table");
		subtable_field = Config.getValue("subtable_field");
		subtable_target = Config.getValue("subtable_target");
		this.object_table_sql = "SELECT ID,T.OBJECT_TYPE,T.TABLENAME,T.SUB_TABLENAME"
				+ ",T.TARGETNAMEFIELD_DATATYPE,T.TARGETNAMEFIELD_LENGTH,T.TARGETNAMEFIELD_TABLECOLUMN,T.TARGETNAMEFIELD_FORMAT,T.TARGETNAMEFIELD_REGEX"
				+ ",T.TARGETVALUEFIELD_DATATYPE,T.TARGETVALUEFIELD_LENGTH,T.TARGETVALUEFIELD_TABLECOLUMN,T.TARGETVALUEFIELD_FORMAT,T.TARGETVALUEFIELD_REGEX "
				+ ",T.SUBTARGETNAMEFIELD_DATATYPE,T.SUBTARGETNAMEFIELD_LENGTH,T.SUBTARGETNAMEFIELD_TABLECOLUMN,T.SUBTARGETNAMEFIELD_FORMAT,T.SUBTARGETNAMEFIELD_REGEX"
				+ ",T.TEMP_TABLENAME,T.TEMP_SUB_TABLENAME "
				+ " FROM "
				+ object_table + " T WHERE T.CONFIG_NAME=?";
		this.field_table_sql = "SELECT OBJECT_ID, DATATYPE, TABLECOLUMN, NAME, TARGETNAME, FORMAT, LENGTH, TEXT, REGEX FROM "
				+ field_table + "  WHERE OBJECT_ID=?";
		this.subtable_field_sql = "SELECT OBJECT_ID, DATATYPE, TABLECOLUMN, NAME, TARGETNAME, FORMAT, LENGTH, TEXT, REGEX  FROM "
				+ subtable_field + " WHERE OBJECT_ID=?";
		this.subtable_target_sql = " SELECT OBJECT_ID, NAME, TARGETNAME, TEXT FROM "
				+ subtable_target + " WHERE OBJECT_ID=?";
	}

	public List<Map> getObjects(String confname) throws SQLException {
		log.debug("OBJECT_TABLE_SQL="+object_table_sql);
		log.debug("CONFIG_NAME="+confname);
		con = DataSource.getConnection();
		PreparedStatement ps = con.prepareStatement(object_table_sql);
		List<Map> os = null;
		
		ps.setString(1, confname);
		ResultSet rs = ps.executeQuery();
		os = this.getValues(rs);
		rs.close();
		ps.close();
		con.close();
		return os;
	}

	private List<Map> getValues(ResultSet rs) throws SQLException {
		List vs = new ArrayList<Map>();
		ResultSetMetaData rsmd = rs.getMetaData();
		int cn = rsmd.getColumnCount();
		while (rs.next()) {
			Map<String, String> values = new HashMap<String, String>();
			for (int i = 0; i < cn; i++) {
				String name = rsmd.getColumnName(i + 1);
				values.put(name, rs.getString(name));
			}
			vs.add(values);
		}
		return vs;
	}

	public List<Map> getFields(int object_id) throws SQLException {
		log.debug("FIELD_TABLE_SQL="+field_table_sql);
		log.debug("OBJECT_ID="+object_id);
		con = DataSource.getConnection();
		PreparedStatement ps = con.prepareStatement(this.field_table_sql);
		List<Map> os = null;
		ps.setInt(1, object_id);
		ResultSet rs = ps.executeQuery();
		os = this.getValues(rs);
		rs.close();
		ps.close();
		con.close();
		return os;
	}

	public List<Map> getSubFields(int object_id) throws SQLException {
		log.debug("SUBTABLE_FIELD_SQL="+subtable_field_sql);
		log.debug("OBJECT_ID="+object_id);
		con = DataSource.getConnection();
		PreparedStatement ps = con.prepareStatement(this.subtable_field_sql);
		List<Map> os = null;
		ps.setInt(1, object_id);
		ResultSet rs = ps.executeQuery();
		os = this.getValues(rs);
		rs.close();
		ps.close();
		con.close();
		return os;
	}

	public List<Map> getSubTargets(int object_id) throws SQLException {
		log.debug("SUBTABLE_TARGET_SQL="+subtable_target_sql);
		log.debug("OBJECT_ID="+object_id);
		con = DataSource.getConnection();
		PreparedStatement ps = con.prepareStatement(this.subtable_target_sql);
		List<Map> os = null;
		ps.setInt(1, object_id);
		ResultSet rs = ps.executeQuery();
		os = this.getValues(rs);
		rs.close();
		ps.close();
		con.close();
		return os;
	}

	public static void main(String a[]) {
		Config.init(2);
		try {
			DataSource.init();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		ConfObjDbLoad c = new ConfObjDbLoad();
		try {
			List l = c.getObjects("PM_CONF");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			DataSource.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
