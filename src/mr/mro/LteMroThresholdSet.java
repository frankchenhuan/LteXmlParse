package mr.mro;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import mr.mro.datasource.DataSource;
/**
 * 预加载MRO处理的各种门限值
 * @author Administrator
 */
public class LteMroThresholdSet {
	/**
	 * 过覆盖相关性筛选门限(%)≥,1
	 */
	public static double RELATIVITY_PERCENT;
	/**
	 * 过覆盖站间距倍数筛选门限≥,1.5
	 */
	public static double DISTANCE_THRESHOLD;
	/**
	 * 过覆盖电平差值门限(dB),6
	 */
	public static double DBM_DIFF_THRESHOLD1;
	/**
	 * 过覆盖小区个数筛选门限≥,5
	 */
	public static double OVER_COVER_THRESHOLD;
	/**
	 * 过覆盖主小区电平值门限≥,-110
	 */
	public static double OVER_COVER_N_POWER;
	
	/**
	 * 重叠覆盖电平差值门限(dB)≤,10
	 */
	public static double DBM_DIFF_THRESHOLD2;
	/**
	 * 重叠覆盖邻小区强度(dBm)≥,-110
	 */
	public static double NEIGHBOR_CELL_DBM;
	/**
	 * 高重叠覆盖筛选门限≥,0.3
	 */
	public static double OVER_LAYING_THRESHOLD;
	/**
	 * 重叠覆盖邻区个数筛选门限≥,3
	 */
	public static double OVER_LAYING_NC_NUM;
	/**
	 * 二维四象限分析RSRP门限
	 */
	public static double TWO_FOUR_RSRP;
	/**
	 * 二维四象限分析RSRQ门限
	 */
	public static double TWO_FOUR_RSRQ;
	/**
	 * 二维四象限分析RTTD门限
	 */
	public static double TWO_FOUR_RTTD;
	/**
	 * 二维四象限分析SINRU门限
	 */
	public static double TWO_FOUR_SINRU;
	
	
	static {
		getThresholdMap();
	}
	
	private static void getThresholdMap(){
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		String sql = "select i_module,i_id,i_value,i_defult_value from lte_etl.t_lte_mr_threshold_set";
		try {
			DataSource.init();
			conn = DataSource.getConnection();
			stmt = conn.createStatement();
			rs = stmt.executeQuery(sql);
			if(rs!=null){
				while(rs.next()){
					int i_module = rs.getInt("i_module");
					int i_id = rs.getInt("i_id");
					String i_value = rs.getString("i_value")==null?"":rs.getString("i_value");
					String i_defult_value = rs.getString("i_defult_value");
					String value = "".equals(i_value)?i_defult_value:i_value;
					double d = Double.parseDouble(value);
					switch(i_module){
						case 1:switch(i_id){
							case 1 : LteMroThresholdSet.RELATIVITY_PERCENT = d;
							case 2 : LteMroThresholdSet.DISTANCE_THRESHOLD = d;
							case 3 : LteMroThresholdSet.DBM_DIFF_THRESHOLD1 = d;
							case 4 : LteMroThresholdSet.OVER_COVER_THRESHOLD = d;
							case 5 : LteMroThresholdSet.OVER_COVER_N_POWER = d;
						}
						case 2:switch(i_id){
							case 1 : LteMroThresholdSet.DBM_DIFF_THRESHOLD2 = d;
							case 2 : LteMroThresholdSet.NEIGHBOR_CELL_DBM = d;
							case 3 : LteMroThresholdSet.OVER_LAYING_THRESHOLD = d;
							case 4 : LteMroThresholdSet.OVER_LAYING_NC_NUM = d;
						}
						case 3:switch(i_id){
							case 1 : LteMroThresholdSet.TWO_FOUR_RSRP = d;
							case 2 : LteMroThresholdSet.TWO_FOUR_RSRQ = d;
							case 3 : LteMroThresholdSet.TWO_FOUR_RTTD = d;
							case 4 : LteMroThresholdSet.TWO_FOUR_SINRU = d;
						}
					}
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally {
			try {
				rs.close();
				stmt.close();
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}finally{
				rs = null;
				stmt=null;
				conn=null;
			}
		}
	}
	
}
