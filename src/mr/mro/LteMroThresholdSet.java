package mr.mro;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import mr.mro.datasource.DataSource;
/**
 * Ԥ����MRO����ĸ�������ֵ
 * @author Administrator
 */
public class LteMroThresholdSet {
	/**
	 * �����������ɸѡ����(%)��,1
	 */
	public static double RELATIVITY_PERCENT;
	/**
	 * ������վ��౶��ɸѡ���ޡ�,1.5
	 */
	public static double DISTANCE_THRESHOLD;
	/**
	 * �����ǵ�ƽ��ֵ����(dB),6
	 */
	public static double DBM_DIFF_THRESHOLD1;
	/**
	 * ������С������ɸѡ���ޡ�,5
	 */
	public static double OVER_COVER_THRESHOLD;
	/**
	 * ��������С����ƽֵ���ޡ�,-110
	 */
	public static double OVER_COVER_N_POWER;
	
	/**
	 * �ص����ǵ�ƽ��ֵ����(dB)��,10
	 */
	public static double DBM_DIFF_THRESHOLD2;
	/**
	 * �ص�������С��ǿ��(dBm)��,-110
	 */
	public static double NEIGHBOR_CELL_DBM;
	/**
	 * ���ص�����ɸѡ���ޡ�,0.3
	 */
	public static double OVER_LAYING_THRESHOLD;
	/**
	 * �ص�������������ɸѡ���ޡ�,3
	 */
	public static double OVER_LAYING_NC_NUM;
	/**
	 * ��ά�����޷���RSRP����
	 */
	public static double TWO_FOUR_RSRP;
	/**
	 * ��ά�����޷���RSRQ����
	 */
	public static double TWO_FOUR_RSRQ;
	/**
	 * ��ά�����޷���RTTD����
	 */
	public static double TWO_FOUR_RTTD;
	/**
	 * ��ά�����޷���SINRU����
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
