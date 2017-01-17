package mr.mro;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import mr.mro.datasource.DataSource;
/**
 * 处理过覆盖小区对明细未定义的邻区处理类
 * 1.从过覆盖小区对表中读取未定义的邻区信息；
 * 2.根据频点+扰码，查询使用该频点+扰码的所有小区，计算距离，用距离最近的邻区更新该邻区的配置信息
 * @author wangyubo
 *
 */
public class OverCoverCoupleHandler {
	protected final Log log = LogFactory.getLog(OverCoverCoupleHandler.class);
	/**
	 * 数据库缓存提交数据
	 */
	private static final int COMMIT_COUNT = 2000;
	private static final int PAGE_COUNT = 10000;
	/**
	 * 数据日期
	 */
	private String dtm_repdate;
	
	private List<LteConf> updateList = new ArrayList<LteConf>();
	
	public OverCoverCoupleHandler(String dtm_repdate){
		this.dtm_repdate = dtm_repdate;
	}
	
	public static void main(String[] args){
		if(args.length<1){
			System.out.println("参数不足！");
			System.exit(0);
		}
		OverCoverCoupleHandler occ = new OverCoverCoupleHandler(args[0]);
		occ.handleNeibhborCells();
		occ.saveNeighborCell();
	}
	
	/**
	 * 查询没有邻区配置信息的小区们，调用处理方法逐个定位邻区
	 */
	public void handleNeibhborCells(){
		Connection conn = null;
		Statement stmt = null;
		ResultSet crs = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		String sql = "SELECT T.I_SUBCOMP_ID,T.I_ENODEB_ID,T.I_CELL_ID,T.I_LONGITUDE,T.I_LATITUDE,T.EARFCN,T.PCI " +
				"FROM LTE_ETL.T_LTE_MR_OCC_D T " +
				"WHERE DTM_REPDATE=DATE'"+dtm_repdate+"' AND I_ENODEB_ID1 IS NULL AND ROWNUM <= ?";
		String sql_count = "SELECT count(DTM_REPDATE) counts " +
				"FROM LTE_ETL.T_LTE_MR_OCC_D T " +
				"WHERE DTM_REPDATE=DATE'"+dtm_repdate+"' AND I_ENODEB_ID1 IS NULL";
		try {
			log.info("handleNeibhborCells 处理无配置邻区 开始执行...");
			DataSource.init();
			conn = DataSource.getConnection();
			stmt = conn.createStatement();
			crs = stmt.executeQuery(sql_count);
			int count = 0;
			if(crs!=null){
				while(crs.next()){
					count = crs.getInt("counts");
				}
			}
			int pages = count%PAGE_COUNT==0?count/PAGE_COUNT:(count/PAGE_COUNT) + 1;
			
			log.info("data number is :"+count+"; page size is :"+pages);
			
			for(int i=0;i<pages;i++){
				ps = conn.prepareStatement(sql);
				ps.setInt(1, PAGE_COUNT);
				rs = ps.executeQuery();
				LteConf lc = null;
				if(rs!=null){
					while(rs.next()){
						lc = new LteConf();
						lc.setI_subcomp_id(rs.getInt("i_subcomp_id"));
						lc.setI_enodeb_id(rs.getInt("i_enodeb_id"));
						lc.setI_cell_id(rs.getInt("i_cell_id"));
						lc.setEarfcn(rs.getInt("earfcn"));
						lc.setPci(rs.getInt("pci"));
						lc.setI_longitude(rs.getDouble("i_longitude"));
						lc.setI_latitude(rs.getDouble("i_latitude"));
						this.locationNeighborCell(lc);
					}
				}
			}
			
			log.info("handleNeibhborCells 处理无配置邻区 执行完成！");
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SQLException e) {
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
	
	/**
	 * 单个处理小区定位其邻区
	 * 1.查询使用频点+扰码的所有小区，通过计算站间距，确定距离最近的小区为其邻区；
	 * 2.将距离最近的小区配置更新进T_LTE_MR_OCC_D表中
	 * @param lc
	 */
	private void locationNeighborCell(LteConf lc){
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		String nb_sql = "SELECT T.I_SUBCOMP_ID,T.I_ENODEB_ID,T.I_CELL_ID,T.V_CELL_CNAME,T.V_CELL_ENAME," +
				"T.I_LONGITUDE,T.I_LATITUDE,T.I_SF,T.I_ANTENNA_HEIGHT,T.I_ANTENNA_DIRECTION,T.TOTAL_DOWNTILT " +
				"FROM T_LTE_CM_EUTRANCELLTDD_D_MR T " +
				"WHERE  EARFCN=? AND PCI=?";
		double DISTANCE = 1000000;
		LteConf lte = null;
		LteConf LTE = null;
		try {
			DataSource.init();
			conn = DataSource.getConnection();
			ps = conn.prepareStatement(nb_sql);
			ps.setInt(1, lc.getEarfcn());
			ps.setInt(2, lc.getPci());
//			ps.setInt(3, lc.getI_subcomp_id());
			rs = ps.executeQuery();
			if(rs!=null){
				while(rs.next()){
					lte = new LteConf();
					lte.setI_subcomp_id(lc.getI_subcomp_id());
					lte.setI_enodeb_id(lc.getI_enodeb_id());
					lte.setI_cell_id(lc.getI_cell_id());
					lte.setEarfcn(lc.getEarfcn());
					lte.setPci(lc.getPci());
					lte.setI_longitude(lc.getI_longitude());
					lte.setI_latitude(lc.getI_latitude());
					
					lte.setI_enodeb_id1(rs.getInt("i_enodeb_id"));
					lte.setI_cell_id1(rs.getInt("i_cell_id"));
					lte.setV_cell_cname1(rs.getString("v_cell_cname"));
					lte.setV_cell_ename1(rs.getString("v_cell_ename"));
					lte.setI_longitude1(rs.getDouble("i_longitude"));
					lte.setI_latitude1(rs.getDouble("i_latitude"));
					lte.setI_sf1(rs.getInt("i_sf"));
					lte.setI_antenna_height1(rs.getDouble("i_antenna_height"));
					lte.setI_antenna_direction1(rs.getInt("i_antenna_direction"));
					lte.setTotal_downtilt1(rs.getInt("total_downtilt"));
					
					double d = Tool.getDistatce(lte.getI_latitude(), lte.getI_latitude1(), lte.getI_longitude(), lte.getI_longitude1());
					lte.setN_distance(d);
					/**
					 * 冒泡法，找到距离最小的邻区
					 */
					if(d < DISTANCE){
						DISTANCE = d;
						LTE = lte;
					}
					
				}
				if(LTE != null){
					updateList.add(LTE);
					if(updateList.size()==COMMIT_COUNT){
						saveNeighborCell();
						updateList.clear();
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}finally {
			try {
				rs.close();
				ps.close();
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}finally{
				rs = null;
				ps=null;
				conn=null;
			}
		}
	}
	
	/**
	 * 保存邻小区的配置信息，将其更新入过覆盖小区对表中
	 */
	public void saveNeighborCell(){
		Connection conn = null;
		PreparedStatement ps = null;
		String update_sql = " UPDATE LTE_ETL.T_LTE_MR_OCC_D T SET T.I_ENODEB_ID1=?,T.I_CELL_ID1=?,T.V_CELL_CNAME1=?,T.V_CELL_ENAME1=?,T.I_LONGITUDE1=?," +
				"T.I_LATITUDE1=?,T.I_COVER_TYPE1=?,T.N_HEIGHT1=?,T.I_ANTENNA_DIRECTION1=?,T.TOTAL_DOWNTILT1=?,T.N_DISTANCE=? " +
				" WHERE T.I_ENODEB_ID=? AND T.I_CELL_ID=? AND T.EARFCN=? AND T.PCI=? AND T.DTM_REPDATE=DATE'"+dtm_repdate+"'";
		try {
			log.info("saveNeighborCell 保存邻区配置信息  开始执行...");
			DataSource.init();
			conn = DataSource.getConnection();
			ps = conn.prepareStatement(update_sql);
			int counts = 0;
			for(LteConf lte : updateList){
				ps.setInt(1, lte.getI_enodeb_id1());
				ps.setInt(2, lte.getI_cell_id1());
				ps.setString(3, lte.getV_cell_cname1());
				ps.setString(4, lte.getV_cell_ename1());
				ps.setDouble(5, lte.getI_longitude1());
				ps.setDouble(6, lte.getI_latitude1());
				ps.setInt(7, lte.getI_sf1());
				ps.setDouble(8, lte.getI_antenna_height1());
				ps.setInt(9, lte.getI_antenna_direction1());
				ps.setInt(10, lte.getTotal_downtilt1());
				ps.setDouble(11, lte.getN_distance());
				ps.setInt(12, lte.getI_enodeb_id());
				ps.setInt(13, lte.getI_cell_id());
				ps.setInt(14, lte.getEarfcn());
				ps.setInt(15, lte.getPci());
				ps.addBatch();
				counts++;
			}
			ps.executeBatch();
			conn.commit();
			log.info("saveNeighborCell 保存邻区配置信息 ， "+counts+"条执行完成！");
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}finally {
			try {
				ps.close();
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}finally{
				ps=null;
				conn=null;
			}
		}
	}
}
