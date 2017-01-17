package mr.mro;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import mr.mro.datasource.DataSource;
/**
 * 过覆盖小区处理类
 * 1.查询邻区>=8，相关性>=1的小区，分组后，拼装CGI，过覆盖影响小区数，被过覆盖小区CGI等信息
 * 2.将处理后的结果集更新入表中。
 * 3.更新配置信息
 * @author wangyubo
 */
public class OverCoverHandler {
	
	protected final Log log = LogFactory.getLog(OverCoverHandler.class);
	/**
	 * 数据库缓存提交数据
	 */
	public static final int COMMIT_COUNT = 2000;
	/**
	 * 数据日期
	 */
	private String dtm_repdate;
	
	/**
	 * 过覆盖小区对，主小区-邻区List集合
	 */
	private Map<String,List<LteConf>> map = new HashMap<String,List<LteConf>>();
	/**
	 * 被过覆盖小区集合
	 */
	private Map<String,List<String>> overedMap = new HashMap<String,List<String>>();
	
	/**
	 * 构造函数
	 * @param dtm_repdate 数据日期
	 */
	public OverCoverHandler(String dtm_repdate){
		this.dtm_repdate = dtm_repdate;
		this.initCellCoupleMap();
	}
	
	public static void main(String[] args){
		if(args.length<1){
			System.out.println("参数不足！");
			System.exit(0);
		}
		//过覆盖小区对
		OverCoverCoupleHandler occ = new OverCoverCoupleHandler(args[0]);
		occ.handleNeibhborCells();
		occ.saveNeighborCell();
		//过覆盖小区
		OverCoverHandler och = new OverCoverHandler(args[0]);
		och.handlerOverCoverCell();
		och.insertOveredCoverCell();
		och.updateConfiguration();
	}
	
	/**
	 * 调用存储过程，更新过覆盖小区的配置信息
	 */
	public void updateConfiguration(){
		Connection conn = null;
		Statement stmt = null;
		String update_sql = "begin lte_etl.lte_mr_etl.LTE_MRO_D_OVERCOVER_UP('"+dtm_repdate+"'); end ;";
		try {
			log.info("updateConfiguration 开始执行...");
			DataSource.init();
			conn = DataSource.getConnection();
			stmt = conn.createStatement();
			stmt.executeUpdate(update_sql);
			log.info("updateConfiguration 执行完成！");
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}finally {
			try {
				stmt.close();
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}finally{
				stmt=null;
				conn=null;
			}
		}
	}
	
	public void initCellCoupleMap(){
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		String select_sql = "SELECT I_ENODEB_ID,I_CELL_ID,I_ENODEB_ID1,I_CELL_ID1,N_ALL_POINT,N_NEIGHBOR_POINT," +
				"DECODE(N_ALL_POINT,0,0,NULL,0,N_NEIGHBOR_POINT/N_ALL_POINT) RELATIVE,N_DISTANCE,I_POLY_INTER " +
				"FROM  LTE_ETL.T_LTE_MR_OCC_D  " +
				"WHERE DTM_REPDATE=DATE'"+dtm_repdate+"' AND I_ENODEB_ID1 IS NOT NULL";
		try {
			log.info("initCellCoupleMap 开始执行...");
			DataSource.init();
			conn = DataSource.getConnection();
			stmt = conn.createStatement();
			rs = stmt.executeQuery(select_sql);
			LteConf lc = null;
			if(rs!=null){
				while(rs.next()){
					int enodeb = rs.getInt("i_enodeb_id");
					int ci = rs.getInt("i_cell_id");
					String key = enodeb+"-"+ci;
					lc = new LteConf();
					lc.setI_enodeb_id(enodeb);
					lc.setI_cell_id(ci);
					lc.setI_enodeb_id1(rs.getInt("i_enodeb_id1"));
					lc.setI_cell_id1(rs.getInt("i_cell_id1"));
					lc.setRelative(rs.getDouble("relative"));
					lc.setN_all_point(rs.getInt("n_all_point"));
					lc.setN_neighbor_point(rs.getInt("n_neighbor_point"));
					lc.setN_distance(rs.getDouble("n_distance"));
					lc.setI_poly_inter(rs.getDouble("i_poly_inter"));
					if(map.containsKey(key)){
						List<LteConf> list = map.get(key);
						list.add(lc);
					}else{
						List<LteConf> list = new ArrayList<LteConf>();
						list.add(lc);
						map.put(key, list);
					}
				}
			}
			log.info("initCellCoupleMap 执行完成！");
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
	 * 筛选符合条件的过覆盖小区，插入天级表t_lte_mr_overCover_cell中
	 */
	public void handlerOverCoverCell(){
		Connection conn = null;
		PreparedStatement ps = null;
		String static_sql = "insert into t_lte_mr_overCover_cell(dtm_repdate,i_Enodeb_Id,i_Cell_Id,v_cgi,n_All_Point,n_Overcover_Cell_Count,v_Overcover_Cell_Cgi) " +
				"values(to_date(?,'yyyy-mm-dd'),?,?,?,?,?,?)";
		try{
			log.info("过覆盖小区处理  开始执行...");
			DataSource.init();
			conn = DataSource.getConnection();
			ps = conn.prepareStatement(static_sql);
			if(!map.isEmpty()){
				Set<Map.Entry<String,List<LteConf>>> set = map.entrySet();
				for(Iterator<Map.Entry<String,List<LteConf>>> it = set.iterator();it.hasNext();){
					Map.Entry<String, List<LteConf>> entry = it.next();
					String key = entry.getKey();
					String[] temp = key.split("-");
					String enodeb = temp[0];
					String ci = temp[1];
					/**
					 * 过覆盖影响小区数
					 */
					int cellNum = 0;
					String cgi = "460-00-"+enodeb+"-"+ci;
					String cells_CGI = "";

					int dataCount = 0;
					List<LteConf> list = entry.getValue();
					int counts = 0;
					for(LteConf lc : list){
						counts = lc.getN_all_point();
						if(lc.getRelative() >= LteMroThresholdSet.RELATIVITY_PERCENT*1.0/100){
							cellNum++;
							String tmp = "460-00-"+lc.getI_enodeb_id1()+"-"+lc.getI_cell_id1();
							cells_CGI += (tmp+",");
//							if(lc.getN_distance() >= lc.getI_poly_inter()*LteMroThresholdSet.DISTANCE_THRESHOLD){
//								
//	 						}
						}
					}
					
					if(cellNum>=LteMroThresholdSet.OVER_COVER_THRESHOLD){
						/**
						 * 被过覆盖处理
						 */
						String[] cgis = cells_CGI.split(",");
						for(String nbr_cgi : cgis){
							if(!nbr_cgi.trim().equals("")){
								if(overedMap.containsKey(nbr_cgi)){
									List<String> li = overedMap.get(nbr_cgi);
									li.add(cgi);
								}else{
									List<String> li = new ArrayList<String>();
									li.add(cgi);
									overedMap.put(nbr_cgi, li);
								}
							}
						}
						
						/**
						 * 过覆盖插入
						 */
			 			ps.setString(1, dtm_repdate);
			 			ps.setString(2, enodeb);
			 			ps.setString(3, ci);
			 			ps.setString(4, cgi);
			 			ps.setInt(5, counts);
			 			ps.setInt(6, cellNum);
			 			ps.setString(7, cells_CGI.replaceAll(",$", ""));
			 			ps.addBatch();
			 			dataCount++;
			 			if(dataCount == COMMIT_COUNT){
			 				ps.executeBatch();
			 				dataCount=0;
			 			}
			 		}
				}
				ps.executeBatch();
			}
			log.info("handlerOverCoverCell 执行完成！");
		}catch(IOException e){
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
	
	/**
	 * 将被过覆盖数据更新进t_lte_mr_overCover_cell中
	 */
	public void insertOveredCoverCell(){
		Connection conn = null;
		PreparedStatement ps = null;
		String static_sql = "UPDATE T_LTE_MR_OVERCOVER_CELL  " +
				"SET V_OVERCOVERED_CELL_CGI=? " +
				"WHERE V_CGI=? AND DTM_REPDATE=TO_DATE('"+dtm_repdate+"','YYYY-MM-DD')";
		try{
			log.info("被过覆盖数据处理  开始执行...");
			DataSource.init();
			conn = DataSource.getConnection();
			ps = conn.prepareStatement(static_sql);
			int counts = 0;
			if(!overedMap.isEmpty()){
				Set<Map.Entry<String,List<String>>> set = overedMap.entrySet();
				for(Iterator<Map.Entry<String,List<String>>> it = set.iterator();it.hasNext();){
					Map.Entry<String, List<String>> entry = it.next();
					String cgi = entry.getKey();
					List<String> cgiList = entry.getValue();
					String cgis = "";
					for(String s : cgiList){
						cgis += (s+",");
					}
					ps.setString(1, cgi);
					ps.setString(2, cgis.replaceAll(",$", ""));
					ps.addBatch();
					counts++;
					if(counts == COMMIT_COUNT){
						ps.executeBatch();
						counts=0;
					}
				}
				ps.executeBatch();
			}
			log.info("被过覆盖数据处理  执行完成！");
		}catch(IOException e){
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
