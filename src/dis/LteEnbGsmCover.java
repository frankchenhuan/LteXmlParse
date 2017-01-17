package dis;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cfg.DataSource;
import cfg.Tools;

/***
 * @author chenhuan
 * 完成lte基站周边gsm基站数量的统计
 * 从数据库中查出lte基站和gsm基站，计算距离然后对比
 * 最终将结果插入到数据库表中
 * 此程序按照分公司计算，不同分公司的基站不会被统计在内
 * **/
public class LteEnbGsmCover {

	protected static final Log log = LogFactory.getLog(LteEnbGsmCover.class);
	/** gsm基站sql* */
	private static String gsm_bts_sql = "SELECT T.I_SUBCOMP_ID,T.V_BTS_NUMBER,MAX(T.I_LONGITUDE) I_LONGITUDE,MAX(T.I_LATITUDE) I_LATITUDE FROM GSM_CM_CELL_D T WHERE T.DTM_REPDATE=? AND T.I_SUBCOMP_ID=? AND T.I_LONGITUDE IS NOT NULL AND T.I_LATITUDE IS NOT NULL AND T.IS_INDOOR_DISTRIBUTION=0 GROUP BY T.I_SUBCOMP_ID,T.V_BTS_NUMBER";

	/** lte基站sql* */
	private static String lte_enb_sql = "SELECT T.I_SUBCOMP_ID,T.I_ENODEB_ID,MAX(T.I_COUNTRY_ID) I_COUNTRY_ID,MAX(T.I_COUNTRY_PART_ID) I_COUNTRY_PART_ID,MAX(T.I_LONGITUDE) I_LONGITUDE,MAX(T.I_LATITUDE) I_LATITUDE FROM T_LTE_CM_EUTRANCELLTDD_D T WHERE T.DTM_REPDATE=? AND T.I_SUBCOMP_ID =? AND T.I_LONGITUDE IS NOT NULL AND T.I_LATITUDE IS NOT NULL AND T.I_SF=0  GROUP BY T.I_SUBCOMP_ID,T.I_ENODEB_ID";
	
	private static String insert_lte_enb = "INSERT INTO T_LTE_ENB_GSM_COVER(I_ENODEB_ID,I_SUBCOMP_ID,I_COUNTRY_ID,I_COUNTRY_PART_ID,I_GSM_NUM,DTM_REPDATE) VALUES(?,?,?,?,?,?)";
	private static final String dateFormat = "yyyyMMdd";
	private static final SimpleDateFormat df = new SimpleDateFormat(dateFormat);
	
	

	/**
	 * @param args
	 * 参数1 分公司id  0表示全省
	 * 参数2 日期 格式yyyyMMdd
	 * 参数3 距离
	 */
	public static void main(String[] args) {
		int subcomp = Integer.parseInt(args[0]);
		String date = args[1];
		int dis = Integer.parseInt(args[2]);
		try {
			DataSource.init();
		} catch (Exception e1) {
			log.error("初始化数据源失败", e1);
			System.exit(1);
		}
		if (subcomp == 0) {// 0表示全省,十八个地市循环生成
			for (int i = 1; i <= 18; i++) {
				try {
					lteEnbGsmCover( i,  date,  dis);
				} catch (Exception e) {
					log.error("分公司ID=" + i, e);
				}
			}
		} else {
			try {
				lteEnbGsmCover(subcomp,  date,  dis);
			} catch (Exception e) {
				log.error("分公司ID=" + subcomp, e);
			}
		}
		log.info("查询GSM基站:分公司ID=" + subcomp + " 日期=" + date);
	}

	/**
	 * 生成指定分公司 日期 距离的LTE基站对gsm基站的覆盖情况
	 */
	private static void lteEnbGsmCover(int subcomp, String date, int dis)
			throws SQLException, ParseException {
		log.info("开始查询LTE基站：分公司ID=" + subcomp + " 日期=" + date);
		List<Map> ltelist = getLteEnb(subcomp, date);
		log.info("开始查询GSM基站：分公司ID=" + subcomp + " 日期=" + date);
		List<Map> gsmlist = getGsmBts(subcomp, date);
		log.info("开始对比距离：分公司ID=" + subcomp + " 日期=" + date);
		ltelist = cover(ltelist, gsmlist, dis);
		log.info("开始入库：分公司ID=" + subcomp + " 日期=" + date);
		insertLteEnb(ltelist, date);
		log.info("结束：分公司ID=" + subcomp + " 日期=" + date + " 基站数:"
				+ ltelist.size());
	}

	/***************************************************************************
	 * 
	 * 根据分公司id查询Lte基站数据
	 * 
	 * @throws SQLException
	 * @throws ParseException
	 **************************************************************************/
	private static List<Map> getLteEnb(int subcomp, String date)
			throws SQLException, ParseException {
		log.info("查询LTE基站:分公司ID=" + subcomp + " 日期=" + date);
		ResultSet rs = null;
		PreparedStatement ps = null;
		Connection con = DataSource.getConnection();
		ps = con.prepareStatement(lte_enb_sql);
		ps.setDate(1, new Date(df.parse(date).getTime()));
		ps.setInt(2, subcomp);
		rs = ps.executeQuery();
		List<Map> lteenb = Tools.getValues(rs);
		rs.close();
		ps.close();
		con.close();
		log.debug("分公司"+subcomp+"查询LTE基站数"+lteenb.size());
		return lteenb;
	}

	/***************************************************************************
	 * 根据分公司id查询GSM基站数据
	 * 
	 * @throws SQLException
	 * @throws ParseException
	 **************************************************************************/
	private static List<Map> getGsmBts(int subcomp, String date)
			throws SQLException, ParseException {

		ResultSet rs = null;
		PreparedStatement ps = null;
		Connection con = DataSource.getConnection();
		ps = con.prepareStatement(gsm_bts_sql);
		ps.setDate(1, new Date(df.parse(date).getTime()));
		ps.setInt(2, subcomp);
		rs = ps.executeQuery();
		List<Map> gsmbts = Tools.getValues(rs);
		rs.close();
		ps.close();
		con.close();
		return gsmbts;
	}

	/***************************************************************************
	 * 根据距离计算Lte基站周边gsm基站数量
	 * 
	 * 将lte基站和gsm基站循环对比，将周边指定距离之内的gsm小区数量进行计数，并绑定到每个基站的gsm_num属性
	 * 返回ltelist,同参数list相同
	 * 
	 **************************************************************************/
	private static List<Map> cover(List<Map> ltelist, List<Map> gsmlist, int dis) {
		for (int i = 0; i < ltelist.size(); i++) {
			Map lteenb = ltelist.get(i);
			double ltelon = Double.parseDouble(lteenb.get("I_LONGITUDE")
					.toString());
			double ltelat = Double.parseDouble(lteenb.get("I_LATITUDE")
					.toString());
			String s = (String) lteenb.get("GSM_NUM");
			int gsm_num = 0;
			if (s != null && !s.equals("")) {
				gsm_num = Integer.parseInt(s);
			}

			for (int j = 0; j < gsmlist.size(); j++) {
				Map gsmbts = gsmlist.get(j);
				double gsmlon = Double.parseDouble(gsmbts.get("I_LONGITUDE")
						.toString());
				double gsmlat = Double.parseDouble(gsmbts.get("I_LATITUDE")
						.toString());
				// 如果距离小于指定距离，则范围内的gsm小区数量+1
				if (Tools.getDistatce(gsmlat, ltelat, gsmlon, ltelon) < dis) {
					gsm_num++;
				}
			}

			if (i != 0 && i % 1000 == 0) {
				log.debug("已对比LTE基站:" + i);
			}
			lteenb.put("GSM_NUM", gsm_num + "");

		}
		return ltelist;
	}

	/***************************************************************************
	 * 将lte基站插入到数据表中T_LTE_ENB_GSM_COVER 返回执行条数
	 * 
	 * @throws SQLException
	 * @throws ParseException
	 **************************************************************************/
	private static int insertLteEnb(List<Map> lteEnb, String date)
			throws SQLException, ParseException {
		PreparedStatement ps = null;
		Connection con = DataSource.getConnection();
		con.setAutoCommit(false);
		int num = 0;
		ps = con.prepareStatement(insert_lte_enb);
		for (int i = 0; i < lteEnb.size(); i++) {
			// I_ENODEB_ID,I_SUBCOMP_ID,I_COUNTRY_ID,I_COUNTRY_PART_ID,I_GSM_NUM,DTM_REPDATE
			Map enbmap = lteEnb.get(i);
			String v = (String) enbmap.get("I_ENODEB_ID");
			if (v == null || v.trim().equals("")) {
				ps.setNull(1, java.sql.Types.INTEGER);
			} else {
				ps.setInt(1, Integer.parseInt(v));
			}
			v = (String) enbmap.get("I_SUBCOMP_ID");
			if (v == null || v.trim().equals("")) {
				ps.setNull(2, java.sql.Types.INTEGER);
			} else {
				ps.setInt(2, Integer.parseInt(v));
			}

			v = (String) enbmap.get("I_COUNTRY_ID");
			if (v == null || v.trim().equals("")) {
				ps.setNull(3, java.sql.Types.INTEGER);
			} else {
				ps.setInt(3, Integer.parseInt(v));
			}

			v = (String) enbmap.get("I_COUNTRY_PART_ID");
			if (v == null || v.trim().equals("")) {
				ps.setNull(4, java.sql.Types.INTEGER);
			} else {
				ps.setInt(4, Integer.parseInt(v));
			}

			v = (String) enbmap.get("GSM_NUM");
			if (v == null || v.trim().equals("")) {
				ps.setInt(5, 0);
			} else {
				ps.setInt(5, Integer.parseInt(v));
			}

			ps.setDate(6, new Date(df.parse(date).getTime()));
			ps.addBatch();
			if (i != 0 && i % 1000 == 0) {
				num += ps.executeBatch().length;
				con.commit();
				log.debug("已入库LTE基站：" + i);
			}
		}
		num += ps.executeBatch().length;
		con.commit();
		ps.close();
		con.close();
		return num;
	}

}
