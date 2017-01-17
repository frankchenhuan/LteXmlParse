package dis;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cfg.DataSource;
import cfg.Tools;

/**
 * @author 陈焕 根据配置的4G小区的2G频点，2G小区信息 用于生成CSFB专项核查模块的小区级已配邻区钻取数据，以应对集团检查2015
 * @param args
 */
public class Lte23LPInfo {
	protected static final Log log = LogFactory.getLog(Lte23LPInfo.class);

	/** gsm小区sql* */
	private static String gsm_cell = "SELECT T.V_MSC,T.I_SUBCOMP_ID I_SUBCOMP_ID,T.I_COUNTRY_ID I_COUNTRY_ID,T.V_BSC V_BSC,T.V_CGI V_CGI,T.I_CELL_ID I_CELL_ID,TO_CHAR(T.DTM_REPDATE,'YYYY-MM-DD') DTM_REPDATE,T.I_BCCH I_BCCH,T.I_LAC I_LAC,CONCAT(T.I_NCC,T.I_BCC)V_BSIC,T.V_BTS_ENG_NAME V_BTS_ENG_NAME,T.V_BTS_CHN_NAME V_BTS_CHN_NAME,T.IS_INDOOR_DISTRIBUTION I_SF,T.I_ANTENNA_DIRECTION I_ANTENNA_DIRECTION,T.I_LONGITUDE I_LONGITUDE,T.I_LATITUDE I_LATITUDE,T.V_ADJCELL V_ADJCELL,T.V_TD_ADJ V_ADJ_TD_CELL FROM GSM_CM_CELL_D T "
			+ "WHERE T.DTM_REPDATE=TRUNC(SYSDATE-1) AND T.I_BCCH IS NOT NULL AND T.I_LONGITUDE IS NOT NULL AND T.I_LATITUDE IS NOT NULL";

	/** lte小区sql* */
	private static String lte_cell = "SELECT T.I_SUBCOMP_ID,T.I_COUNTRY_ID,T.I_ENODEB_ID,T.I_CELL_ID,T.V_CELL_CNAME,T.I_LONGITUDE,T.I_LATITUDE,T.V_ADJ_BCCH_LIST,T.I_SF,T.V_FACTORY FROM T_LTE_GSM_RELATION_OMIT T WHERE T.V_ADJ_BCCH_LIST IS NOT NULL";

	private static String insert_lte_gsm = "INSERT INTO T_LTE_GSM_RELATION_OMIT_INFO(DTM_REPDATE,V_PROV_NAME,I_SUBCOMP_ID,I_COUNTRY_ID,I_CELL_ID,I_ENODEB_ID,V_CELL_CNAME,I_LONGITUDE,I_LATITUDE,I_SF,V_FACTORY,V_BSC,I_CELL_ID1,V_CGI,I_BCCH,I_LAC,V_BTS_CHN_NAME,V_BTS_ENG_NAME,V_BSIC,I_LONGITUDE1,I_LATITUDE1,IS_INDOOR_DISTRIBUTION,I_ANTENNA_DIRECTION,I_DISTANCE,I_LOST_BCCH,I_SUBCOMP_ID1,I_COUNTRY_ID1,V_CHECK,I_LOST_ADJ) VALUES(TRUNC(SYSDATE-1),'河南省',?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

	private static String td_cell = "SELECT T.I_SUBCOMP_ID,T.I_COUNTRY_ID,	T.I_COUNTRY_PART_ID,	T.I_COVERAGE_ID,	T.A_ID,	TO_CHAR(T.DTM_REPDATE,'YYYY-MM-DD') DTM_REPDATE,T2.V_CELLNAME_CN V_CELL_CNNAME,T2.V_CELLNAME_EN V_CELL_ENNAME,T.V_RNC V_RNC,T.I_RNCID I_RNC_ID,T.I_UARFCN I_UARFCN,T.I_CELLPARAMETERID I_CELLPARAMETERID,T.V_CELL_ID I_CELL_ID,T.I_LAC_ID I_LAC_ID,T.I_SF,T.V_MANUFACTURER V_FACTORY,T2.I_ANTENNA_HEIGHT I_ANTENNA_HEIGHT,(T2.I_MECHANICAL_ANGLE+T2.I_RETTILEVALUE) I_ANGLE,T2.I_ANTENNA_DIRECTION I_ANTENNA_DIRECTION,T2.I_LONGITUDE I_LONGITUDE,T.C051 V_CGI,T2.I_LATITUDE I_LATITUDE FROM T_TD_CM_UTRANCELL_D T "
			+ " INNER JOIN T_TD_CM_CELL_DIC T2 ON T.I_SUBCOMP_ID=T2.I_SUBCOMP_ID AND T.V_CELL_ID=T2.V_CELL_ID "
			+ " WHERE T.DTM_REPDATE=TRUNC(SYSDATE-1) AND T.I_LONGITUDE IS NOT NULL AND T.I_LATITUDE IS NOT NULL AND T.I_UARFCN IS NOT NULL";
	private static String lte_cell_td = "SELECT T.I_SUBCOMP_ID,T.I_COUNTRY_ID,T.I_ENODEB_ID,T.I_CELL_ID,T.I_LONGITUDE,T.I_LATITUDE,T.V_CELL_CHNAME,T.N_STATION_HEIGHT,T.N_BEARING,T.N_RETTILTVALUE,T.I_COVER_TYPE,T.I_COUNTRY_PART_ID,T.V_FACTORY,T.I_SF,T.I_TD_TRX_NUM FROM T_LTE_TD_RELATION_OMIT T WHERE T.I_TD_TRX_NUM IS NOT NULL";
	private static String insert_lte_td = "INSERT INTO T_LTE_TD_RELATION_OMIT_INFO(DTM_REPDATE,I_CHECK_RULE_ID,I_SUBCOMP_ID,I_COUNTRY_ID,V_CELL_CHNAME,I_ENODEB_ID,I_CELL_ID  ,I_LONGITUDE,I_LATITUDE,N_STATION_HEIGHT,N_RETTILTVALUE,N_BEARING,I_COVER_TYPE,V_TD_CELL_CHNAME,V_TD_CELL_ENNAME,N_DISTANCE,I_TD_RNC_ID,V_TD_RNC,I_TD_UARFCN,I_TD_CELLPARAMETERSID,I_TD_LONGITUDE,I_TD_LATITUDE,N_TD_BEARING ,N_TD_COVER_TYPE,I_IS_SAME_STATION,I_CELL_ID1,I_SUBCOMP_ID1,I_COUNTRY_ID1,I_LP,I_SF1,I_CHANGE,V_CHECK,I_SF,I_TD_LAC) values(trunc(sysdate-1),0,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

	private static final String dateFormat = "yyyyMMdd";
	private static final SimpleDateFormat df = new SimpleDateFormat(dateFormat);

	/** 存放gsm频点列表 */
	private static List bcchs = new ArrayList<String>();

	/** * */
	private static List uarfcns = new ArrayList<String>();

	/***************************************************************************
	 * 初始化gsm频点列表,td频点列表
	 */
	static {
		for (int i = 0; i <= 95; i++) {
			bcchs.add(i + "");
		}
		bcchs.add(116 + "");
		for (int i = 512; i <= 636; i++) {
			bcchs.add(i + "");
		}
		bcchs.add(975 + "");
		for (int i = 988; i <= 1023; i++) {
			bcchs.add(i + "");
		}

		for (int i = 9529; i <= 9570; i++) {
			uarfcns.add(i);
		}
		for (int i = 10054; i <= 10121; i++) {
			uarfcns.add(i);
		}
	}

	public static void main(String[] args) {
		try {
			DataSource.init();
		} catch (Exception e1) {
			log.error("初始化数据源失败，程序退出", e1);
			System.exit(1);
		}
		if (args.length == 0) {
			log.error("参数错误");
			System.exit(1);
		}
	
		String a1 = args[0];
		List<Map> ltelist = null;
		/**参数传入0表示,生成全部,含而2G和3G数据*/
		/**参数传入2表示2G,3表示3G**/
		if (a1.equals("0") || a1.equals("2")) {
			try {
				ltelist = getLteCellForGsm();
			} catch (Exception e1) {
				log.error("查询LTE小区异常", e1);
				System.exit(1);
			}
			try {

				List<Map> gsmlist = getGSMCellInfo();
				Map<String, List> bcchGsmMap = bcchGsmInit(gsmlist);
				int num = addLteToGsmAdj(ltelist, bcchGsmMap);
				log.info("GSM共入库" + num);
			} catch (Exception e) {
				log.error("GSM异常退出", e);
			}
		}

		// TD小区数据生成
		if (a1.equals("0") || a1.equals("3")) {
			try {
				ltelist = getLteCellForTD();
			} catch (Exception e1) {
				log.error("查询LTE小区异常--for-TD", e1);
				System.exit(1);
			}
			try {

				List<Map> tdlist = getTDCellInfo();
				Map<String, List> uarfcnTDMap = uarfcnTDInit(tdlist);
				int num = addLteToTDAdj(ltelist, uarfcnTDMap);
				log.info("TD共入库" + num);
			} catch (Exception e) {
				log.error("TD异常退出", e);
			}
		}

		/*
		 * try {
		 * 
		 * List<Map> tdlist = getTDCellInfo(); Map<String, List> uarfcnGsmMap =
		 * uarfcnTDInit(tdlist); int num = addLteToTDAdj(ltelist, uarfcnGsmMap);
		 * log.info("TD共入库" + num); } catch (Exception e) { log.error("TD异常退出",
		 * e); }
		 */
	}

	/***************************************************************************
	 * 
	 * 查询所有GSM小区
	 * 
	 * @throws SQLException
	 * @throws ParseException
	 **************************************************************************/
	private static List<Map> getGSMCellInfo() throws SQLException,
			ParseException {
		log.info("开始查询GSM小区信息");
		ResultSet rs = null;
		PreparedStatement ps = null;
		Connection con = DataSource.getConnection();
		ps = con.prepareStatement(gsm_cell);
		rs = ps.executeQuery();
		List<Map> gsmcells = Tools.getValues(rs);
		rs.close();
		ps.close();
		con.close();
		log.info("GSM小区查询结束" + gsmcells.size());
		return gsmcells;
	}

	/***************************************************************************
	 * 
	 * 查询所有GSM小区
	 * 
	 * @throws SQLException
	 * @throws ParseException
	 **************************************************************************/
	private static List<Map> getTDCellInfo() throws SQLException,
			ParseException {
		log.info("开始查询TD小区信息");
		ResultSet rs = null;
		PreparedStatement ps = null;
		Connection con = DataSource.getConnection();
		ps = con.prepareStatement(td_cell);
		rs = ps.executeQuery();
		List<Map> tdcells = Tools.getValues(rs);
		rs.close();
		ps.close();
		con.close();
		log.info("TD小区查询结束" + tdcells.size());
		return tdcells;
	}

	/**
	 * 
	 * 根据分公司id和bcch，将gsm小区分成不同的组，存放map
	 */
	private static Map<String, List> bcchGsmInit(List<Map> gsmcells) {
		log.info("开始按bcch分组存放gsm小区");
		Map<String, List> bcchMap = new HashMap<String, List>();
		/** 初始化存放不同bcch小区的list对象 */
		for (int i = 0; i < bcchs.size(); i++) {
			for (int j = 1; j <= 18; j++) {// 分公司ID,18个分公司
				List bcchcells = new ArrayList<Map>();
				bcchMap.put(j + "_" + bcchs.get(i), bcchcells);
			}
		}

		/***********************************************************************
		 * 循环gsm小区列表，将不同分公司和bcch的小区存放到不同的list中
		 **********************************************************************/
		for (int i = 0; i < gsmcells.size(); i++) {
			Map cell = gsmcells.get(i);
			String i_subcomp_id = cell.get("I_SUBCOMP_ID").toString();
			String i_bcch = cell.get("I_BCCH").toString();
			String key = i_subcomp_id + "_" + i_bcch;
			List bcchcells = bcchMap.get(key);
			bcchcells.add(cell);
		}
		log.info("bcch分组存放结束");
		return bcchMap;
	}

	/**
	 * 
	 * 根据分公司id和uarfcn，将td小区分成不同的组，存放map
	 */
	private static Map<String, List> uarfcnTDInit(List<Map> tdcells) {
		log.info("开始按uarfcn分组存放TD小区");
		Map<String, List> uarfcnMap = new HashMap<String, List>();
		/** 初始化存放不同uarfcn小区的list对象 */
		for (int i = 0; i < uarfcns.size(); i++) {
			for (int j = 1; j <= 18; j++) {// 分公司ID,18个分公司
				List uarfcncells = new ArrayList<Map>();
				uarfcnMap.put(j + "_" + uarfcns.get(i), uarfcncells);
			}
		}

		/***********************************************************************
		 * 循环gsm小区列表，将不同分公司和bcch的小区存放到不同的list中
		 **********************************************************************/
		for (int i = 0; i < tdcells.size(); i++) {
			Map cell = tdcells.get(i);
			String i_subcomp_id = cell.get("I_SUBCOMP_ID").toString();
			String i_bcch = cell.get("I_UARFCN").toString();
			String key = i_subcomp_id + "_" + i_bcch;
			List uarfcncells = uarfcnMap.get(key);
			uarfcncells.add(cell);
		}
		log.info("uarfcn分组存放结束");
		return uarfcnMap;
	}

	/***************************************************************************
	 * 
	 * 查询Lte基站数据
	 * 
	 * @throws SQLException
	 * @throws ParseException
	 **************************************************************************/
	private static List<Map> getLteCellForGsm() throws SQLException,
			ParseException {
		log.info("查询LTE小区信息--for_gsm");
		ResultSet rs = null;
		PreparedStatement ps = null;
		Connection con = DataSource.getConnection();
		ps = con.prepareStatement(lte_cell);
		rs = ps.executeQuery();
		List<Map> ltecell = Tools.getValues(rs);
		rs.close();
		ps.close();
		con.close();
		log.info("LTE小区查询完成--for_gsm" + ltecell.size());
		return ltecell;
	}

	/***************************************************************************
	 * 
	 * 查询Lte基站数据
	 * 
	 * @throws SQLException
	 * @throws ParseException
	 **************************************************************************/
	private static List<Map> getLteCellForTD() throws SQLException,
			ParseException {
		log.info("查询LTE小区信息--for-TD");
		ResultSet rs = null;
		PreparedStatement ps = null;
		Connection con = DataSource.getConnection();
		ps = con.prepareStatement(lte_cell_td);
		rs = ps.executeQuery();
		List<Map> ltecell = Tools.getValues(rs);
		rs.close();
		ps.close();
		con.close();
		log.info("LTE小区查询完成--for-TD" + ltecell.size());
		return ltecell;
	}

	/***************************************************************************
	 * 将频点转换为邻区，存放到lte小区对象中
	 * 
	 * @throws SQLException
	 * @throws NumberFormatException
	 * 
	 */
	private static int addLteToGsmAdj(List<Map> ltelist,
			Map<String, List> bcchGsmMap) throws NumberFormatException,
			SQLException {
		log.info("GSM根据频点计算最近邻区");
		PreparedStatement ps = null;
		Connection con = DataSource.getConnection();
		con.setAutoCommit(false);
		int num = 0;
		ps = con.prepareStatement(insert_lte_gsm);
		for (int i = 0; i < ltelist.size(); i++) {
			Map ltecell = ltelist.get(i);
			// List<Map> gsmadjs = new ArrayList<Map>();
			// ltecell.put("GSM_ADJ", gsmadjs);
			String i_subcomp_id = ltecell.get("I_SUBCOMP_ID").toString();
			String adjbcchlist = ltecell.get("V_ADJ_BCCH_LIST").toString();
			// List<Map> gsmadjs = (List) ltecell.get("GSM_ADJ");
			// String i_subcomp_id = (String) ltecell.get("I_SUBCOMP_ID");
			String i_country_id = (String) ltecell.get("I_COUNTRY_ID");
			String i_cell_id = (String) ltecell.get("I_CELL_ID");
			String i_enodeb_id = (String) ltecell.get("I_ENODEB_ID");
			String v_cell_cname = (String) ltecell.get("V_CELL_CNAME");
			String i_longitude = (String) ltecell.get("I_LONGITUDE");
			String i_latitude = (String) ltecell.get("I_LATITUDE");
			String i_sf = (String) ltecell.get("I_SF");
			String v_factory = (String) ltecell.get("V_FACTORY");
			String[] bcchs = adjbcchlist.split("/");
			double ltelon = Double.parseDouble(ltecell.get("I_LONGITUDE")
					.toString());
			double ltelat = Double.parseDouble(ltecell.get("I_LATITUDE")
					.toString());
			for (int j = 0; j < bcchs.length; j++) {
				String key = i_subcomp_id + "_" + bcchs[j];
				List<Map> bcchGsmlist = bcchGsmMap.get(key);
				if (bcchGsmlist == null) {
					continue;
				}
				Map min_gsmcell = null;// 存放最小距离的gsm小区
				double min_dis = -1d;// 存放最小距离
				for (int k = 0; k < bcchGsmlist.size(); k++) {
					Map gsmcell = bcchGsmlist.get(k);
					double gsmlon = Double.parseDouble(gsmcell.get(
							"I_LONGITUDE").toString());
					double gsmlat = Double.parseDouble(gsmcell
							.get("I_LATITUDE").toString());
					// 计算距离
					double dis = Tools.getDistatce(gsmlat, ltelat, gsmlon,
							ltelon);

					/***********************************************************
					 * 最小距离大于当前小区距离是，将最小距离等于当前最小距离，并且替换最小距离小区
					 **********************************************************/
					if (min_dis == -1 || min_dis > dis) {
						min_dis = dis;
						min_gsmcell = new HashMap();
						min_gsmcell.putAll(gsmcell);
						min_gsmcell.put("I_DISTANCE", dis + "");
					}
				}
				/**
				 * 存放最近的邻区
				 */
				if (min_dis != -1) {
					// gsmadjs.add(min_gsmcell);
					ps.setInt(1, Integer.parseInt(i_subcomp_id));
					ps.setInt(2, Integer.parseInt(i_country_id));
					ps.setInt(3, Integer.parseInt(i_cell_id));
					ps.setInt(4, Integer.parseInt(i_enodeb_id));
					if (v_cell_cname == null || v_cell_cname.equals("")) {
						ps.setNull(5, java.sql.Types.VARCHAR);
					} else {
						ps.setString(5, v_cell_cname);
					}
					ps.setDouble(6, Double.parseDouble(i_longitude));
					ps.setDouble(7, Double.parseDouble(i_latitude));
					ps.setInt(8, Integer.parseInt(i_sf));
					ps.setString(9, v_factory);
					// Map gsmcell = gsmadjs.get(j);
					String v_bsc = (String) min_gsmcell.get("V_BSC");
					if (v_bsc == null || v_bsc.equals("")) {
						ps.setNull(10, java.sql.Types.VARCHAR);
					} else {
						ps.setString(10, v_bsc);
					}
					String i_cell_id1 = (String) min_gsmcell.get("I_CELL_ID");
					ps.setInt(11, Integer.parseInt(i_cell_id1));
					String v_cgi = (String) min_gsmcell.get("V_CGI");
					ps.setString(12, v_cgi);
					String i_bcch = (String) min_gsmcell.get("I_BCCH");
					ps.setInt(13, Integer.parseInt(i_bcch));
					String i_lac = (String) min_gsmcell.get("I_LAC");
					ps.setInt(14, Integer.parseInt(i_lac));
					String v_bts_chn_name = (String) min_gsmcell
							.get("V_BTS_CHN_NAME");
					if (v_bts_chn_name == null || v_bts_chn_name.equals("")) {
						ps.setNull(15, java.sql.Types.VARCHAR);
					} else {
						ps.setString(15, v_bts_chn_name);
					}
					String v_bts_eng_name = (String) min_gsmcell
							.get("V_BTS_ENG_NAME");
					if (v_bts_eng_name == null || v_bts_eng_name.equals("")) {
						ps.setNull(16, java.sql.Types.VARCHAR);
					} else {
						ps.setString(16, v_bts_eng_name);
					}
					String v_bsic = (String) min_gsmcell.get("V_BSIC");
					if (v_bsic == null || v_bsic.equals("")) {
						ps.setNull(17, java.sql.Types.VARCHAR);
					} else {
						ps.setString(17, v_bsic);
					}
					String i_longitude1 = (String) min_gsmcell
							.get("I_LONGITUDE");
					ps.setDouble(18, Double.parseDouble(i_longitude1));
					String i_latitude1 = (String) min_gsmcell.get("I_LATITUDE");
					ps.setDouble(19, Double.parseDouble(i_latitude1));
					String is_indoor_distribution = (String) min_gsmcell
							.get("I_SF");
					// ps.setInt(20, Integer.parseInt(is_indoor_distribution));
					if (is_indoor_distribution == null
							|| is_indoor_distribution.equals("")) {
						ps.setNull(20, java.sql.Types.VARCHAR);
					} else {
						ps.setString(20, is_indoor_distribution);
					}
					String i_antenna_direction = (String) min_gsmcell
							.get("I_ANTENNA_DIRECTION");
					ps.setInt(21, Integer.parseInt(i_antenna_direction));
					String i_distance = (String) min_gsmcell.get("I_DISTANCE");
					ps.setDouble(22, Double.parseDouble(i_distance));
					int i_lost_bcch = 0;
					ps.setInt(23, i_lost_bcch);
					String i_subcomp_id1 = (String) min_gsmcell
							.get("I_SUBCOMP_ID");
					ps.setInt(24, Integer.parseInt(i_subcomp_id1));
					String i_country_id1 = (String) min_gsmcell
							.get("I_COUNTRY_ID");
					ps.setInt(25, Integer.parseInt(i_country_id1));
					String v_check = "";
					ps.setString(26, v_check);
					int i_lost_adj = 0;
					ps.setInt(27, i_lost_adj);
					ps.addBatch();
				}

			}
			if (i != 0 && i % 1000 == 0) {
				num += ps.executeBatch().length;
				con.commit();
				log.debug("GSM已完成" + i + "LTE小区GSM邻区明细入库");
			}
		}
		num += ps.executeBatch().length;
		con.commit();
		log.info("完成计算");
		ps.close();
		con.close();
		return num;
	}

	/***************************************************************************
	 * 将频点转换为邻区，存放到lte小区对象中
	 * 
	 * @throws SQLException
	 * @throws NumberFormatException
	 * 
	 */
	private static int addLteToTDAdj(List<Map> ltelist,
			Map<String, List> uarfcnTDMap) throws NumberFormatException,
			SQLException {
		log.info("TD根据频点计算最近邻区");
		PreparedStatement ps = null;
		Connection con = DataSource.getConnection();
		con.setAutoCommit(false);
		int num = 0;
		ps = con.prepareStatement(insert_lte_td);
		for (int i = 0; i < ltelist.size(); i++) {
			Map ltecell = ltelist.get(i);
			// List<Map> gsmadjs = new ArrayList<Map>();
			// ltecell.put("GSM_ADJ", gsmadjs);
			String i_subcomp_id = ltecell.get("I_SUBCOMP_ID").toString();
			String uarfcnlist = ltecell.get("I_TD_TRX_NUM").toString();
			// List<Map> gsmadjs = (List) ltecell.get("GSM_ADJ");
			// String i_subcomp_id = (String) ltecell.get("I_SUBCOMP_ID");
			String i_country_id = (String) ltecell.get("I_COUNTRY_ID");
			String i_cell_id = (String) ltecell.get("I_CELL_ID");
			String i_enodeb_id = (String) ltecell.get("I_ENODEB_ID");
			String v_cell_cname = (String) ltecell.get("V_CELL_CHNAME");
			String i_longitude = (String) ltecell.get("I_LONGITUDE");
			String i_latitude = (String) ltecell.get("I_LATITUDE");
			String i_sf = (String) ltecell.get("I_SF");
			String v_factory = (String) ltecell.get("V_FACTORY");
			String N_STATION_HEIGHT = (String) ltecell.get("N_STATION_HEIGHT");
			String N_BEARING = (String) ltecell.get("N_BEARING");
			String N_RETTILTVALUE = (String) ltecell.get("N_RETTILTVALUE");
			String I_COVER_TYPE = (String) ltecell.get("I_COVER_TYPE");
			String[] uarfcns = uarfcnlist.split("/");
			double ltelon = Double.parseDouble(ltecell.get("I_LONGITUDE")
					.toString());
			double ltelat = Double.parseDouble(ltecell.get("I_LATITUDE")
					.toString());
			for (int j = 0; j < uarfcns.length; j++) {
				String key = i_subcomp_id + "_" + uarfcns[j];
				List<Map> uarfcnTDlist = uarfcnTDMap.get(key);
				if (uarfcnTDlist == null) {
					continue;
				}
				Map min_tdcell = null;// 存放最小距离的td小区
				double min_dis = -1d;// 存放最小距离
				for (int k = 0; k < uarfcnTDlist.size(); k++) {
					Map tdcell = uarfcnTDlist.get(k);
					double tdlon = Double.parseDouble(tdcell.get("I_LONGITUDE")
							.toString());
					double tdlat = Double.parseDouble(tdcell.get("I_LATITUDE")
							.toString());
					// 计算距离
					double dis = Tools
							.getDistatce(tdlat, ltelat, tdlon, ltelon);

					/***********************************************************
					 * 最小距离大于当前小区距离是，将最小距离等于当前最小距离，并且替换最小距离小区
					 **********************************************************/
					if (min_dis == -1 || min_dis > dis) {
						min_dis = dis;
						min_tdcell = new HashMap();
						min_tdcell.putAll(tdcell);
						min_tdcell.put("I_DISTANCE", dis + "");
					}
				}
				/**
				 * 存放最近的邻区
				 */
				if (min_dis != -1) {
					// gsmadjs.add(min_gsmcell);
					ps.setInt(1, Integer.parseInt(i_subcomp_id));
					ps.setInt(2, Integer.parseInt(i_country_id));
					if (v_cell_cname == null || v_cell_cname.equals("")) {
						ps.setNull(3, java.sql.Types.VARCHAR);
					} else {
						ps.setString(3, v_cell_cname);
					}

					ps.setInt(4, Integer.parseInt(i_enodeb_id));
					ps.setInt(5, Integer.parseInt(i_cell_id));
					ps.setDouble(6, Double.parseDouble(i_longitude));
					ps.setDouble(7, Double.parseDouble(i_latitude));
					ps.setString(8, N_STATION_HEIGHT);
					ps.setString(9, N_RETTILTVALUE);
					ps.setString(10, N_BEARING);
					ps.setString(11, I_COVER_TYPE);

					String V_TD_CELL_CNNAME = (String) min_tdcell
							.get("V_CELL_CNNAME");
					if (V_TD_CELL_CNNAME == null || V_TD_CELL_CNNAME.equals("")) {
						ps.setNull(12, java.sql.Types.VARCHAR);
					} else {
						ps.setString(12, V_TD_CELL_CNNAME);
					}
					String V_TD_CELL_ENNAME = (String) min_tdcell
							.get("V_CELL_ENNAME");
					if (V_TD_CELL_ENNAME == null || V_TD_CELL_ENNAME.equals("")) {
						ps.setNull(13, java.sql.Types.VARCHAR);
					} else {
						ps.setString(13, V_TD_CELL_ENNAME);
					}
					Double i_distance = Double.parseDouble((String) min_tdcell
							.get("I_DISTANCE"));
					ps.setDouble(14, i_distance);

					String i_rnc_id = (String) min_tdcell.get("I_RNC_ID");
					ps.setInt(15, Integer.parseInt(i_rnc_id));

					String V_RNC = (String) min_tdcell.get("V_RNC");
					ps.setString(16, V_RNC);

					String I_UARFCN = (String) min_tdcell.get("I_UARFCN");
					ps.setString(17, I_UARFCN);

					String I_CELLPARAMETERID = (String) min_tdcell
							.get("I_CELLPARAMETERID");
					ps.setString(18, I_CELLPARAMETERID);

					String i_longitude1 = (String) min_tdcell
							.get("I_LONGITUDE");
					ps.setDouble(19, Double.parseDouble(i_longitude1));
					String i_latitude1 = (String) min_tdcell.get("I_LATITUDE");
					ps.setDouble(20, Double.parseDouble(i_latitude1));

					String I_ANTENNA_DIRECTION = (String) min_tdcell
							.get("I_ANTENNA_DIRECTION");
					ps.setString(21, I_ANTENNA_DIRECTION);

					String I_COVERAGE_ID = (String) min_tdcell
							.get("I_COVERAGE_ID");
					ps.setString(22, I_COVERAGE_ID);

					if (i_distance < 50) {
						ps.setInt(23, 1);
					} else {
						ps.setInt(23, 0);
					}
					String i_cell_id1 = (String) min_tdcell.get("I_CELL_ID");
					ps.setInt(24, Integer.parseInt(i_cell_id1));
					String i_subcomp_id1 = (String) min_tdcell
							.get("I_SUBCOMP_ID");
					ps.setInt(25, Integer.parseInt(i_subcomp_id1));
					String i_country_id1 = (String) min_tdcell
							.get("I_COUNTRY_ID");
					ps.setInt(26, Integer.parseInt(i_country_id1));

					ps.setInt(27, 0);

					String I_SF1 = (String) min_tdcell.get("I_SF");
					ps.setString(28, I_SF1);

					ps.setString(29, "3");
					ps.setNull(30, java.sql.Types.VARCHAR);
					ps.setString(31, i_sf);

					String I_LAC_ID = (String) min_tdcell.get("I_LAC_ID");
					ps.setString(32, I_LAC_ID);
					ps.addBatch();
				}

			}
			if (i != 0 && i % 1000 == 0) {
				num += ps.executeBatch().length;
				con.commit();
				log.debug("TD已完成" + i + "LTE小区TD邻区明细入库");
			}
		}
		num += ps.executeBatch().length;
		con.commit();
		log.info("TD完成计算");
		ps.close();
		con.close();
		return num;
	}
}
