package dis;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cfg.DataSource;
import cfg.Tools;

public class GsmToLteLP {

	protected static final Log log = LogFactory.getLog(GsmToLteLP.class);

	private static final String dateFormat = "yyyyMMdd";
	private static final SimpleDateFormat df = new SimpleDateFormat(dateFormat);

	private String GSM_SQL = "SELECT TO_CHAR(DTM_REPDATE,'yyyymmdd') DTM_REPDATE,I_SUBCOMP_ID,I_COUNTRY_ID,V_BTS_ENG_NAME,V_BTS_CHN_NAME,I_LAC,I_CELL_ID,I_LONGITUDE,I_LATITUDE,I_ANTENNA_DIRECTION,FREQ_BAND_IN_USE,I_BCCH,I_CONVERAGE_ID,V_REP_FACTORY,IS_INDOOR_DISTRIBUTION I_SF, T.I_AREA_ID,T.I_COUNTRY_PART_ID,T.I_LTE_FREQ FROM GSM_CM_CELL_D T WHERE T.DTM_REPDATE=? AND T.I_SUBCOMP_ID=? AND I_LONGITUDE IS NOT NULL AND I_LATITUDE IS NOT NULL AND I_ANTENNA_DIRECTION IS NOT NULL AND IS_INDOOR_DISTRIBUTION IS NOT NULL";

	private String LTE_SQL = "SELECT TO_CHAR(DTM_REPDATE,'yyyymmdd') DTM_REPDATE,I_SUBCOMP_ID,I_ENODEB_ID,I_CELL_ID,T.V_CELL_CNAME,T.V_CELL_ENAME,T.I_CONVERAGE_ID,T.I_LONGITUDE,T.I_LATITUDE,T.I_ANTENNA_DIRECTION,T.EARFCN,I_SF,T.I_COUNTRY_PART_ID,T.A_ID FROM T_LTE_CM_EUTRANCELLTDD_D T WHERE T.DTM_REPDATE=? AND T.I_SUBCOMP_ID=? AND I_LONGITUDE IS NOT NULL AND I_LATITUDE IS NOT NULL AND I_ANTENNA_DIRECTION IS NOT NULL AND I_SF IS NOT NULL";

	private String TD_SQL = "SELECT T.I_SUBCOMP_ID,T.T_CELLID I_CELL_ID,T.V_LTE_TRX,T.ADJ_LTE_LIST,T.T_LAC,T.V_PCI,D.I_LONGITUDE,D.I_LATITUDE,D.I_SF FROM T_TD_CM_LTE_RELATION_EXP T INNER JOIN T_TD_CM_CELL_DIC D ON T.I_SUBCOMP_ID=D.I_SUBCOMP_ID AND T.T_CELLID=D.V_CELL_ID WHERE T.I_SUBCOMP_ID=? AND D.I_SF IS NOT NULL AND D.I_LONGITUDE IS NOT NULL AND D.I_LATITUDE IS NOT NULL";

	private String INSERT_LP = "INSERT INTO T_GSM_LTE_REL_OMIT_NEW( DTM_REPDATE, I_SUBCOMP_ID, I_COUNTRY_ID, V_BTS_ENG_NAME, V_BTS_CHN_NAME, I_LAC,I_CELL_ID, I_LONGITUDE,I_LATITUDE, I_ANTENNA_DIRECTION, FREQ_BAND_IN_USE, I_BCCH, I_CONVERAGE_ID, V_REP_FACTORY,V_4GFREQ_1, V_4GFREQ_2, V_4GFREQ_3, I_SF, I_COUNTRY_PART_ID, I_AREA_ID,I_GZ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? , ?)";

	private String INSERT_LP_INFO = "INSERT INTO T_GSM_LTE_REL_OMIT_INFO_NEW (DTM_REPDATE,  I_SUBCOMP_ID,  I_CELL_ID,  I_ENODEB_ID,  I_CELL_ID_4G,  V_ENG_NAME_4G,  V_CHN_NAME_4G,  I_CONVERAGE_ID,  I_LONGITUDE, I_LATITUDE, I_LONGITUDE_4G, I_LATITUDE_4G, I_ANTENNA_DIRECTION_4G, I_4GFREQ, I_DISTANCE, I_SF, I_COUNTRY_PART_ID, I_AREA_ID,I_LP)  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

	private List<Map> GSMCELLS = new ArrayList<Map>();

	private List<Map> TDCELLS = new ArrayList<Map>();

	/** LTE小区的map形式存放，key为"基站号-小区号" */
	private Map<String, Map> LTECELLS_MAP = new HashMap<String, Map>();
	/** 存放LTE小区 */
	private List<Map> LTECELLS = new ArrayList<Map>();

	/** 室分共站距离要求(米) */
	private int GZ_DISTATCE_SF = 50;

	/** 室外共站距离要求(米) */
	private int GZ_DISTATCE_SW = 50;

	/** 室外共站方向角偏差值 */
	private int GZ_ANGLE = 30;

	/** 非共站距离要求(米) */
	private int FGZ_DISTATCE = 2000;

	/** 室外非共站核查距离范围内最多保存的小区数量 */
	private int ADJ_COUNT_FOR_DIS_SW = 9;

	/** 共室分核查距离范围内最多保存的小区数量 */
	private int ADJ_COUNT_FOR_DIS_SF = 6;

	/** 室外非共站 4G小区室外时两小区之间的夹角 */
	private int ANGLE_FGZ = 60;

	/** 距离在各map中的key */
	private static final String DISTATCE_MAP_KEY = "DISTATCE_MAP_KEY";

	/** 存放在每个GSM小区中对应共站LTE列表的key */
	private static final String GSM_GZ_LTE_CELLS_LIST = "GSM_GZ_LTE_CELLS_LIST";

	/** 存放在每个GSM小区中对应核查距离范围内的LTE小区列表的KEY */
	private static final String GSM_TO_LTE_CELLS_FOR_DIS_LIST = "GSM_TO_LTE_CELLS_FOR_DIS_LIST";

	/** 存放在每个GSM小区中对应共站TD列表的key */
	private static final String GSM_GZ_TD_CELLS_LIST = "GSM_GZ_TD_CELLS_LIST";

	/** 存放在每个GSM小区中对应核查距离范围内的TD小区列表的KEY */
	private static final String GSM_TO_TD_CELLS_FOR_DIS_LIST = "GSM_TO_TD_CELLS_FOR_DIS_LIST";

	/** GSM应配LTE小区列表的KEY */
	private static final String GSM_YP_LTE_LIST = "GSM_YP_LTE_LIST";

	/** LTE小区漏配标示在LTE小区MAP的KEY */
	private static final String LTE_LP_FLAG = "LTE_LP_FLAG";

	/** GSM小区应配LTE频点在GSM小区MAP中的KEY */
	private static final String GSM_YP_EARFCN_LIST_KEY = "GSM_YP_EARFCN_LIST_KEY";

	/** GSM小区漏配LTE频点在GSM小区MAP中的KEY */
	private static final String GSM_LP_EARFCN_LIST_KEY = "GSM_LP_EARFCN_LIST_KEY";

	/** GSM共站漏配标示在map中的key */
	private static final String GSM_GZ_LP_KEY = "GSM_GZ_LP_KEY";

	private PreparedStatement ps = null;
	private PreparedStatement ps_info = null;
	private Connection con = null;

	/** 用于对邻区按照距离进行排序的方法 */
	Comparator<Map> comparator = new Comparator<Map>() {
		public int compare(Map o, Map o2) {
			double dis = Double.parseDouble(o.get(GsmToLteLP.DISTATCE_MAP_KEY)
					.toString());
			double dis2 = Double.parseDouble(o2
					.get(GsmToLteLP.DISTATCE_MAP_KEY).toString());
			if (dis < dis2) {
				return -1;
			} else if (dis > dis2) {
				return 1;
			} else {
				return 0;
			}
		}
	};

	private void init() throws IOException, SQLException {
		// DataSource.init();
		con = DataSource.getConnection();
		con.setAutoCommit(false);
		ps = con.prepareStatement(this.INSERT_LP);
		ps_info = con.prepareStatement(this.INSERT_LP_INFO);
	}

	private void close() throws IOException, SQLException {
		// DataSource.init();
		con.setAutoCommit(true);
		ps.close();
		ps_info.close();
		con.close();
	}

	private void commit() throws IOException, SQLException {
		// DataSource.init();

		ps.executeBatch();
		ps_info.executeBatch();
		con.commit();
		// con.close();
	}

	/**
	 * 查询gsm小区
	 * 
	 * @throws SQLException
	 * @throws ParseException
	 */
	private void selectGsmCells(String date, int subcomp) throws SQLException,
			ParseException {
		log.info("开始查询GSM小区信息");
		ResultSet rs = null;
		PreparedStatement ps = null;
		Connection con = DataSource.getConnection();
		ps = con.prepareStatement(this.GSM_SQL);
		ps.setDate(1, new Date(df.parse(date).getTime()));
		ps.setInt(2, subcomp);
		rs = ps.executeQuery();
		GSMCELLS = Tools.getValues(rs);
		rs.close();
		ps.close();
		con.close();
		log.info("GSM查询结束，GSM=" + this.GSMCELLS.size());
	}

	/**
	 * 查询TD小区
	 * 
	 * @throws SQLException
	 * @throws ParseException
	 */
	private void selectTdCells(String date, int subcomp) throws SQLException,
			ParseException {
		log.info("开始查询TD小区信息");
		ResultSet rs = null;
		PreparedStatement ps = null;
		Connection con = DataSource.getConnection();
		ps = con.prepareStatement(this.TD_SQL);
		ps.setInt(1, subcomp);
		rs = ps.executeQuery();
		TDCELLS = Tools.getValues(rs);
		rs.close();
		ps.close();
		con.close();
		log.info("TD查询结束，TD=" + this.TDCELLS.size());
	}

	/**
	 * 查询LTE小区
	 * 
	 * @throws SQLException
	 * @throws ParseException
	 */
	private void selectLteCells(String date, int subcomp) throws SQLException,
			ParseException {
		log.info("开始查询LTE小区信息");
		ResultSet rs = null;
		PreparedStatement ps = null;
		Connection con = DataSource.getConnection();
		ps = con.prepareStatement(this.LTE_SQL);
		ps.setDate(1, new Date(df.parse(date).getTime()));
		ps.setInt(2, subcomp);
		rs = ps.executeQuery();
		LTECELLS = Tools.getValues(rs);
		rs.close();
		ps.close();
		con.close();
		log.info("LTE查询结束，LTE=" + this.LTECELLS.size());

		/** 将list形式转化为map形式保存 */
		for (int i = 0; i < LTECELLS.size(); i++) {
			Map ltecell = LTECELLS.get(i);
			String enodeb = ltecell.get("I_ENODEB_ID").toString();
			String cellid = ltecell.get("I_CELL_ID").toString();
			this.LTECELLS_MAP.put(enodeb + "-" + cellid, ltecell);
		}
	}

	/**
	 * 计算LTE和TD与GSM小区的距离，保存共站和距离较近的小区列表,并入库
	 * 
	 * @throws SQLException
	 * @throws IOException
	 * @throws ParseException
	 */
	private void calculateAndInsert() throws IOException, SQLException,
			ParseException {
		this.init();

		for (int i = 0; i < GSMCELLS.size(); i++) {
			// long time1 = System.currentTimeMillis();
			// log.debug(time1);
			Map gsmcell = GSMCELLS.get(i);
			/** GSM小区室分标示 */
			String gsm_sf = gsmcell.get("I_SF").toString();
			/** 存放共站lte小区 */
			List<Map> gz_ltecells = new ArrayList<Map>();

			/** 存放核查距离范围内的lte小区 */
			List<Map> dis_ltecells = new ArrayList<Map>();

			/** 存放共站td小区 */
			List<Map> gz_tdcells = new ArrayList<Map>();

			/** 存放核查距离范围内的td小区 */
			// List<Map> dis_tdcells = new ArrayList<Map>();
			gsmcell.put(this.GSM_GZ_LTE_CELLS_LIST, gz_ltecells);
			gsmcell.put(this.GSM_TO_LTE_CELLS_FOR_DIS_LIST, dis_ltecells);
			gsmcell.put(this.GSM_GZ_TD_CELLS_LIST, gz_tdcells);
			// gsmcell.put(this.GSM_TO_TD_CELLS_FOR_DIS_LIST, dis_tdcells);

			/** 计算GSM与LTE小区距离，并保存共站lte小区和核查范围内lte小区 */
			for (int j = 0; j < LTECELLS.size(); j++) {
				Map ltecell = LTECELLS.get(j);
				/** 得到GSM和LTE小区之间的距离 */
				double dis = this.getDistatce(gsmcell, ltecell);
				/** 得到室分标示 */
				String lte_sf = ltecell.get("I_SF").toString();

				/** GSM小区为室分小区，且距离小区室分规定距离，将此lte小区加入gsm小区共站列表 */
				if (gsm_sf.equals("1") && lte_sf.equals("1")
						&& dis < this.GZ_DISTATCE_SF) {
					Map new_ltecell = new HashMap();
					new_ltecell.putAll(ltecell);
					new_ltecell.put(this.DISTATCE_MAP_KEY, dis);
					gz_ltecells.add(new_ltecell);
					ltecell.put(this.DISTATCE_MAP_KEY, dis);
					gz_ltecells.add(ltecell);
					/** GSM小区为室外小区，且距离小区室外规定距离，将此lte小区加入gsm小区共站列表 */
				} else if (gsm_sf.equals("0") && lte_sf.equals("0")
						&& dis < this.GZ_DISTATCE_SW) {
					Map new_ltecell = new HashMap();
					new_ltecell.putAll(ltecell);
					new_ltecell.put(this.DISTATCE_MAP_KEY, dis);
					gz_ltecells.add(new_ltecell);
					ltecell.put(this.DISTATCE_MAP_KEY, dis);
					gz_ltecells.add(ltecell);
					/** GSM小区为室外，且距离小区室外最大核查距离 */
				} else if (gsm_sf.equals("0") && dis < this.FGZ_DISTATCE) {
					Map new_ltecell = new HashMap();
					new_ltecell.putAll(ltecell);
					new_ltecell.put(this.DISTATCE_MAP_KEY, dis);
					dis_ltecells.add(new_ltecell);
					ltecell.put(this.DISTATCE_MAP_KEY, dis);
					dis_ltecells.add(ltecell);
				}
			}

			/** 计算GSM与TD小区距离，并保存共站TD小区和核查范围内TD小区 */
			/** 如果gsm为室外,并且无LTE共站,开始查找GSM和TD的共站 */
			if (gsm_sf.equals("0") && gz_ltecells.size() == 0) {
				for (int j = 0; j < TDCELLS.size(); j++) {
					Map tdcell = TDCELLS.get(j);
					String td_sf = tdcell.get("I_SF").toString();
					double dis = this.getDistatce(gsmcell, tdcell);
					/** 保存与gsm室外共站的td小区 */
					if (dis < this.GZ_DISTATCE_SW) {
						gz_tdcells.add(tdcell);
					}

				}
			}
			/** 如果gsm为室外，并且与TD LTE均无共站进行排序，对筛选出来的距离范围内的小区进行排续并删除多余的邻区 */
			// Collections.sort(gz_ltecells, comparator);
			if (gsm_sf.equals("0") && gz_ltecells.size() == 0
					&& gz_tdcells.size() == 0) {
				Collections.sort(dis_ltecells, comparator);
				/** 如果保存的小区数大于要求的最大小区数则删除 */
				if (dis_ltecells.size() > this.ADJ_COUNT_FOR_DIS_SW) {

					List removecells = new ArrayList();
					for (int x = dis_ltecells.size(); x > this.ADJ_COUNT_FOR_DIS_SW; x--) {
						removecells.add(dis_ltecells.get(x - 1));
					}
					dis_ltecells.removeAll(removecells);
				}
			}
			// long time2 = System.currentTimeMillis();
			// log.debug("距离筛选用时:" + (time2 - time1) / 1000);
			/** 计算应配 */
			this.calculateYPLP(gsmcell);
			// long time3 = System.currentTimeMillis();
			// log.debug("计算漏配用时:" + (time3 - time2) / 1000);
			this.insertGsmToLteLP(gsmcell);
			// long time4 = System.currentTimeMillis();
			// log.debug("入库用时:" + (time4 - time3) / 1000);

			/** 清空对象，释放内存 */
			gsmcell.clear();

			if (i != 0 && i % 1000 == 0) {
				this.commit();
			}
			if (i % 100 == 0) {
				log.debug("已完成:" + i);
			}
		}
		this.commit();
		this.close();
	}

	/** 计算应配漏配 */
	private void calculateYPLP(Map gsmcell) {

		/** LTE应配列表 */
		List<Map> yp_ltecell = null;
		String gsm_sf = gsmcell.get("I_SF").toString();

		List<Map> gz_ltecells = (List<Map>) gsmcell
				.get(this.GSM_GZ_LTE_CELLS_LIST);
		List<Map> gz_tdcells = (List<Map>) gsmcell
				.get(this.GSM_GZ_TD_CELLS_LIST);
		List<Map> dis_ltecells = (List<Map>) gsmcell
				.get(this.GSM_TO_LTE_CELLS_FOR_DIS_LIST);
		/*
		 * List<Map> dis_tdcells = (List<Map>) gsmcell
		 * .get(this.GSM_TO_TD_CELLS_FOR_DIS_LIST);
		 */

		/** 室外gsm小区 */
		if (gsm_sf.equals("0")) {

			if (gz_ltecells.size() > 0) {// 大于0表示LTE有共站
				yp_ltecell = calculateLteGZYP_SW(gsmcell, gz_ltecells);
				gsmcell.put(this.GSM_GZ_LP_KEY, "1");
			} else if (gz_tdcells.size() > 0)// 大于0表示TD有共站
			{
				yp_ltecell = calculateTDGZYP_SW(gsmcell, gz_tdcells);
				gsmcell.put(this.GSM_GZ_LP_KEY, "1");
			} else // 无共站情况
			{
				yp_ltecell = calculateFGZYP_SW(gsmcell, dis_ltecells);
				gsmcell.put(this.GSM_GZ_LP_KEY, "0");
			}
			/** 室内gsm小区 */
		} else if (gsm_sf.equals("1")) {
			yp_ltecell = this.calculateGZYP_SF(gsmcell, gz_ltecells);
			gsmcell.put(this.GSM_GZ_LP_KEY, "1");
		}
		/** 将计算好的应配lte小区放入gsm对象中 */
		gsmcell.put(this.GSM_YP_LTE_LIST, yp_ltecell);

		/** 将应配小区进行去重 */
		Map<String, Map> ypmap = new HashMap<String, Map>();
		for (int x = 0; x < yp_ltecell.size(); x++) {
			Map ypcell = yp_ltecell.get(x);
			String enodeb = ypcell.get("I_ENODEB_ID").toString();
			String cellid = ypcell.get("I_CELL_ID").toString();
			ypmap.put(enodeb + "-" + cellid, ypcell);
		}
		yp_ltecell.clear();
		yp_ltecell.addAll(ypmap.values());

		/** 设置漏配和应配相关属性 */
		/*
		 * log.debug("GSM小区" + gsmcell.get("I_CELL_ID") + "应配小区" +
		 * yp_ltecell.size());
		 */
		this.setLpYp(gsmcell);
		// log.debug("应配频点=" + gsmcell.get(GSM_YP_EARFCN_LIST_KEY));
		// log.debug("AA="+gsmcell.get("AA"));
	}

	/**
	 * 设置应配配和漏配相关信息和标示
	 * 
	 * @param earfcns
	 *            字符串格式earfcn,earfcn,earfcn
	 * @param ltecells
	 *            需要判断的lte小区列表
	 */
	private void setLpYp(Map gsmcell) {
		List<Map> ltecells = (List<Map>) gsmcell.get(this.GSM_YP_LTE_LIST);

		/** 已配频点列表 */
		String yip_str = (String) gsmcell.get("I_LTE_FREQ");
		String[] yip_list = yip_str == null ? new String[0] : yip_str
				.split(",");

		/** 应配频点列表 */
		Map<String, String> yingp_earfcns = new HashMap<String, String>();
		/** 应配频点列表，字符串形式 */
		String yingp_earfcns_str = "";
		/** 漏配频点列表，字符串形式 */
		String lp_earfcn_str = "";

		for (Map o : ltecells) {
			String earfcn = (String) o.get("EARFCN");
			yingp_earfcns.put(earfcn, earfcn);
		}

		List<String> lp_earfcns = new ArrayList<String>();

		/** 计算漏配 */
		for (String yingp : yingp_earfcns.values()) {
			boolean isLp = true;// 默认漏配
			for (String yip : yip_list) {
				// 如果应配和已配相等则不漏配
				if (yingp.equals(yip)) {
					isLp = false;
					break;
				}
			}
			/** 保存漏配列表 */
			if (isLp) {
				lp_earfcns.add(yingp);

				if (lp_earfcn_str.equals("")) {
					lp_earfcn_str = yingp;
				} else {
					lp_earfcn_str = yingp + "," + lp_earfcn_str;
				}
			}

			/** 保存应配列表的字符串形式 */
			if (yingp_earfcns_str.equals("")) {
				yingp_earfcns_str = yingp;
			} else {
				yingp_earfcns_str = yingp + "," + yingp_earfcns_str;
			}
		}

		/** 设置LTE邻区漏配标示 */

		for (Map o : ltecells) {
			String earfcn = (String) o.get("EARFCN");
			o.put(this.LTE_LP_FLAG, "0");
			for (String lpearfcn : lp_earfcns) {
				/** 如果小区频点为漏配频点则小区为漏配小区 */
				if (earfcn.equals(lpearfcn)) {
					o.put(this.LTE_LP_FLAG, "1");
				}
			}
		}

		// log.debug("yingp_earfcns_str=" + yingp_earfcns_str);
		// log.debug("lp_earfcn_str=" + lp_earfcn_str);
		/** 设置应配频点 */
		gsmcell.put(this.GSM_YP_EARFCN_LIST_KEY, yingp_earfcns_str);
		// gsmcell.put("AA", yingp_earfcns_str);

		/** 设置漏配频点 */
		gsmcell.put(GSM_LP_EARFCN_LIST_KEY, lp_earfcn_str);
	}

	/**
	 * 计算lte共站应配
	 */
	private List<Map> calculateLteGZYP_SW(Map gsmcell, List<Map> gz_ltecells) {
		// 方向角
		int gsm_direction = Integer.parseInt(gsmcell.get("I_ANTENNA_DIRECTION")
				.toString());
		List<Map> yp_ltecell = new ArrayList<Map>();
		for (int i = 0; i < gz_ltecells.size(); i++) {
			Map ltecell = gz_ltecells.get(i);
			Map ypcell = new HashMap();
			// ypcell.putAll(ltecell);
			// 首先应配列表加入共站小区
			yp_ltecell.add(ltecell);

			// 取得LTE小区的邻区，将LTE的邻区也加入到应配列表
			String lteadj_str = (String) ltecell.get("ADJ_LTE_LIST");
			if (lteadj_str != null && !lteadj_str.equals("")) {
				String[] lteadj_list = lteadj_str.split("/");
				List<Map> lteadjs = getLtecellsFromLteCellMap(lteadj_str);
				for (int x = 0; x < lteadjs.size(); x++) {
					Map lteadjcell = lteadjs.get(x);
					int lte_direction = Integer.parseInt(lteadjcell.get(
							"I_ANTENNA_DIRECTION").toString());

					/** gsm小区如果与共站lte小区的邻区方向角小于30度，则为应配邻区,加入应配列表 */
					if (Tools.calculateAngle(gsm_direction, lte_direction) < this.GZ_ANGLE) {
						this.setDistatce(gsmcell, lteadjcell);
						yp_ltecell.add(lteadjcell);
					}
				}

			}
		}
		return yp_ltecell;
	}

	/**
	 * 计算TD共站应配
	 */
	private List<Map> calculateTDGZYP_SW(Map gsmcell, List<Map> gz_tdcells) {
		// 方向角
		int gsm_direction = Integer.parseInt(gsmcell.get("I_ANTENNA_DIRECTION")
				.toString());
		List<Map> yp_ltecell = new ArrayList<Map>();
		for (int i = 0; i < gz_tdcells.size(); i++) {
			Map tdcell = gz_tdcells.get(i);
			// Map ypcell = new HashMap();
			// ypcell.putAll(tdcell);
			// 首先应配列表加入共站小区
			// yp_ltecell.add(ypcell);

			// 取得TD小区的LTE邻区，将LTE邻区也加入到应配列表
			String lteadj_str = (String) tdcell.get("ADJ_LTE_LIST");
			if (lteadj_str != null && !lteadj_str.equals("")) {
				String[] lteadj_list = lteadj_str.split(",");
				List<Map> lteadjs = getLtecellsFromLteCellMap(lteadj_str
						.replaceAll(",", "/").replaceAll("_", "-"));
				/** 设置gsm小区和应配lte小区距离，并设置为lte小区属性 */
				for (int x = 0; x < lteadjs.size(); x++) {
					this.setDistatce(gsmcell, lteadjs.get(x));
				}
				yp_ltecell.addAll(lteadjs);
			}
		}
		return yp_ltecell;
	}

	/**
	 * 计算GSM室外非共站情况的应配列表
	 * 
	 * @param gsmcell
	 *            为gsm小区
	 * @param dis_ltecells
	 *            核查距离范围内的lte小区列表
	 * 
	 */
	private List<Map> calculateFGZYP_SW(Map gsmcell, List<Map> dis_ltecells) {
		// 方向角
		String gsm_sf = gsmcell.get("I_SF").toString();
		List<Map> yp_ltecell = new ArrayList<Map>();
		for (int i = 0; i < dis_ltecells.size(); i++) {
			Map ltecell = dis_ltecells.get(i);
			/*
			 * Map ypcell = new HashMap(); ypcell.putAll(ltecell);
			 */
			/** 如果是室分直接加入应配 */
			if (gsm_sf.equals("1")) {
				// yp_ltecell.add(ypcell);
				yp_ltecell.add(ltecell);
			} else {
				/** 室外需要满足法线算法 */
				if (this.isGZYP(gsmcell, ltecell)) {
					// yp_ltecell.add(ypcell);
					yp_ltecell.add(ltecell);
				}
			}
		}
		return yp_ltecell;
	}

	/** 计算GSM和LTE共室分应配列表 */
	private List<Map> calculateGZYP_SF(Map gsmcell, List<Map> gz_ltecells) {
		List<Map> yp_ltecell = new ArrayList<Map>();
		for (int i = 0; i < gz_ltecells.size(); i++) {
			Map ltecell = gz_ltecells.get(i);
			Map ypcell = new HashMap();
			// ypcell.putAll(ltecell);
			/** 首先加入共室分的lte小区 */
			// yp_ltecell.add(ypcell);
			yp_ltecell.add(ltecell);
			String adjstr = (String) ltecell.get("ADJ_LTE_LIST");

			/** 得到该LTE小区的LTE邻区 */
			if (adjstr != null && !adjstr.equals("")) {
				List<Map> lteadjs = this.getLtecellsFromLteCellMap(adjstr);

				/** 计算lte邻区与gsm小区的距离，并设置到邻区属性中 */
				for (int x = 0; x < lteadjs.size(); x++) {
					Map lteadjcell = lteadjs.get(x);
					this.setDistatce(gsmcell, lteadjcell);
				}

				/** 按照距离对邻区列表进行排序 */
				Collections.sort(lteadjs, this.comparator);
				/** 如果保存的小区数大于要求的最大小区数则删除 */
				if (lteadjs.size() > this.ADJ_COUNT_FOR_DIS_SF) {

					List removecells = new ArrayList();
					for (int x = lteadjs.size(); x > this.ADJ_COUNT_FOR_DIS_SF; x--) {
						removecells.add(lteadjs.get(x - 1));
					}
					lteadjs.removeAll(removecells);
				}
				/** 计算完成后加入应配列表 */
				yp_ltecell.addAll(lteadjs);
			}
		}
		return yp_ltecell;
	}

	/** 根据法线算法，判断是否应配 */
	private boolean isGZYP(Map gsmcell, Map ltecell) {
		double longitude1 = Double.parseDouble(gsmcell.get("I_LONGITUDE")
				.toString());
		double latitude1 = Double.parseDouble(gsmcell.get("I_LONGITUDE")
				.toString());
		double longitude2 = Double.parseDouble(ltecell.get("I_LONGITUDE")
				.toString());
		double latitude2 = Double.parseDouble(ltecell.get("I_LONGITUDE")
				.toString());
		double direction1 = Double.parseDouble(gsmcell.get(
				"I_ANTENNA_DIRECTION").toString());
		double direction2 = Double.parseDouble(ltecell.get(
				"I_ANTENNA_DIRECTION").toString());
		double dis = this.getDistatce(gsmcell, ltecell);
		double jd1 = 0;
		double jd2 = 0;
		if (longitude2 > longitude1 && latitude2 > latitude1) {
			jd1 = Math.abs(direction1
					- (Math.atan(((longitude2 - longitude1) * 111194.996457729)
							/ ((latitude2 - latitude1) * 89842.9843869154))
							/ Math.PI * 180));
			jd2 = Math
					.abs(direction2
							- (180 + Math
									.atan(((longitude1 - longitude2) * 111194.996457729)
											/ ((latitude1 - latitude2) * 89842.9843869154))
									/ Math.PI * 180));
		} else if (longitude2 > longitude1 && latitude2 < latitude1) {
			jd1 = Math
					.abs(direction1
							- (180 - Math
									.atan(((longitude2 - longitude1) * 111194.996457729)
											/ ((latitude1 - latitude2) * 89842.9843869154))
									/ Math.PI * 180));
			jd2 = Math
					.abs(direction2
							- (360 - Math
									.atan(((longitude1 - longitude2) * 111194.996457729)
											/ ((latitude2 - latitude1) * 89842.9843869154))
									/ Math.PI * 180));
		} else if (longitude2 < longitude1 && latitude2 < latitude1) {
			jd1 = Math
					.abs(direction1
							- (180 + Math
									.atan(((longitude1 - longitude2) * 111194.996457729)
											/ ((latitude1 - latitude2) * 89842.9843869154))
									/ Math.PI * 180));
			jd2 = Math.abs(direction2
					- (Math.atan(((longitude2 - longitude1) * 111194.996457729)
							/ ((latitude2 - latitude1) * 89842.9843869154))
							/ Math.PI * 180));
		} else if (longitude2 < longitude1 && latitude2 > latitude1) {
			jd1 = Math
					.abs(direction1
							- (360 - Math
									.atan(((longitude1 - longitude2) * 111194.996457729)
											/ ((latitude2 - latitude1) * 89842.9843869154))
									/ Math.PI * 180));
			jd2 = Math
					.abs(direction2
							- (180 - Math
									.atan(((longitude2 - longitude1) * 111194.996457729)
											/ ((latitude1 - latitude2) * 89842.9843869154))
									/ Math.PI * 180));
		}
		jd1 = jd1 > 180 ? 360 - jd1 : jd1;
		jd2 = jd2 > 180 ? 360 - jd2 : jd2;
		if (jd1 + jd2 < this.ANGLE_FGZ * 2) {
			return true;
		} else if (jd1 <= this.ANGLE_FGZ && dis <= 1000
				& direction2 >= direction1 + 120 % 360
				&& direction2 <= direction1 + 240 % 360) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * 根据字符串得到指定的lte小区,字符串格式为'基站号-小区号/基站号-小区号' 返回的List中的map对象均未复制对象，非原始对象
	 */
	private List<Map> getLtecellsFromLteCellMap(String str) {
		List<Map> ltecells = new ArrayList<Map>();
		String[] lteadj_list = str.split("/");
		for (int x = 0; x < lteadj_list.length; x++) {
			Map ltecell = this.LTECELLS_MAP.get(lteadj_list[x]);
			if (ltecell != null) {
				/*
				 * Map newltecell = new HashMap(); newltecell.putAll(ltecell);
				 * ltecells.add(newltecell);
				 */
				ltecells.add(ltecell);
			}
		}
		return ltecells;
	}

	/** 计算两个小区之间的距离 */
	private static double getDistatce(Map cell1, Map cell2) {
		double lon1 = Double.parseDouble(cell1.get("I_LONGITUDE").toString());
		double lat1 = Double.parseDouble(cell1.get("I_LONGITUDE").toString());
		double lon2 = Double.parseDouble(cell2.get("I_LONGITUDE").toString());
		double lat2 = Double.parseDouble(cell2.get("I_LONGITUDE").toString());

		double dis = cfg.Tools.getDistatce(lat1, lat2, lon1, lon2);
		return dis;
	}

	/** 设置两个小区之间的距离, 将距离设置在从小区中 */
	private static void setDistatce(Map master, Map slave) {
		double dis = getDistatce(master, slave);
		slave.put(DISTATCE_MAP_KEY, dis);
		return;
	}

	/**
	 * 将计算结果输出到数据库
	 * 
	 * @throws SQLException
	 * @throws ParseException
	 */
	private void insertGsmToLteLP(Map gsmcell) throws SQLException,
			ParseException {
		String dtm_repdate = gsmcell.get("DTM_REPDATE").toString();
		ps.setDate(1, new Date(df.parse(dtm_repdate).getTime()));
		String i_subcomp_id = (String) gsmcell.get("I_SUBCOMP_ID");
		ps.setInt(2, Integer.parseInt(i_subcomp_id));
		String i_country_id = (String) gsmcell.get("I_COUNTRY_ID");
		ps.setInt(3, Integer.parseInt(i_country_id));
		String v_bts_eng_name = (String) gsmcell.get("V_BTS_ENG_NAME");
		if (v_bts_eng_name == null || v_bts_eng_name.equals("")) {
			ps.setNull(4, java.sql.Types.VARCHAR);
		} else {
			ps.setString(4, v_bts_eng_name);
		}
		String v_bts_chn_name = (String) gsmcell.get("V_BTS_CHN_NAME");
		if (v_bts_chn_name == null || v_bts_chn_name.equals("")) {
			ps.setNull(5, java.sql.Types.VARCHAR);
		} else {
			ps.setString(5, v_bts_chn_name);
		}
		String i_lac = (String) gsmcell.get("I_LAC");
		if (i_lac == null || i_lac.equals("")) {
			ps.setNull(6, java.sql.Types.VARCHAR);
		} else {
			ps.setString(6, i_lac);
		}
		String i_cell_id = (String) gsmcell.get("I_CELL_ID");
		if (i_cell_id == null || i_cell_id.equals("")) {
			ps.setNull(7, java.sql.Types.VARCHAR);
		} else {
			ps.setString(7, i_cell_id);
		}
		double i_longitude = Double.parseDouble(gsmcell.get("I_LONGITUDE")
				.toString());
		ps.setDouble(8, i_longitude);
		double i_latitude = Double.parseDouble(gsmcell.get("I_LATITUDE")
				.toString());
		ps.setDouble(9, i_latitude);
		String i_antenna_direction = (String) gsmcell
				.get("I_ANTENNA_DIRECTION");
		if (i_antenna_direction == null || i_antenna_direction.equals("")) {
			ps.setNull(10, java.sql.Types.VARCHAR);
		} else {
			ps.setString(10, i_antenna_direction);
		}

		String freq_band_in_use = (String) gsmcell.get("FREQ_BAND_IN_USE");
		if (freq_band_in_use == null || freq_band_in_use.equals("")) {
			ps.setNull(11, java.sql.Types.VARCHAR);
		} else {
			ps.setString(11, freq_band_in_use);
		}

		String i_bcch = (String) gsmcell.get("I_BCCH");
		if (i_bcch == null || i_bcch.equals("")) {
			ps.setNull(12, java.sql.Types.VARCHAR);
		} else {
			ps.setString(12, i_bcch);
		}
		String i_converage_id = (String) gsmcell.get("I_CONVERAGE_ID");
		if (i_converage_id == null || i_converage_id.equals("")) {
			ps.setNull(13, java.sql.Types.VARCHAR);
		} else {
			ps.setString(13, i_converage_id);
		}
		String v_rep_factory = (String) gsmcell.get("V_REP_FACTORY");
		if (v_rep_factory == null || v_rep_factory.equals("")) {
			ps.setNull(14, java.sql.Types.VARCHAR);
		} else {
			ps.setString(14, v_rep_factory);
		}
		String v_4gfreq_1 = (String) gsmcell.get("I_LTE_FREQ");
		if (v_4gfreq_1 == null || v_4gfreq_1.equals("")) {
			ps.setNull(15, java.sql.Types.VARCHAR);
		} else {
			ps.setString(15, v_4gfreq_1);
		}
		String v_4gfreq_2 = gsmcell.get(GSM_YP_EARFCN_LIST_KEY).toString();
		// log.debug("应配频点=" + v_4gfreq_2);
		if (v_4gfreq_2 == null || v_4gfreq_2.equals("")) {
			ps.setNull(16, java.sql.Types.VARCHAR);
		} else {
			ps.setString(16, v_4gfreq_2);
		}

		String v_4gfreq_3 = (String) gsmcell.get(GSM_LP_EARFCN_LIST_KEY);
		// log.debug("漏配频点=" + v_4gfreq_3);
		if (v_4gfreq_3 == null || v_4gfreq_3.equals("")) {
			ps.setNull(17, java.sql.Types.VARCHAR);
		} else {
			ps.setString(17, v_4gfreq_3);
		}
		String i_sf = (String) gsmcell.get("I_SF");
		if (i_sf == null || i_sf.equals("")) {
			ps.setNull(18, java.sql.Types.VARCHAR);
		} else {
			ps.setString(18, i_sf);
		}
		String i_country_part_id = (String) gsmcell.get("I_COUNTRY_PART_ID");
		if (i_country_part_id == null || i_country_part_id.equals("")) {
			ps.setNull(19, java.sql.Types.VARCHAR);
		} else {
			ps.setString(19, i_country_part_id);
		}
		String i_area_id = (String) gsmcell.get("I_AREA_ID");
		if (i_area_id == null || i_area_id.equals("")) {
			ps.setNull(20, java.sql.Types.VARCHAR);
		} else {
			ps.setString(20, i_area_id);
		}

		String i_gz = (String) gsmcell.get(this.GSM_GZ_LP_KEY);
		if (i_gz == null || i_gz.equals("")) {
			ps.setNull(21, java.sql.Types.VARCHAR);
		} else {
			ps.setString(21, i_gz);
		}
		ps.addBatch();

		List<Map> ypltecells = (List<Map>) gsmcell.get(this.GSM_YP_LTE_LIST);
		for (Map ltecell : ypltecells) {
			ps_info.setDate(1, new Date(df.parse(dtm_repdate).getTime()));
			ps_info.setInt(2, Integer.parseInt(i_subcomp_id));
			if (i_cell_id == null || i_cell_id.equals("")) {
				ps_info.setNull(3, java.sql.Types.VARCHAR);
			} else {
				ps_info.setString(3, i_cell_id);
			}
			String i_enodeb_id = (String) ltecell.get("I_ENODEB_ID");
			if (i_enodeb_id == null || i_enodeb_id.equals("")) {
				ps_info.setNull(4, java.sql.Types.VARCHAR);
			} else {
				ps_info.setString(4, i_enodeb_id);
			}
			String i_cell_id4g = (String) ltecell.get("I_CELL_ID");
			if (i_cell_id4g == null || i_cell_id4g.equals("")) {
				ps_info.setNull(5, java.sql.Types.VARCHAR);
			} else {
				ps_info.setString(5, i_cell_id4g);
			}
			String v_cell_ename = (String) ltecell.get("V_CELL_ENAME");
			if (v_cell_ename == null || v_cell_ename.equals("")) {
				ps_info.setNull(6, java.sql.Types.VARCHAR);
			} else {
				ps_info.setString(6, v_cell_ename);
			}
			String v_cell_cname = (String) ltecell.get("V_CELL_CNAME");
			if (v_cell_cname == null || v_cell_cname.equals("")) {
				ps_info.setNull(7, java.sql.Types.VARCHAR);
			} else {
				ps_info.setString(7, v_cell_cname);
			}
			String i_converage_id_4g = (String) ltecell.get("I_CONVERAGE_ID");
			if (i_converage_id_4g == null || i_converage_id_4g.equals("")) {
				ps_info.setNull(8, java.sql.Types.VARCHAR);
			} else {
				ps_info.setString(8, i_converage_id_4g);
			}
			ps_info.setDouble(9, i_longitude);
			ps_info.setDouble(10, i_latitude);
			double i_longitude_4g = Double.parseDouble(ltecell.get(
					"I_LONGITUDE").toString());
			ps_info.setDouble(11, i_longitude_4g);
			double i_latitude_4g = Double.parseDouble(ltecell.get("I_LATITUDE")
					.toString());
			ps_info.setDouble(12, i_latitude_4g);
			String i_antenna_direction_4g = (String) ltecell
					.get("I_ANTENNA_DIRECTION");
			if (i_antenna_direction_4g == null
					|| i_antenna_direction_4g.equals("")) {
				ps_info.setNull(13, java.sql.Types.VARCHAR);
			} else {
				ps_info.setString(13, i_antenna_direction_4g);
			}

			String i_4gfreq = (String) ltecell.get("EARFCN");
			if (i_4gfreq == null || i_4gfreq.equals("")) {
				ps_info.setNull(14, java.sql.Types.VARCHAR);
			} else {
				ps_info.setString(14, i_4gfreq);
			}
			double i_distance = Double.parseDouble(ltecell.get(
					this.DISTATCE_MAP_KEY).toString());
			ps_info.setDouble(15, i_distance);
			String i_sf_4g = (String) ltecell.get("I_SF");
			if (i_sf_4g == null || i_sf_4g.equals("")) {
				ps_info.setNull(16, java.sql.Types.VARCHAR);
			} else {
				ps_info.setString(16, i_sf_4g);
			}
			String i_country_part_id_4g = (String) ltecell
					.get("I_COUNTRY_PART_ID");
			if (i_country_part_id_4g == null || i_country_part_id_4g.equals("")) {
				ps_info.setNull(17, java.sql.Types.VARCHAR);
			} else {
				ps_info.setString(17, i_country_part_id_4g);
			}
			String i_area_id_4g = (String) ltecell.get("A_ID");
			if (i_area_id_4g == null || i_area_id_4g.equals("")) {
				ps_info.setNull(18, java.sql.Types.VARCHAR);
			} else {
				ps_info.setString(18, i_area_id_4g);
			}
			String i_lp = (String) ltecell.get(this.LTE_LP_FLAG);
			if (i_lp == null || i_lp.equals("")) {
				ps_info.setNull(19, java.sql.Types.VARCHAR);
			} else {
				ps_info.setString(19, i_lp);
			}
			ps_info.addBatch();
		}
	}

	/**
	 * 计算gsm漏配TD的程序入口
	 * 
	 * @throws ParseException
	 * @throws SQLException
	 * @throws IOException
	 */
	public void calculate(String date, int subcomp) throws SQLException,
			ParseException, IOException {

		log.info("开始计算分公司" + subcomp);
		this.selectGsmCells(date, subcomp);
		this.selectLteCells(date, subcomp);
		this.selectTdCells(date, subcomp);
		this.calculateAndInsert();
		log.info("完成分公司计算:" + subcomp);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String subcompid = args[0];
		String date = args[1];
		try {
			DataSource.init();
		} catch (Exception e1) {
			log.error("初始化数据源失败，程序退出", e1);
			System.exit(1);
		}
		if (args.length != 2) {
			log.error("参数错误");
			System.exit(1);
		}
		GsmToLteLP gsmlp = new GsmToLteLP();
		try {
			if (subcompid.equals("0")) {
				for (int i = 18; i > 0; i--) {
					gsmlp.calculate(date, i);
				}
			} else {
				gsmlp.calculate(date, Integer.parseInt(subcompid));
			}
		} catch (Exception e) {
			log.error("程序错误", e);
			System.exit(1);
		}
	}
}
