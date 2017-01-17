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

	/** LTEС����map��ʽ��ţ�keyΪ"��վ��-С����" */
	private Map<String, Map> LTECELLS_MAP = new HashMap<String, Map>();
	/** ���LTEС�� */
	private List<Map> LTECELLS = new ArrayList<Map>();

	/** �ҷֹ�վ����Ҫ��(��) */
	private int GZ_DISTATCE_SF = 50;

	/** ���⹲վ����Ҫ��(��) */
	private int GZ_DISTATCE_SW = 50;

	/** ���⹲վ�����ƫ��ֵ */
	private int GZ_ANGLE = 30;

	/** �ǹ�վ����Ҫ��(��) */
	private int FGZ_DISTATCE = 2000;

	/** ����ǹ�վ�˲���뷶Χ����ౣ���С������ */
	private int ADJ_COUNT_FOR_DIS_SW = 9;

	/** ���ҷֺ˲���뷶Χ����ౣ���С������ */
	private int ADJ_COUNT_FOR_DIS_SF = 6;

	/** ����ǹ�վ 4GС������ʱ��С��֮��ļн� */
	private int ANGLE_FGZ = 60;

	/** �����ڸ�map�е�key */
	private static final String DISTATCE_MAP_KEY = "DISTATCE_MAP_KEY";

	/** �����ÿ��GSMС���ж�Ӧ��վLTE�б��key */
	private static final String GSM_GZ_LTE_CELLS_LIST = "GSM_GZ_LTE_CELLS_LIST";

	/** �����ÿ��GSMС���ж�Ӧ�˲���뷶Χ�ڵ�LTEС���б��KEY */
	private static final String GSM_TO_LTE_CELLS_FOR_DIS_LIST = "GSM_TO_LTE_CELLS_FOR_DIS_LIST";

	/** �����ÿ��GSMС���ж�Ӧ��վTD�б��key */
	private static final String GSM_GZ_TD_CELLS_LIST = "GSM_GZ_TD_CELLS_LIST";

	/** �����ÿ��GSMС���ж�Ӧ�˲���뷶Χ�ڵ�TDС���б��KEY */
	private static final String GSM_TO_TD_CELLS_FOR_DIS_LIST = "GSM_TO_TD_CELLS_FOR_DIS_LIST";

	/** GSMӦ��LTEС���б��KEY */
	private static final String GSM_YP_LTE_LIST = "GSM_YP_LTE_LIST";

	/** LTEС��©���ʾ��LTEС��MAP��KEY */
	private static final String LTE_LP_FLAG = "LTE_LP_FLAG";

	/** GSMС��Ӧ��LTEƵ����GSMС��MAP�е�KEY */
	private static final String GSM_YP_EARFCN_LIST_KEY = "GSM_YP_EARFCN_LIST_KEY";

	/** GSMС��©��LTEƵ����GSMС��MAP�е�KEY */
	private static final String GSM_LP_EARFCN_LIST_KEY = "GSM_LP_EARFCN_LIST_KEY";

	/** GSM��վ©���ʾ��map�е�key */
	private static final String GSM_GZ_LP_KEY = "GSM_GZ_LP_KEY";

	private PreparedStatement ps = null;
	private PreparedStatement ps_info = null;
	private Connection con = null;

	/** ���ڶ��������վ����������ķ��� */
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
	 * ��ѯgsmС��
	 * 
	 * @throws SQLException
	 * @throws ParseException
	 */
	private void selectGsmCells(String date, int subcomp) throws SQLException,
			ParseException {
		log.info("��ʼ��ѯGSMС����Ϣ");
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
		log.info("GSM��ѯ������GSM=" + this.GSMCELLS.size());
	}

	/**
	 * ��ѯTDС��
	 * 
	 * @throws SQLException
	 * @throws ParseException
	 */
	private void selectTdCells(String date, int subcomp) throws SQLException,
			ParseException {
		log.info("��ʼ��ѯTDС����Ϣ");
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
		log.info("TD��ѯ������TD=" + this.TDCELLS.size());
	}

	/**
	 * ��ѯLTEС��
	 * 
	 * @throws SQLException
	 * @throws ParseException
	 */
	private void selectLteCells(String date, int subcomp) throws SQLException,
			ParseException {
		log.info("��ʼ��ѯLTEС����Ϣ");
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
		log.info("LTE��ѯ������LTE=" + this.LTECELLS.size());

		/** ��list��ʽת��Ϊmap��ʽ���� */
		for (int i = 0; i < LTECELLS.size(); i++) {
			Map ltecell = LTECELLS.get(i);
			String enodeb = ltecell.get("I_ENODEB_ID").toString();
			String cellid = ltecell.get("I_CELL_ID").toString();
			this.LTECELLS_MAP.put(enodeb + "-" + cellid, ltecell);
		}
	}

	/**
	 * ����LTE��TD��GSMС���ľ��룬���湲վ�;���Ͻ���С���б�,�����
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
			/** GSMС���ҷֱ�ʾ */
			String gsm_sf = gsmcell.get("I_SF").toString();
			/** ��Ź�վlteС�� */
			List<Map> gz_ltecells = new ArrayList<Map>();

			/** ��ź˲���뷶Χ�ڵ�lteС�� */
			List<Map> dis_ltecells = new ArrayList<Map>();

			/** ��Ź�վtdС�� */
			List<Map> gz_tdcells = new ArrayList<Map>();

			/** ��ź˲���뷶Χ�ڵ�tdС�� */
			// List<Map> dis_tdcells = new ArrayList<Map>();
			gsmcell.put(this.GSM_GZ_LTE_CELLS_LIST, gz_ltecells);
			gsmcell.put(this.GSM_TO_LTE_CELLS_FOR_DIS_LIST, dis_ltecells);
			gsmcell.put(this.GSM_GZ_TD_CELLS_LIST, gz_tdcells);
			// gsmcell.put(this.GSM_TO_TD_CELLS_FOR_DIS_LIST, dis_tdcells);

			/** ����GSM��LTEС�����룬�����湲վlteС���ͺ˲鷶Χ��lteС�� */
			for (int j = 0; j < LTECELLS.size(); j++) {
				Map ltecell = LTECELLS.get(j);
				/** �õ�GSM��LTEС��֮��ľ��� */
				double dis = this.getDistatce(gsmcell, ltecell);
				/** �õ��ҷֱ�ʾ */
				String lte_sf = ltecell.get("I_SF").toString();

				/** GSMС��Ϊ�ҷ�С�����Ҿ���С���ҷֹ涨���룬����lteС������gsmС����վ�б� */
				if (gsm_sf.equals("1") && lte_sf.equals("1")
						&& dis < this.GZ_DISTATCE_SF) {
					Map new_ltecell = new HashMap();
					new_ltecell.putAll(ltecell);
					new_ltecell.put(this.DISTATCE_MAP_KEY, dis);
					gz_ltecells.add(new_ltecell);
					ltecell.put(this.DISTATCE_MAP_KEY, dis);
					gz_ltecells.add(ltecell);
					/** GSMС��Ϊ����С�����Ҿ���С������涨���룬����lteС������gsmС����վ�б� */
				} else if (gsm_sf.equals("0") && lte_sf.equals("0")
						&& dis < this.GZ_DISTATCE_SW) {
					Map new_ltecell = new HashMap();
					new_ltecell.putAll(ltecell);
					new_ltecell.put(this.DISTATCE_MAP_KEY, dis);
					gz_ltecells.add(new_ltecell);
					ltecell.put(this.DISTATCE_MAP_KEY, dis);
					gz_ltecells.add(ltecell);
					/** GSMС��Ϊ���⣬�Ҿ���С���������˲���� */
				} else if (gsm_sf.equals("0") && dis < this.FGZ_DISTATCE) {
					Map new_ltecell = new HashMap();
					new_ltecell.putAll(ltecell);
					new_ltecell.put(this.DISTATCE_MAP_KEY, dis);
					dis_ltecells.add(new_ltecell);
					ltecell.put(this.DISTATCE_MAP_KEY, dis);
					dis_ltecells.add(ltecell);
				}
			}

			/** ����GSM��TDС�����룬�����湲վTDС���ͺ˲鷶Χ��TDС�� */
			/** ���gsmΪ����,������LTE��վ,��ʼ����GSM��TD�Ĺ�վ */
			if (gsm_sf.equals("0") && gz_ltecells.size() == 0) {
				for (int j = 0; j < TDCELLS.size(); j++) {
					Map tdcell = TDCELLS.get(j);
					String td_sf = tdcell.get("I_SF").toString();
					double dis = this.getDistatce(gsmcell, tdcell);
					/** ������gsm���⹲վ��tdС�� */
					if (dis < this.GZ_DISTATCE_SW) {
						gz_tdcells.add(tdcell);
					}

				}
			}
			/** ���gsmΪ���⣬������TD LTE���޹�վ�������򣬶�ɸѡ�����ľ��뷶Χ�ڵ�С������������ɾ����������� */
			// Collections.sort(gz_ltecells, comparator);
			if (gsm_sf.equals("0") && gz_ltecells.size() == 0
					&& gz_tdcells.size() == 0) {
				Collections.sort(dis_ltecells, comparator);
				/** ��������С��������Ҫ������С������ɾ�� */
				if (dis_ltecells.size() > this.ADJ_COUNT_FOR_DIS_SW) {

					List removecells = new ArrayList();
					for (int x = dis_ltecells.size(); x > this.ADJ_COUNT_FOR_DIS_SW; x--) {
						removecells.add(dis_ltecells.get(x - 1));
					}
					dis_ltecells.removeAll(removecells);
				}
			}
			// long time2 = System.currentTimeMillis();
			// log.debug("����ɸѡ��ʱ:" + (time2 - time1) / 1000);
			/** ����Ӧ�� */
			this.calculateYPLP(gsmcell);
			// long time3 = System.currentTimeMillis();
			// log.debug("����©����ʱ:" + (time3 - time2) / 1000);
			this.insertGsmToLteLP(gsmcell);
			// long time4 = System.currentTimeMillis();
			// log.debug("�����ʱ:" + (time4 - time3) / 1000);

			/** ��ն����ͷ��ڴ� */
			gsmcell.clear();

			if (i != 0 && i % 1000 == 0) {
				this.commit();
			}
			if (i % 100 == 0) {
				log.debug("�����:" + i);
			}
		}
		this.commit();
		this.close();
	}

	/** ����Ӧ��©�� */
	private void calculateYPLP(Map gsmcell) {

		/** LTEӦ���б� */
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

		/** ����gsmС�� */
		if (gsm_sf.equals("0")) {

			if (gz_ltecells.size() > 0) {// ����0��ʾLTE�й�վ
				yp_ltecell = calculateLteGZYP_SW(gsmcell, gz_ltecells);
				gsmcell.put(this.GSM_GZ_LP_KEY, "1");
			} else if (gz_tdcells.size() > 0)// ����0��ʾTD�й�վ
			{
				yp_ltecell = calculateTDGZYP_SW(gsmcell, gz_tdcells);
				gsmcell.put(this.GSM_GZ_LP_KEY, "1");
			} else // �޹�վ���
			{
				yp_ltecell = calculateFGZYP_SW(gsmcell, dis_ltecells);
				gsmcell.put(this.GSM_GZ_LP_KEY, "0");
			}
			/** ����gsmС�� */
		} else if (gsm_sf.equals("1")) {
			yp_ltecell = this.calculateGZYP_SF(gsmcell, gz_ltecells);
			gsmcell.put(this.GSM_GZ_LP_KEY, "1");
		}
		/** ������õ�Ӧ��lteС������gsm������ */
		gsmcell.put(this.GSM_YP_LTE_LIST, yp_ltecell);

		/** ��Ӧ��С������ȥ�� */
		Map<String, Map> ypmap = new HashMap<String, Map>();
		for (int x = 0; x < yp_ltecell.size(); x++) {
			Map ypcell = yp_ltecell.get(x);
			String enodeb = ypcell.get("I_ENODEB_ID").toString();
			String cellid = ypcell.get("I_CELL_ID").toString();
			ypmap.put(enodeb + "-" + cellid, ypcell);
		}
		yp_ltecell.clear();
		yp_ltecell.addAll(ypmap.values());

		/** ����©���Ӧ��������� */
		/*
		 * log.debug("GSMС��" + gsmcell.get("I_CELL_ID") + "Ӧ��С��" +
		 * yp_ltecell.size());
		 */
		this.setLpYp(gsmcell);
		// log.debug("Ӧ��Ƶ��=" + gsmcell.get(GSM_YP_EARFCN_LIST_KEY));
		// log.debug("AA="+gsmcell.get("AA"));
	}

	/**
	 * ����Ӧ�����©�������Ϣ�ͱ�ʾ
	 * 
	 * @param earfcns
	 *            �ַ�����ʽearfcn,earfcn,earfcn
	 * @param ltecells
	 *            ��Ҫ�жϵ�lteС���б�
	 */
	private void setLpYp(Map gsmcell) {
		List<Map> ltecells = (List<Map>) gsmcell.get(this.GSM_YP_LTE_LIST);

		/** ����Ƶ���б� */
		String yip_str = (String) gsmcell.get("I_LTE_FREQ");
		String[] yip_list = yip_str == null ? new String[0] : yip_str
				.split(",");

		/** Ӧ��Ƶ���б� */
		Map<String, String> yingp_earfcns = new HashMap<String, String>();
		/** Ӧ��Ƶ���б��ַ�����ʽ */
		String yingp_earfcns_str = "";
		/** ©��Ƶ���б��ַ�����ʽ */
		String lp_earfcn_str = "";

		for (Map o : ltecells) {
			String earfcn = (String) o.get("EARFCN");
			yingp_earfcns.put(earfcn, earfcn);
		}

		List<String> lp_earfcns = new ArrayList<String>();

		/** ����©�� */
		for (String yingp : yingp_earfcns.values()) {
			boolean isLp = true;// Ĭ��©��
			for (String yip : yip_list) {
				// ���Ӧ������������©��
				if (yingp.equals(yip)) {
					isLp = false;
					break;
				}
			}
			/** ����©���б� */
			if (isLp) {
				lp_earfcns.add(yingp);

				if (lp_earfcn_str.equals("")) {
					lp_earfcn_str = yingp;
				} else {
					lp_earfcn_str = yingp + "," + lp_earfcn_str;
				}
			}

			/** ����Ӧ���б���ַ�����ʽ */
			if (yingp_earfcns_str.equals("")) {
				yingp_earfcns_str = yingp;
			} else {
				yingp_earfcns_str = yingp + "," + yingp_earfcns_str;
			}
		}

		/** ����LTE����©���ʾ */

		for (Map o : ltecells) {
			String earfcn = (String) o.get("EARFCN");
			o.put(this.LTE_LP_FLAG, "0");
			for (String lpearfcn : lp_earfcns) {
				/** ���С��Ƶ��Ϊ©��Ƶ����С��Ϊ©��С�� */
				if (earfcn.equals(lpearfcn)) {
					o.put(this.LTE_LP_FLAG, "1");
				}
			}
		}

		// log.debug("yingp_earfcns_str=" + yingp_earfcns_str);
		// log.debug("lp_earfcn_str=" + lp_earfcn_str);
		/** ����Ӧ��Ƶ�� */
		gsmcell.put(this.GSM_YP_EARFCN_LIST_KEY, yingp_earfcns_str);
		// gsmcell.put("AA", yingp_earfcns_str);

		/** ����©��Ƶ�� */
		gsmcell.put(GSM_LP_EARFCN_LIST_KEY, lp_earfcn_str);
	}

	/**
	 * ����lte��վӦ��
	 */
	private List<Map> calculateLteGZYP_SW(Map gsmcell, List<Map> gz_ltecells) {
		// �����
		int gsm_direction = Integer.parseInt(gsmcell.get("I_ANTENNA_DIRECTION")
				.toString());
		List<Map> yp_ltecell = new ArrayList<Map>();
		for (int i = 0; i < gz_ltecells.size(); i++) {
			Map ltecell = gz_ltecells.get(i);
			Map ypcell = new HashMap();
			// ypcell.putAll(ltecell);
			// ����Ӧ���б���빲վС��
			yp_ltecell.add(ltecell);

			// ȡ��LTEС������������LTE������Ҳ���뵽Ӧ���б�
			String lteadj_str = (String) ltecell.get("ADJ_LTE_LIST");
			if (lteadj_str != null && !lteadj_str.equals("")) {
				String[] lteadj_list = lteadj_str.split("/");
				List<Map> lteadjs = getLtecellsFromLteCellMap(lteadj_str);
				for (int x = 0; x < lteadjs.size(); x++) {
					Map lteadjcell = lteadjs.get(x);
					int lte_direction = Integer.parseInt(lteadjcell.get(
							"I_ANTENNA_DIRECTION").toString());

					/** gsmС������빲վlteС�������������С��30�ȣ���ΪӦ������,����Ӧ���б� */
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
	 * ����TD��վӦ��
	 */
	private List<Map> calculateTDGZYP_SW(Map gsmcell, List<Map> gz_tdcells) {
		// �����
		int gsm_direction = Integer.parseInt(gsmcell.get("I_ANTENNA_DIRECTION")
				.toString());
		List<Map> yp_ltecell = new ArrayList<Map>();
		for (int i = 0; i < gz_tdcells.size(); i++) {
			Map tdcell = gz_tdcells.get(i);
			// Map ypcell = new HashMap();
			// ypcell.putAll(tdcell);
			// ����Ӧ���б���빲վС��
			// yp_ltecell.add(ypcell);

			// ȡ��TDС����LTE��������LTE����Ҳ���뵽Ӧ���б�
			String lteadj_str = (String) tdcell.get("ADJ_LTE_LIST");
			if (lteadj_str != null && !lteadj_str.equals("")) {
				String[] lteadj_list = lteadj_str.split(",");
				List<Map> lteadjs = getLtecellsFromLteCellMap(lteadj_str
						.replaceAll(",", "/").replaceAll("_", "-"));
				/** ����gsmС����Ӧ��lteС�����룬������ΪlteС������ */
				for (int x = 0; x < lteadjs.size(); x++) {
					this.setDistatce(gsmcell, lteadjs.get(x));
				}
				yp_ltecell.addAll(lteadjs);
			}
		}
		return yp_ltecell;
	}

	/**
	 * ����GSM����ǹ�վ�����Ӧ���б�
	 * 
	 * @param gsmcell
	 *            ΪgsmС��
	 * @param dis_ltecells
	 *            �˲���뷶Χ�ڵ�lteС���б�
	 * 
	 */
	private List<Map> calculateFGZYP_SW(Map gsmcell, List<Map> dis_ltecells) {
		// �����
		String gsm_sf = gsmcell.get("I_SF").toString();
		List<Map> yp_ltecell = new ArrayList<Map>();
		for (int i = 0; i < dis_ltecells.size(); i++) {
			Map ltecell = dis_ltecells.get(i);
			/*
			 * Map ypcell = new HashMap(); ypcell.putAll(ltecell);
			 */
			/** ������ҷ�ֱ�Ӽ���Ӧ�� */
			if (gsm_sf.equals("1")) {
				// yp_ltecell.add(ypcell);
				yp_ltecell.add(ltecell);
			} else {
				/** ������Ҫ���㷨���㷨 */
				if (this.isGZYP(gsmcell, ltecell)) {
					// yp_ltecell.add(ypcell);
					yp_ltecell.add(ltecell);
				}
			}
		}
		return yp_ltecell;
	}

	/** ����GSM��LTE���ҷ�Ӧ���б� */
	private List<Map> calculateGZYP_SF(Map gsmcell, List<Map> gz_ltecells) {
		List<Map> yp_ltecell = new ArrayList<Map>();
		for (int i = 0; i < gz_ltecells.size(); i++) {
			Map ltecell = gz_ltecells.get(i);
			Map ypcell = new HashMap();
			// ypcell.putAll(ltecell);
			/** ���ȼ��빲�ҷֵ�lteС�� */
			// yp_ltecell.add(ypcell);
			yp_ltecell.add(ltecell);
			String adjstr = (String) ltecell.get("ADJ_LTE_LIST");

			/** �õ���LTEС����LTE���� */
			if (adjstr != null && !adjstr.equals("")) {
				List<Map> lteadjs = this.getLtecellsFromLteCellMap(adjstr);

				/** ����lte������gsmС���ľ��룬�����õ����������� */
				for (int x = 0; x < lteadjs.size(); x++) {
					Map lteadjcell = lteadjs.get(x);
					this.setDistatce(gsmcell, lteadjcell);
				}

				/** ���վ���������б�������� */
				Collections.sort(lteadjs, this.comparator);
				/** ��������С��������Ҫ������С������ɾ�� */
				if (lteadjs.size() > this.ADJ_COUNT_FOR_DIS_SF) {

					List removecells = new ArrayList();
					for (int x = lteadjs.size(); x > this.ADJ_COUNT_FOR_DIS_SF; x--) {
						removecells.add(lteadjs.get(x - 1));
					}
					lteadjs.removeAll(removecells);
				}
				/** ������ɺ����Ӧ���б� */
				yp_ltecell.addAll(lteadjs);
			}
		}
		return yp_ltecell;
	}

	/** ���ݷ����㷨���ж��Ƿ�Ӧ�� */
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
	 * �����ַ����õ�ָ����lteС��,�ַ�����ʽΪ'��վ��-С����/��վ��-С����' ���ص�List�е�map�����δ���ƶ��󣬷�ԭʼ����
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

	/** ��������С��֮��ľ��� */
	private static double getDistatce(Map cell1, Map cell2) {
		double lon1 = Double.parseDouble(cell1.get("I_LONGITUDE").toString());
		double lat1 = Double.parseDouble(cell1.get("I_LONGITUDE").toString());
		double lon2 = Double.parseDouble(cell2.get("I_LONGITUDE").toString());
		double lat2 = Double.parseDouble(cell2.get("I_LONGITUDE").toString());

		double dis = cfg.Tools.getDistatce(lat1, lat2, lon1, lon2);
		return dis;
	}

	/** ��������С��֮��ľ���, �����������ڴ�С���� */
	private static void setDistatce(Map master, Map slave) {
		double dis = getDistatce(master, slave);
		slave.put(DISTATCE_MAP_KEY, dis);
		return;
	}

	/**
	 * ����������������ݿ�
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
		// log.debug("Ӧ��Ƶ��=" + v_4gfreq_2);
		if (v_4gfreq_2 == null || v_4gfreq_2.equals("")) {
			ps.setNull(16, java.sql.Types.VARCHAR);
		} else {
			ps.setString(16, v_4gfreq_2);
		}

		String v_4gfreq_3 = (String) gsmcell.get(GSM_LP_EARFCN_LIST_KEY);
		// log.debug("©��Ƶ��=" + v_4gfreq_3);
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
	 * ����gsm©��TD�ĳ������
	 * 
	 * @throws ParseException
	 * @throws SQLException
	 * @throws IOException
	 */
	public void calculate(String date, int subcomp) throws SQLException,
			ParseException, IOException {

		log.info("��ʼ����ֹ�˾" + subcomp);
		this.selectGsmCells(date, subcomp);
		this.selectLteCells(date, subcomp);
		this.selectTdCells(date, subcomp);
		this.calculateAndInsert();
		log.info("��ɷֹ�˾����:" + subcomp);
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
			log.error("��ʼ������Դʧ�ܣ������˳�", e1);
			System.exit(1);
		}
		if (args.length != 2) {
			log.error("��������");
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
			log.error("�������", e);
			System.exit(1);
		}
	}
}
