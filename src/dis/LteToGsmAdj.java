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
 * @author �»� �������õ�4GС����2GƵ�㣬2GС����Ϣ ��������CSFBר��˲�ģ���С��������������ȡ���ݣ���Ӧ�Լ��ż��2015
 * @param args
 */
public class LteToGsmAdj {
	protected static final Log log = LogFactory.getLog(LteToGsmAdj.class);

	/** gsmС��sql* */
	private static String gsm_cell = "SELECT T.V_MSC,T.I_SUBCOMP_ID I_SUBCOMP_ID,T.I_COUNTRY_ID I_COUNTRY_ID,T.V_BSC V_BSC,T.V_CGI V_CGI,T.I_CELL_ID I_CELL_ID,TO_CHAR(T.DTM_REPDATE,'YYYY-MM-DD') DTM_REPDATE,T.I_BCCH I_BCCH,T.I_LAC I_LAC,CONCAT(T.I_NCC,T.I_BCC)V_BSIC,T.V_BTS_ENG_NAME V_BTS_ENG_NAME,T.V_BTS_CHN_NAME V_BTS_CHN_NAME,T.IS_INDOOR_DISTRIBUTION I_SF,T.I_ANTENNA_DIRECTION I_ANTENNA_DIRECTION,T.I_LONGITUDE I_LONGITUDE,T.I_LATITUDE I_LATITUDE,T.V_ADJCELL V_ADJCELL,T.V_TD_ADJ V_ADJ_TD_CELL FROM GSM_CM_CELL_D T "
			+ "WHERE T.DTM_REPDATE=TRUNC(SYSDATE-1) AND T.I_BCCH IS NOT NULL AND T.I_LONGITUDE IS NOT NULL AND T.I_LATITUDE IS NOT NULL";

	/** lteС��sql* */
	private static String lte_cell = "SELECT T.I_SUBCOMP_ID,T.I_COUNTRY_ID,T.I_ENODEB_ID,T.I_CELL_ID,T.V_CELL_CNAME,T.I_LONGITUDE,T.I_LATITUDE,T.V_ADJ_BCCH_LIST,T.I_SF,T.V_FACTORY FROM T_LTE_GSM_RELATION_OMIT T WHERE T.V_ADJ_BCCH_LIST IS NOT NULL";

	private static String insert_lte_enb = "INSERT INTO T_LTE_GSM_RELATION_OMIT_INFO(DTM_REPDATE,V_PROV_NAME,I_SUBCOMP_ID,I_COUNTRY_ID,I_CELL_ID,I_ENODEB_ID,V_CELL_CNAME,I_LONGITUDE,I_LATITUDE,I_SF,V_FACTORY,V_BSC,I_CELL_ID1,V_CGI,I_BCCH,I_LAC,V_BTS_CHN_NAME,V_BTS_ENG_NAME,V_BSIC,I_LONGITUDE1,I_LATITUDE1,IS_INDOOR_DISTRIBUTION,I_ANTENNA_DIRECTION,I_DISTANCE,I_LOST_BCCH,I_SUBCOMP_ID1,I_COUNTRY_ID1,V_CHECK,I_LOST_ADJ) VALUES(TRUNC(SYSDATE-1),'����ʡ',?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
	private static final String dateFormat = "yyyyMMdd";
	private static final SimpleDateFormat df = new SimpleDateFormat(dateFormat);

	/** ���gsmƵ���б� */
	private static List bcchs = new ArrayList<String>();

	/***************************************************************************
	 * ��ʼ��gsmƵ���б�
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
	}

	public static void main(String[] args) {
		try {
			DataSource.init();
		} catch (Exception e1) {
			log.error("��ʼ������Դʧ�ܣ������˳�", e1);
			System.exit(1);
		}
		try {
			List<Map> gsmlist = getGSMCellInfo();
			Map<String, List> bcchGsmMap = bcchGsmInit(gsmlist);
			List<Map> ltelist = getLteCell();
			ltelist=addLteToGsmAdj(ltelist,bcchGsmMap);
			int num=insertLteToGsmInfo(ltelist);
			log.info("�����"+num);
			
		} catch (Exception e) {
			log.error("�쳣�˳�", e);
		}
	}

	/***************************************************************************
	 * 
	 * ��ѯ����GSMС��
	 * 
	 * @throws SQLException
	 * @throws ParseException
	 **************************************************************************/
	private static List<Map> getGSMCellInfo() throws SQLException,
			ParseException {
		log.info("��ʼ��ѯGSMС����Ϣ");
		ResultSet rs = null;
		PreparedStatement ps = null;
		Connection con = DataSource.getConnection();
		ps = con.prepareStatement(gsm_cell);
		rs = ps.executeQuery();
		List<Map> gsmcells = Tools.getValues(rs);
		rs.close();
		ps.close();
		con.close();
		log.info("GSMС����ѯ����"+gsmcells.size());
		return gsmcells;
	}

	/**
	 * 
	 * ���ݷֹ�˾id��bcch����gsmС���ֳɲ�ͬ���飬���map
	 */
	private static Map<String, List> bcchGsmInit(List<Map> gsmcells) {
		log.info("��ʼ��bcch������gsmС��");
		Map<String, List> bcchMap = new HashMap<String, List>();
		/** ��ʼ����Ų�ͬbcchС����list���� */
		for (int i = 0; i < bcchs.size(); i++) {
			for (int j = 1; j <= 18; j++) {// �ֹ�˾ID,18���ֹ�˾
				List bcchcells = new ArrayList<Map>();
				bcchMap.put(j + "_" + bcchs.get(i), bcchcells);
			}
		}

		/***********************************************************************
		 * ѭ��gsmС���б�����ͬ�ֹ�˾��bcch��С����ŵ���ͬ��list��
		 **********************************************************************/
		for (int i = 0; i < gsmcells.size(); i++) {
			Map cell = gsmcells.get(i);
			String i_subcomp_id = cell.get("I_SUBCOMP_ID").toString();
			String i_bcch = cell.get("I_BCCH").toString();
			String key = i_subcomp_id + "_" + i_bcch;
			List bcchcells = bcchMap.get(key);
			bcchcells.add(cell);
		}
		log.info("bcch�����Ž���");
		return bcchMap;
	}

	/***************************************************************************
	 * 
	 * ���ݷֹ�˾id��ѯLte��վ����
	 * 
	 * @throws SQLException
	 * @throws ParseException
	 **************************************************************************/
	private static List<Map> getLteCell() throws SQLException, ParseException {
		log.info("��ѯLTEС����Ϣ");
		ResultSet rs = null;
		PreparedStatement ps = null;
		Connection con = DataSource.getConnection();
		ps = con.prepareStatement(lte_cell);
		rs = ps.executeQuery();
		List<Map> ltecell = Tools.getValues(rs);
		rs.close();
		ps.close();
		con.close();
		log.debug("LTEС����ѯ���"+ltecell.size());
		return ltecell;
	}

	/***************************************************************************
	 * ��Ƶ��ת��Ϊ��������ŵ�lteС��������
	 * 
	 */
	private static List<Map> addLteToGsmAdj(List<Map> ltelist,
			Map<String, List> bcchGsmMap) {
		for (int i = 0; i < ltelist.size(); i++) {
			Map ltecell = ltelist.get(i);
			List<Map> gsmadjs = new ArrayList<Map>();
			ltecell.put("GSM_ADJ", gsmadjs);
			String i_subcomp_id = ltecell.get("I_SUBCOMP_ID").toString();
			String adjbcchlist = ltecell.get("V_ADJ_BCCH_LIST").toString();
			String[] bcchs = adjbcchlist.split("/");
			double ltelon = Double.parseDouble(ltecell.get("I_LONGITUDE")
					.toString());
			double ltelat = Double.parseDouble(ltecell.get("I_LATITUDE")
					.toString());
			for (int j = 0; j < bcchs.length; j++) {
				String key = i_subcomp_id + "_" + bcchs[j];
				List<Map> bcchGsmlist = bcchGsmMap.get(key);
				Map min_gsmcell = null;// �����С�����gsmС��
				double min_dis = -1d;// �����С����
				for (int k = 0; k < bcchGsmlist.size(); k++) {
					Map gsmcell = bcchGsmlist.get(k);
					double gsmlon = Double.parseDouble(gsmcell.get(
							"I_LONGITUDE").toString());
					double gsmlat = Double.parseDouble(gsmcell
							.get("I_LATITUDE").toString());
					// �������
					double dis = Tools.getDistatce(gsmlat, ltelat, gsmlon,
							ltelon);

					/***********************************************************
					 * ��С������ڵ�ǰС�������ǣ�����С������ڵ�ǰ��С���룬�����滻��С����С��
					 **********************************************************/
					if (min_dis == -1 || min_dis > dis) {
						min_dis = dis;
						min_gsmcell = gsmcell;
					}
				}
				/**
				 * ������������
				 */
				if (min_dis != -1) {
					gsmadjs.add(min_gsmcell);
				}
			}
		}
		return ltelist;
	}
	
	/***************************************************************************
	 * ��lte��վ���뵽���ݱ���T_LTE_GSM_RELATION_OMIT_INFO ����ִ������
	 * 
	 * @throws SQLException
	 * @throws ParseException
	 **************************************************************************/
	private static int insertLteToGsmInfo(List<Map> ltelist)
			throws SQLException, ParseException {
		PreparedStatement ps = null;
		Connection con = DataSource.getConnection();
		con.setAutoCommit(false);
		int num = 0;
		ps = con.prepareStatement(insert_lte_enb);
		for (int i = 0; i < ltelist.size(); i++) {
			// I_ENODEB_ID,I_SUBCOMP_ID,I_COUNTRY_ID,I_COUNTRY_PART_ID,I_GSM_NUM,DTM_REPDATE
			Map ltecell = ltelist.get(i);
			List<Map> gsmadjs=(List)ltecell.get("GSM_ADJ");
			String i_subcomp_id=(String)ltecell.get("I_SUBCOMP_ID");
			String i_country_id=(String)ltecell.get("I_COUNTRY_ID");
			String i_cell_id=(String)ltecell.get("I_CELL_ID");
			String i_enodeb_id=(String)ltecell.get("I_ENODEB_ID");
			String v_cell_cname=(String)ltecell.get("V_CELL_CNAME");
			String i_longitude=(String)ltecell.get("I_LONGITUDE");
			String i_latitude=(String)ltecell.get("I_LATITUDE");
			String i_sf=(String)ltecell.get("I_SF");
			String v_factory=(String)ltecell.get("V_FACTORY");
			for(int j=0;j<gsmadjs.size();j++)
			{

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
				Map gsmcell=gsmadjs.get(j);
				String v_bsc=(String)gsmcell.get("V_BSC");
				if (v_bsc == null || v_bsc.equals("")) {
					ps.setNull(10, java.sql.Types.VARCHAR);
				} else {
					ps.setString(10, v_bsc);
				}
				String i_cell_id1=(String)gsmcell.get("I_CELL_ID");
				ps.setInt(11, Integer.parseInt(i_cell_id1));
				String v_cgi=(String)gsmcell.get("V_CGI");
				ps.setString(12, v_cgi);
				String i_bcch=(String)gsmcell.get("I_BCCH");
				ps.setInt(13, Integer.parseInt(i_bcch));
				String i_lac=(String)gsmcell.get("I_LAC");
				ps.setInt(14, Integer.parseInt(i_lac));
				String v_bts_chn_name=(String)gsmcell.get("V_BTS_CHN_NAME");
				if (v_bts_chn_name == null || v_bts_chn_name.equals("")) {
					ps.setNull(15, java.sql.Types.VARCHAR);
				} else {
					ps.setString(15, v_bts_chn_name);
				}
				String v_bts_eng_name=(String)gsmcell.get("V_BTS_ENG_NAME");
				if (v_bts_eng_name == null || v_bts_eng_name.equals("")) {
					ps.setNull(16, java.sql.Types.VARCHAR);
				} else {
					ps.setString(16, v_bts_eng_name);
				}
				String v_bsic=(String)gsmcell.get("V_BSIC");
				if (v_bsic == null || v_bsic.equals("")) {
					ps.setNull(17, java.sql.Types.VARCHAR);
				} else {
					ps.setString(17, v_bsic);
				}
				String i_longitude1=(String)gsmcell.get("I_LONGITUDE");
				ps.setDouble(18, Double.parseDouble(i_longitude1));
				String i_latitude1=(String)gsmcell.get("I_LATITUDE");
				ps.setDouble(19, Double.parseDouble(i_latitude1));
				String is_indoor_distribution=(String)gsmcell.get("IS_INDOOR_DISTRIBUTION");
				ps.setInt(20, Integer.parseInt(is_indoor_distribution));
				String i_antenna_direction=(String)gsmcell.get("I_ANTENNA_DIRECTION");
				ps.setInt(21, Integer.parseInt(i_antenna_direction));
				String i_distance=(String)gsmcell.get("I_DISTANCE");
				ps.setInt(22, Integer.parseInt(i_distance));
				int i_lost_bcch=0;
				ps.setInt(23, i_lost_bcch);
				String i_subcomp_id1=(String)gsmcell.get("I_SUBCOMP_ID");
				ps.setInt(24, Integer.parseInt(i_subcomp_id1));
				String i_country_id1=(String)gsmcell.get("I_COUNTRY_ID");
				ps.setInt(25, Integer.parseInt(i_country_id1));
				String v_check="";
				ps.setNull(26, java.sql.Types.NULL);
				int i_lost_adj=0;
				ps.setInt(27, i_lost_adj);
				ps.addBatch();
			}
			if (i != 0 && i % 50 == 0) {
				num += ps.executeBatch().length;
				con.commit();
				log.debug("�����" + i+"LTEС��GSM������ϸ���");
			}
			num += ps.executeBatch().length;
			con.commit();
		}
		
		ps.close();
		con.close();
		return num;
	}
}
