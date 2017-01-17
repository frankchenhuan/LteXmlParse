package dis;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cfg.CsvOutPrint;
import cfg.CsvWriter;
import cfg.DataSource;

public class DisMain {

	Map subcompType = new HashMap<String, String>();

	List<GsmCell> allgsmcell = new ArrayList<GsmCell>();// 所有gsm小区
	List<LteCell> lteCells = new ArrayList<LteCell>();
	Connection con = null;
	List<GsmCell> gsmcell_gll = new ArrayList<GsmCell>();// 高流量gsm小区

	String gsmsql = "select t.i_cell_id,t.i_subcomp_id,c.v_subcomp_name"
			+ ",t.i_longitude,t.i_latitude"
			+ ",round((SUM_N_RLC_UL24+SUM_N_RLC_DL24+SUM_N_RLC_UL_MCS24+SUM_N_RLC_DL_MCS24)/1024/1024,2) liuliang "
			+ ",is_indoor_distribution indoor from gsm_pm_cell_d t "
			+ "inner join t_company c on t.i_subcomp_id=c.i_subcomp_id  "
			+ "WHERE t.dtm_repdate=trunc(sysdate-1) and t.i_longitude is not null and t.i_latitude is not null"
			+ " order by t.i_subcomp_id";

	public DisMain() {
		subcompType.put("郑州", 1);
		subcompType.put("洛阳", 2);
		subcompType.put("新乡", 2);
		subcompType.put("南阳", 2);
		subcompType.put("安阳", 2);
		subcompType.put("开封", 3);
		subcompType.put("商丘", 3);
		subcompType.put("周口", 3);
		subcompType.put("信阳", 3);
		subcompType.put("平顶山", 3);
		subcompType.put("驻马店", 3);
		subcompType.put("三门峡", 3);
		subcompType.put("焦作", 3);
		subcompType.put("漯河", 3);
		subcompType.put("许昌", 3);
		subcompType.put("濮阳", 3);
		subcompType.put("鹤壁", 4);
		subcompType.put("济源", 4);
	}

	/**
	 * 读取GSM小区
	 * 
	 * @throws SQLException
	 */
	private void readGsmCell() throws SQLException {
		ResultSet rs = null;
		PreparedStatement ps = null;
		try {
			con = DataSource.getConnection();
			ps = con.prepareStatement(this.gsmsql);
			rs = ps.executeQuery();
			allgsmcell.clear();
			
			while (rs.next()) {
				GsmCell gc = new GsmCell();
				gc.setCellid(rs.getInt("i_cell_id"));
				gc.setIndoor(rs.getBoolean("indoor"));
//				System.out.println(rs.getBoolean("indoor")
//						+ rs.getString("indoor"));
				gc.setLatitude(rs.getFloat("i_latitude"));
				gc.setLongitude(rs.getFloat("i_longitude"));
				gc.setSubcompid(rs.getInt("i_subcomp_id"));
				gc.setSubcompname(rs.getString("v_subcomp_name"));
				gc.setLiuliang(rs.getDouble("liuliang"));
				allgsmcell.add(gc);
			}
			
		} finally {
			try {
				rs.close();
				ps.close();
				con.close();
			} catch (Exception e) {
			}
		}
	}

	/**
	 * 读取LTE小区
	 * 
	 * @throws IOException
	 */
	private void readLteCell(String file) throws IOException {
		List<List<String>> data = cfg.Tools.readCSVFile(file);
		for (int x = 1; x < data.size(); x++) {
			List<String> line = data.get(x);
			int num = line.size();
			LteCell lc = new LteCell();
			lc.setSubcompname(num>0?line.get(0):"");
			//lc.setEnodeb(Integer.parseInt(line.get(1).trim()));
			String longitude = num > 2 ? line.get(2).trim() : "";
			String latitude = num > 3 ? line.get(3).trim() : "";
			if (longitude.equals("") || latitude.equals("")) {
				continue;
			}
			lc.setLongitude(Float.parseFloat(longitude));
			lc.setLatitude(Float.parseFloat(latitude));
			this.lteCells.add(lc);
			
		}
		//System.out.println(this.lteCells.size());
	}

	/**
	 * 根据条件判断周边是否有lte小区
	 */
	private List<GsmCell> checkNoLte() {
		List<GsmCell> gcs = new ArrayList<GsmCell>();
		for (int i = 0; i < allgsmcell.size(); i++) {
			if(i%1000==0)
			{
				System.out.println("核查进度:"+i);
			}
			GsmCell gc = allgsmcell.get(i);
			if (gc.isIndoor())// 室分
			{
				int cityType = Integer.parseInt(subcompType.get(gc
						.getSubcompname())
						+ "");
				//System.out.println(gc.getLiuliang());
				switch (cityType) {// 判断城市分类
				case 1:
				case 2:
					if (gc.getLiuliang() > 600) {// 判断流量
						boolean isExistsLte = false;
						for (int j = 0; j < lteCells.size(); j++) {
							LteCell lc = lteCells.get(j);
							double dis = cfg.Tools.getDistatce(
									gc.getLatitude(), lc.getLatitude(), gc
											.getLongitude(), lc.getLongitude());
							if (dis <= 50) {// 判断距离是否小于要求范围
								isExistsLte = true;
								break;
							}
						}
						// 设置是否有LTE覆盖
						gc.setExistsLte(isExistsLte);
						gcs.add(gc);
					}
					break;
				case 3:
				case 4:
					if (gc.getLiuliang() > 900) {// 判断流量

						boolean isExistsLte = false;
						for (int j = 0; j < lteCells.size(); j++) {
							LteCell lc = lteCells.get(j);
							double dis = cfg.Tools.getDistatce(
									gc.getLatitude(), lc.getLatitude(), gc
											.getLongitude(), lc.getLongitude());
							if (dis <= 50) {// 判断距离是否小于要求范围
								isExistsLte = true;
								break;
							}
						}
						// 设置是否有LTE覆盖
						gc.setExistsLte(isExistsLte);
						gcs.add(gc);
					}
					break;
				default:
					break;
				}
			} else {// 室外
				int cityType = Integer.parseInt(subcompType.get(gc
						.getSubcompname())
						+ "");
				switch (cityType) {// 判断城市分类
				case 1:
				case 2:
					if (gc.getLiuliang() > 600) {// 判断流量

						boolean isExistsLte = false;
						for (int j = 0; j < lteCells.size(); j++) {
							LteCell lc = lteCells.get(j);
							double dis = cfg.Tools.getDistatce(
									gc.getLatitude(), lc.getLatitude(), gc
											.getLongitude(), lc.getLongitude());
							if (dis <= 300) {// 判断距离是否小于要求范围
								isExistsLte = true;
								break;
							}
						}
						// 设置是否有LTE覆盖
						gc.setExistsLte(isExistsLte);
						gcs.add(gc);
						//System.out.println(gc.getCellid());
					}
					break;
				case 3:
				case 4:
					if (gc.getLiuliang() > 600) {// 判断流量

						boolean isExistsLte = false;
						for (int j = 0; j < lteCells.size(); j++) {
							LteCell lc = lteCells.get(j);
							double dis = cfg.Tools.getDistatce(
									gc.getLatitude(), lc.getLatitude(), gc
											.getLongitude(), lc.getLongitude());
							if (dis <= 300) {// 判断距离是否小于要求范围
								isExistsLte = true;
								break;
							}
						}
						// 设置是否有LTE覆盖
						gc.setExistsLte(isExistsLte);
						gcs.add(gc);
					}
					break;
				default:
					break;
				}
			}
		}
		return gcs;
	}

	/**
	 * 输出结果到CSV文件
	 * 
	 * @throws IOException
	 */
	private void outputToCSV(String path) throws IOException {
		List<GsmCell> gcs = this.checkNoLte();
		CsvOutPrint cop = new CsvWriter(path + "/" + "GSM高流量小区"
				+ System.currentTimeMillis() + ".csv");
		List line = new ArrayList<String>();
		line.add("地市");
		line.add("小区号");
		line.add("是否室分");
		line.add("经度");
		line.add("维度");
		line.add("流量");
		line.add("周边是否有LTE基站");
		cop.write(line, true);
		
		for (int i = 0; i < gcs.size(); i++) {
			line.clear();
			GsmCell gc = gcs.get(i);
			line.add(gc.getSubcompname()+"");
			line.add(gc.getCellid()+"");
			line.add(gc.isIndoor()+"");
			line.add(gc.getLongitude()+"");
			line.add(gc.getLatitude()+"");
			line.add(gc.getLiuliang()+"");
			line.add(gc.isExistsLte()+"");
			cop.write(line, true);
		}
		cop.flush();
		cop.close();
	}

	public void checkAndOut(String outpath, String ltedatapath)
			throws SQLException, IOException {
		this.readGsmCell();
		System.out.println("GSM小区总数："+this.allgsmcell.size());
		this.readLteCell(ltedatapath);
		System.out.println("LTE基站总数："+this.lteCells.size());
		this.outputToCSV(outpath);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		DisMain dm = new DisMain();
		String outpath = "E:\\ch";
		String ltedatapath = "E:\\ch\\LTE规划数据.csv";
		try {
			DataSource.init();
			dm.checkAndOut(outpath, ltedatapath);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
