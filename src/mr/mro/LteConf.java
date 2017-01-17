package mr.mro;
/**
 * LTE小区配置信息实体类
 * @author Administrator
 */
public class LteConf {
	private Integer i_subcomp_id;
	private Integer i_enodeb_id;
	private Integer i_cell_id;
	private Double  i_longitude;
	private Double i_latitude;
	private Integer earfcn;
	private Integer pci;
	
	private Integer i_enodeb_id1;
	private Integer i_cell_id1;
	private Double i_longitude1;
	private Double i_latitude1;
	private String v_cell_cname1;
	private String v_cell_ename1;
	private Integer i_sf1;
	private Double i_antenna_height1;
	private Integer i_antenna_direction1;
	private Integer total_downtilt1;
	private Double n_distance;
	private Double relative;
	
	private Integer n_all_point;
	private Integer n_neighbor_point;
	private Double i_poly_inter;
	
	public Double getRelative() {
		return relative;
	}
	public void setRelative(Double relative) {
		this.relative = relative;
	}
	public String getV_cell_cname1() {
		return v_cell_cname1;
	}
	public void setV_cell_cname1(String v_cell_cname1) {
		this.v_cell_cname1 = v_cell_cname1;
	}
	public String getV_cell_ename1() {
		return v_cell_ename1;
	}
	public void setV_cell_ename1(String v_cell_ename1) {
		this.v_cell_ename1 = v_cell_ename1;
	}
	public Integer getI_sf1() {
		return i_sf1;
	}
	public void setI_sf1(Integer i_sf1) {
		this.i_sf1 = i_sf1;
	}
	public Double getI_antenna_height1() {
		return i_antenna_height1;
	}
	public void setI_antenna_height1(Double i_antenna_height1) {
		this.i_antenna_height1 = i_antenna_height1;
	}
	public Integer getI_antenna_direction1() {
		return i_antenna_direction1;
	}
	public void setI_antenna_direction1(Integer i_antenna_direction1) {
		this.i_antenna_direction1 = i_antenna_direction1;
	}
	public Integer getTotal_downtilt1() {
		return total_downtilt1;
	}
	public void setTotal_downtilt1(Integer total_downtilt1) {
		this.total_downtilt1 = total_downtilt1;
	}
	public Double getN_distance() {
		return n_distance;
	}
	public void setN_distance(Double n_distance) {
		this.n_distance = n_distance;
	}
	public Integer getI_subcomp_id() {
		return i_subcomp_id;
	}
	public void setI_subcomp_id(Integer i_subcomp_id) {
		this.i_subcomp_id = i_subcomp_id;
	}
	public Integer getI_enodeb_id() {
		return i_enodeb_id;
	}
	public void setI_enodeb_id(Integer i_enodeb_id) {
		this.i_enodeb_id = i_enodeb_id;
	}
	public Integer getI_cell_id() {
		return i_cell_id;
	}
	public void setI_cell_id(Integer i_cell_id) {
		this.i_cell_id = i_cell_id;
	}
	public Double getI_longitude() {
		return i_longitude;
	}
	public void setI_longitude(Double i_longitude) {
		this.i_longitude = i_longitude;
	}
	public Double getI_latitude() {
		return i_latitude;
	}
	public void setI_latitude(Double i_latitude) {
		this.i_latitude = i_latitude;
	}
	public Integer getI_enodeb_id1() {
		return i_enodeb_id1;
	}
	public void setI_enodeb_id1(Integer i_enodeb_id1) {
		this.i_enodeb_id1 = i_enodeb_id1;
	}
	public Integer getI_cell_id1() {
		return i_cell_id1;
	}
	public void setI_cell_id1(Integer i_cell_id1) {
		this.i_cell_id1 = i_cell_id1;
	}
	public Double getI_longitude1() {
		return i_longitude1;
	}
	public void setI_longitude1(Double i_longitude1) {
		this.i_longitude1 = i_longitude1;
	}
	public Double getI_latitude1() {
		return i_latitude1;
	}
	public void setI_latitude1(Double i_latitude1) {
		this.i_latitude1 = i_latitude1;
	}
	public Integer getEarfcn() {
		return earfcn;
	}
	public void setEarfcn(Integer earfcn) {
		this.earfcn = earfcn;
	}
	public Integer getPci() {
		return pci;
	}
	public void setPci(Integer pci) {
		this.pci = pci;
	}
	public Integer getN_all_point() {
		return n_all_point;
	}
	public void setN_all_point(Integer n_all_point) {
		this.n_all_point = n_all_point;
	}
	public Integer getN_neighbor_point() {
		return n_neighbor_point;
	}
	public void setN_neighbor_point(Integer n_neighbor_point) {
		this.n_neighbor_point = n_neighbor_point;
	}
	public Double getI_poly_inter() {
		return i_poly_inter;
	}
	public void setI_poly_inter(Double i_poly_inter) {
		this.i_poly_inter = i_poly_inter;
	}
}
