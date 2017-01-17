package dis;

public class LteCell {
	int cellid;//小区号
	int enodeb;//基站号
	int subcompid;//分公司ID
	String subcompname;//分公司
	float longitude;//经度
	float latitude;//维度
	int gsmNum=0;//周边GSM小区数量
	public int getGsmNum() {
		return gsmNum;
	}
	
	public void addGsmNum() {
		this.gsmNum++;
	}
	public int getCellid() {
		return cellid;
	}
	public void setCellid(int cellid) {
		this.cellid = cellid;
	}
	public int getEnodeb() {
		return enodeb;
	}
	public void setEnodeb(int enodeb) {
		this.enodeb = enodeb;
	}
	public int getSubcompid() {
		return subcompid;
	}
	public void setSubcompid(int subcompid) {
		this.subcompid = subcompid;
	}
	public String getSubcompname() {
		return subcompname;
	}
	public void setSubcompname(String subcompname) {
		this.subcompname = subcompname;
	}
	public float getLongitude() {
		return longitude;
	}
	public void setLongitude(float longitude) {
		this.longitude = longitude;
	}
	public float getLatitude() {
		return latitude;
	}
	public void setLatitude(float latitude) {
		this.latitude = latitude;
	}
	
}
