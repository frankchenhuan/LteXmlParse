package dis;

public class LteCell {
	int cellid;//С����
	int enodeb;//��վ��
	int subcompid;//�ֹ�˾ID
	String subcompname;//�ֹ�˾
	float longitude;//����
	float latitude;//ά��
	int gsmNum=0;//�ܱ�GSMС������
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
