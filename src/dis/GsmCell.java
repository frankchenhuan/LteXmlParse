package dis;

public class GsmCell {
	int cellid;// С����
	int subcompid;// �ֹ�˾ID
	String subcompname;// �ֹ�˾
	float longitude;// ����
	float latitude;// ά��
	boolean indoor;// �Ƿ��ҷ�
	double liuliang;// ����
	boolean isExistsLte;//�ܱ��Ƿ����LTE��վ

	public int getCellid() {
		return cellid;
	}

	public void setCellid(int cellid) {
		this.cellid = cellid;
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

	public boolean isIndoor() {
		return indoor;
	}

	public void setIndoor(boolean indoor) {
		this.indoor = indoor;
	}

	public double getLiuliang() {
		return liuliang;
	}

	public void setLiuliang(double liuliang) {
		this.liuliang = liuliang;
	}

	public boolean isExistsLte() {
		return isExistsLte;
	}

	public void setExistsLte(boolean isExistsLte) {
		this.isExistsLte = isExistsLte;
	}

}
