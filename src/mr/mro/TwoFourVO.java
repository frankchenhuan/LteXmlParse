package mr.mro;

import java.util.HashMap;
import java.util.Map;
/**
 * 二维四象限实体类
 * @author Administrator
 */
public class TwoFourVO {
	private Integer rsrp_rsrq1 = 0;
	private Integer rsrp_rsrq2 = 0;
	private Integer rsrp_rsrq3 = 0;
	private Integer rsrp_rsrq4 = 0;
	private Integer rsrp_rttd1 = 0;
	private Integer rsrp_rttd2 = 0;
	private Integer rsrp_rttd3 = 0;
	private Integer rsrp_rttd4 = 0;
	private Integer rsrp_sinru1 = 0;
	private Integer rsrp_sinru2 = 0;
	private Integer rsrp_sinru3 = 0;
	private Integer rsrp_sinru4 = 0;
	private Integer rttd_rsrq1 = 0;
	private Integer rttd_rsrq2 = 0;
	private Integer rttd_rsrq3 = 0;
	private Integer rttd_rsrq4 = 0;
	private Integer sinru_rsrq1 = 0;
	private Integer sinru_rsrq2 = 0;
	private Integer sinru_rsrq3 = 0;
	private Integer sinru_rsrq4 = 0;
	private Integer rttd_sinru1 = 0;
	private Integer rttd_sinru2 = 0;
	private Integer rttd_sinru3 = 0;
	private Integer rttd_sinru4 = 0;
	
	
	public static void main(String[] args){
		Map<String,TwoFourVO> map = new HashMap<String,TwoFourVO>();
		TwoFourVO tf = new TwoFourVO();
		map.put("a", tf);
		tf.setRsrp_rsrq1(1);
		
		TwoFourVO tf1 = map.get("a");
		System.out.println(tf1.getRsrp_rsrq1());
	}
	public Integer getRttd_rsrq1() {
		return rttd_rsrq1;
	}

	public void setRttd_rsrq1(Integer rttd_rsrq1) {
		this.rttd_rsrq1 = rttd_rsrq1;
	}

	public Integer getRttd_rsrq2() {
		return rttd_rsrq2;
	}

	public void setRttd_rsrq2(Integer rttd_rsrq2) {
		this.rttd_rsrq2 = rttd_rsrq2;
	}

	public Integer getRttd_rsrq3() {
		return rttd_rsrq3;
	}

	public void setRttd_rsrq3(Integer rttd_rsrq3) {
		this.rttd_rsrq3 = rttd_rsrq3;
	}

	public Integer getRttd_rsrq4() {
		return rttd_rsrq4;
	}

	public void setRttd_rsrq4(Integer rttd_rsrq4) {
		this.rttd_rsrq4 = rttd_rsrq4;
	}

	public Integer getSinru_rsrq1() {
		return sinru_rsrq1;
	}

	public void setSinru_rsrq1(Integer sinru_rsrq1) {
		this.sinru_rsrq1 = sinru_rsrq1;
	}

	public Integer getSinru_rsrq2() {
		return sinru_rsrq2;
	}

	public void setSinru_rsrq2(Integer sinru_rsrq2) {
		this.sinru_rsrq2 = sinru_rsrq2;
	}

	public Integer getSinru_rsrq3() {
		return sinru_rsrq3;
	}

	public void setSinru_rsrq3(Integer sinru_rsrq3) {
		this.sinru_rsrq3 = sinru_rsrq3;
	}

	public Integer getSinru_rsrq4() {
		return sinru_rsrq4;
	}

	public void setSinru_rsrq4(Integer sinru_rsrq4) {
		this.sinru_rsrq4 = sinru_rsrq4;
	}

	public Integer getRttd_sinru1() {
		return rttd_sinru1;
	}

	public void setRttd_sinru1(Integer rttd_sinru1) {
		this.rttd_sinru1 = rttd_sinru1;
	}

	public Integer getRttd_sinru2() {
		return rttd_sinru2;
	}

	public void setRttd_sinru2(Integer rttd_sinru2) {
		this.rttd_sinru2 = rttd_sinru2;
	}

	public Integer getRttd_sinru3() {
		return rttd_sinru3;
	}

	public void setRttd_sinru3(Integer rttd_sinru3) {
		this.rttd_sinru3 = rttd_sinru3;
	}

	public Integer getRttd_sinru4() {
		return rttd_sinru4;
	}

	public void setRttd_sinru4(Integer rttd_sinru4) {
		this.rttd_sinru4 = rttd_sinru4;
	}
	
	public Integer getRsrp_rsrq1() {
		return rsrp_rsrq1;
	}
	public void setRsrp_rsrq1(Integer rsrp_rsrq1) {
		this.rsrp_rsrq1 = rsrp_rsrq1;
	}
	public Integer getRsrp_rsrq2() {
		return rsrp_rsrq2;
	}
	public void setRsrp_rsrq2(Integer rsrp_rsrq2) {
		this.rsrp_rsrq2 = rsrp_rsrq2;
	}
	public Integer getRsrp_rsrq3() {
		return rsrp_rsrq3;
	}
	public void setRsrp_rsrq3(Integer rsrp_rsrq3) {
		this.rsrp_rsrq3 = rsrp_rsrq3;
	}
	public Integer getRsrp_rsrq4() {
		return rsrp_rsrq4;
	}
	public void setRsrp_rsrq4(Integer rsrp_rsrq4) {
		this.rsrp_rsrq4 = rsrp_rsrq4;
	}
	public Integer getRsrp_rttd1() {
		return rsrp_rttd1;
	}
	public void setRsrp_rttd1(Integer rsrp_rttd1) {
		this.rsrp_rttd1 = rsrp_rttd1;
	}
	public Integer getRsrp_rttd2() {
		return rsrp_rttd2;
	}
	public void setRsrp_rttd2(Integer rsrp_rttd2) {
		this.rsrp_rttd2 = rsrp_rttd2;
	}
	public Integer getRsrp_rttd3() {
		return rsrp_rttd3;
	}
	public void setRsrp_rttd3(Integer rsrp_rttd3) {
		this.rsrp_rttd3 = rsrp_rttd3;
	}
	public Integer getRsrp_rttd4() {
		return rsrp_rttd4;
	}
	public void setRsrp_rttd4(Integer rsrp_rttd4) {
		this.rsrp_rttd4 = rsrp_rttd4;
	}
	public Integer getRsrp_sinru1() {
		return rsrp_sinru1;
	}
	public void setRsrp_sinru1(Integer rsrp_sinru1) {
		this.rsrp_sinru1 = rsrp_sinru1;
	}
	public Integer getRsrp_sinru2() {
		return rsrp_sinru2;
	}
	public void setRsrp_sinru2(Integer rsrp_sinru2) {
		this.rsrp_sinru2 = rsrp_sinru2;
	}
	public Integer getRsrp_sinru3() {
		return rsrp_sinru3;
	}
	public void setRsrp_sinru3(Integer rsrp_sinru3) {
		this.rsrp_sinru3 = rsrp_sinru3;
	}
	public Integer getRsrp_sinru4() {
		return rsrp_sinru4;
	}
	public void setRsrp_sinru4(Integer rsrp_sinru4) {
		this.rsrp_sinru4 = rsrp_sinru4;
	}
	
}
