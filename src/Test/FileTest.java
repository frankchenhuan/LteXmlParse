package Test;

public class FileTest {
	
	public static void main(String[] args) {
		String dn="ALU-CMHA-ZZ,SubNetwork=1,ManagedElement=B_RNORUNANBENDIDIANDC_268739,EnbFunction=460-00-268739,EutranCellTdd=128";
		int index = dn.indexOf("EutranCellTdd");
		String celltdd = "";
		String enb = "";
		String me = "";
		String subn = "";
		String dc = "";
		String dns[] = dn.split(",");

		/** 对各个对象的截取 */
		for (int i = 0; i < dns.length; i++) {
			String s = dns[i];
			String ds[] = s.split("=");
			if (ds.length <= 1) {
				continue;
			}
			
			if (ds[0].equalsIgnoreCase("DC")) {
				dc = ds[1];
			} else if (ds[0].equalsIgnoreCase("SubNetwork")) {
				subn = ds[1];
			} else if (ds[0].equalsIgnoreCase("ManagedElement")) {
				me = (!me.equals("") ? me + ";" + ds[1] : ds[1]);
			} else if (ds[0].equalsIgnoreCase("EnbFunction")) {
				enb = ds[1];
			} else if (ds[0].equalsIgnoreCase("EutranCellTdd")) {
				celltdd = ds[1];
			}
		}
	}
}
