package mr.mrs;

import java.io.File;

import cfg.Config;

public class MrsMain {

	/**
	 * ����1:����·�� ����2:���·�� ����3:���� ����4:�Ƿ��Զ���ѹ true or false
	 */
	public static void main(String args[]) {
		Config.init(4);

		String inpath = args[0];
		String outpath = args[1];
		String factory = args[2];
		String autoUnGzip = args[3];

		// String path = "E:\\LTE����\\LTEMR\\NS\\mr8";
		// String
		// path="E:\\LTE����\\LTEMR\\HW\\MR���ɽ��\\MR���ɽ��\\234013_֣��_��վ_����_����������";
		// File f = new File(
		// "E:\\LTE����\\LTEMR\\NS\\MR_all\\MRS\\TD-LTE_MRS_NSN_OMC_422000_20131203090000.xml");

		MrsParse mrsp = new MrsParse();
		mrsp.parseAndOut(inpath, outpath, factory, Boolean
				.parseBoolean(autoUnGzip));
	}
}
