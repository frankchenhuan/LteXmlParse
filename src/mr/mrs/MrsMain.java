package mr.mrs;

import java.io.File;

import cfg.Config;

public class MrsMain {

	/**
	 * 参数1:输入路径 参数2:输出路径 参数3:厂家 参数4:是否自动解压 true or false
	 */
	public static void main(String args[]) {
		Config.init(4);

		String inpath = args[0];
		String outpath = args[1];
		String factory = args[2];
		String autoUnGzip = args[3];

		// String path = "E:\\LTE数据\\LTEMR\\NS\\mr8";
		// String
		// path="E:\\LTE数据\\LTEMR\\HW\\MR集采结果\\MR集采结果\\234013_郑州_宏站_市区_商务西三街";
		// File f = new File(
		// "E:\\LTE数据\\LTEMR\\NS\\MR_all\\MRS\\TD-LTE_MRS_NSN_OMC_422000_20131203090000.xml");

		MrsParse mrsp = new MrsParse();
		mrsp.parseAndOut(inpath, outpath, factory, Boolean
				.parseBoolean(autoUnGzip));
	}
}
