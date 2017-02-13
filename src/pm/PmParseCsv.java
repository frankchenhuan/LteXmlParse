package pm;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import com.csvreader.CsvReader;

import cfg.ConfObj;
import cfg.Config;
import cfg.KeyConstant;

public class PmParseCsv extends PmParse {
	protected String objectType;//对象类型
	protected String version; //版本
	protected String timeStamp;//时间戳
	protected String timeZone;//时区
	protected String period;//周期
	protected String vendorName;// 厂家
	protected String elementType;
	
	protected String begintime;//数据的开始时间
	protected String endtime;//数据的结束时间
	
	protected SimpleDateFormat df1 = new SimpleDateFormat("yyyyMMddHHmmss");
	protected SimpleDateFormat df2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	public PmParseCsv() {
		this.dataext="csv";
	}

	/** 根据文件名、文件头初始化文件的基本信息 
	 * @throws ParseException */
	private void initFileInfo(String filename, String filetitle){
		
		log.debug(filetitle);
		String el2[]=filetitle.split("\\|");
		this.timeStamp = el2[0].split("=")[1];
		this.timeZone = el2[1].split("=")[1];
		
		this.vendorName = el2[3].split("=")[1];
		this.elementType = el2[4].split("=")[1];
		
		String el1[]=filename.split("-");
		
		this.objectType = el1[2];
		this.version = el1[4];
		String datetime=el1[5];
		if(filename.indexOf("-Ri")>0)
		{
			this.period = el1[7].substring(0,2);
		}else
		{
			this.period = el1[6].substring(0,2);
		}
		
		/**根据文件名中的 时间，计算开始时间和结束时间*/
		try {
			Date d1=df1.parse(datetime);
			this.begintime=df2.format(d1);
			long t=d1.getTime()+Integer.parseInt(this.period)*60*1000;
			d1=new  Date(t);
			this.endtime=df2.format(d1);
			log.debug(begintime+"   "+endtime);
		} catch (ParseException e) {
			log.error("解析文件时，日期处理错误："+datetime,e);
		}
	}

	@Override
	public void parseXML(File f) throws IOException, SQLException {
		String filename = f.getName();
		CsvReader cr = new CsvReader(f.getAbsolutePath(), '|', Charset.forName(Config.getCharSet()));
		cr.readRecord();
		/** 初始化基本信息 */

		this.initFileInfo(filename, cr.getRawRecord());

		cr.readHeaders();
		ConfObj confobj = null;// 当前正在解析的对象类型
		confobj = objcfgs.get(this.objectType);
		if (confobj == null) {
			cr.close();
			return;
		}

		String headers[] = cr.getHeaders();
		PmDataObj pmdataobj = null;
		while (cr.readRecord()) {
			String rmUID = cr.get("rmUID");
			String startTime = cr.get("startTime");
			String dn = cr.get("Dn");
			String userLabel = cr.get("UserLabel");
			pmdataobj = pmData.get(rmUID + "_" + startTime);
			if (pmdataobj == null) {
				pmdataobj = new PmDataObj();
				pmData.put(rmUID + "_" + startTime, pmdataobj);
			}
			pmdataobj.setPmConfObj(confobj);
			pmdataobj.addValue(KeyConstant.KEY_OBJDN, dn);
			pmdataobj.addValue(KeyConstant.KEY_OBJUSERLABEL, userLabel);
			pmdataobj.addValue(KeyConstant.KEY_FILENAME, f.getName());
			pmdataobj.addValue(KeyConstant.KEY_BEGINTIME, this.begintime);
			pmdataobj.addValue(KeyConstant.KEY_ENDTIME, this.endtime);
			String dns[] = dn.split(",");
			for (int y = 0; y < dns.length; y++) {
				String ds[] = dns[y].split("=");
				if (ds.length > 1) {
					pmdataobj.addValue("DN-" + ds[0].toUpperCase(), ds[1]);
				}
			}

			for (int i = 0; i < headers.length; i++) {
				String header = headers[i];
				String v=cr.get(header);
				pmdataobj.addValue(header.toUpperCase(), v);
			}
			if (pmData.size() >= Config.getCommitCount()) {
				log.debug(pmData.size());
				insertObjData();
				pmData.clear();
			}
		}
		cr.close();

	}
}
