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
	protected String objectType;//��������
	protected String version; //�汾
	protected String timeStamp;//ʱ���
	protected String timeZone;//ʱ��
	protected String period;//����
	protected String vendorName;// ����
	protected String elementType;
	
	protected String begintime;//���ݵĿ�ʼʱ��
	protected String endtime;//���ݵĽ���ʱ��
	
	protected SimpleDateFormat df1 = new SimpleDateFormat("yyyyMMddHHmmss");
	protected SimpleDateFormat df2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	public PmParseCsv() {
		this.dataext="csv";
	}

	/** �����ļ������ļ�ͷ��ʼ���ļ��Ļ�����Ϣ 
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
		
		/**�����ļ����е� ʱ�䣬���㿪ʼʱ��ͽ���ʱ��*/
		try {
			Date d1=df1.parse(datetime);
			this.begintime=df2.format(d1);
			long t=d1.getTime()+Integer.parseInt(this.period)*60*1000;
			d1=new  Date(t);
			this.endtime=df2.format(d1);
			log.debug(begintime+"   "+endtime);
		} catch (ParseException e) {
			log.error("�����ļ�ʱ�����ڴ������"+datetime,e);
		}
	}

	@Override
	public void parseXML(File f) throws IOException, SQLException {
		String filename = f.getName();
		CsvReader cr = new CsvReader(f.getAbsolutePath(), '|', Charset.forName(Config.getCharSet()));
		cr.readRecord();
		/** ��ʼ��������Ϣ */

		this.initFileInfo(filename, cr.getRawRecord());

		cr.readHeaders();
		ConfObj confobj = null;// ��ǰ���ڽ����Ķ�������
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
