package mr.mro;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import mr.mro.datasource.DataSource;

/**
 * lte-MRO处理主程序，负责解析合并同小时enodeb文件，并计算各项指标
 * @author Administrator
 *
 */
public class MroParse {
	
	protected final Log log = LogFactory.getLog(MroParse.class);
	
	/**
	 * 厂家ID:NSN 01;HUAWEI 02;ZTE 03;ALCATEL 04;
	 */
	public String FACTORY;
	/**
	 * RSRP转化电平值系数,140
	 */
	public static int STATIC_DBM = 140;
	
	private String omcid;
	private String enodeb;
	private String dtm_repdate;
	private List<File> files;
	private List<Boolean> isFileCorrect = new ArrayList<Boolean>();
	/**
	 * 全局变量，统计总采样点数
	 */
	//private int counts;
	/**
	 * 覆盖评估数据结果
	 * key = ci
	 * value = Map<key=dbm,value=count>
	 */
	private Map<String,Map<String,Integer>> coverMap = new HashMap<String,Map<String,Integer>>();
	/**
	 * 差值评估数据结果
	 * key = ci
	 * value = Map<key=diffDbm,value=count>
	 */
	private Map<String,Map<Integer,Integer>> diffMap = new HashMap<String,Map<Integer,Integer>>();
	/**
	 * 过覆盖评估数据结果
	 * key = ci
	 * value = Map<key=NC,value=count>
	 */
	private Map<String,Map<String,Integer>> overMap = new HashMap<String,Map<String,Integer>>();
	
	/**
	 * 重叠覆盖小区对数据集合
	 * key = ci
	 * value = Map<key=NC,value=count>
	 */
	private Map<String,Map<String,Integer>> olCoupleMap = new HashMap<String,Map<String,Integer>>();
	
	/**
	 * 重叠覆盖强度评估数据结果
	 * key = ci
	 * value = Map<key=diffDbm,value=count>
	 */
	private Map<String,Map<Double,Integer>> powerMap = new HashMap<String,Map<Double,Integer>>();
	
	/**
	 * 记录小区的总采样点数
	 * key = ci
	 * value = counts
	 */
	private Map<String,Integer> countMap = new HashMap<String,Integer>();
	
	/**
	 * 记录主小区>=-110的小区的总采样点数
	 * key = ci
	 * value = counts
	 */
	private Map<String,Integer> count110Map = new HashMap<String,Integer>();
	
	/**
	 * 记录主小区>=-100的小区的总采样点数
	 * key = ci
	 * value = counts
	 */
	private Map<String,Integer> count100Map = new HashMap<String,Integer>();
	
	/**
	 * 重叠覆盖评估数据
	 */
	private Map<String,OverLayingVO> overlayingMap = new HashMap<String,OverLayingVO>();
	
	
	/**
	 * 孤立小区数据集合
	 */
	private Map<String,AloneCell> aloneCellMap = new HashMap<String,AloneCell>();
	
	/**
	 * 存放临时文件，便于使用后删除
	 */
	private List<File> tempFileList = new ArrayList<File>();
	
	/**
	 * 二维四象限分析
	 * key = ci
	 * value = TwoFourVO
	 */
	private Map<String,TwoFourVO> twoFourMap = new HashMap<String,TwoFourVO>();
	
	/**
	 * 构造函数
	 * @param fileName
	 */
	public MroParse(List<File> fileName,String omcid){
		this.files = fileName;
		this.omcid = omcid;
		init();
	}
	
	/**
	 * 初始化enodeb和日期
	 */
	private void init(){
		if(files.size()>0){
			String name = files.get(0).getName();
			String[] temp = name.split("_");
			enodeb = temp[4];
			dtm_repdate = temp[5].substring(0,4)+"-"+temp[5].substring(4, 6)+"-"+temp[5].substring(6, 8)
					+" "+temp[5].substring(8, 10)+":00:00";
			if("NSN".equalsIgnoreCase(temp[2])){
				FACTORY="01";
			}else if("HUAWEI".equalsIgnoreCase(temp[2])){
				FACTORY="02";
			}else if("ZTE".equalsIgnoreCase(temp[2])){
				FACTORY="03";
			}else if("ALCATEL".equalsIgnoreCase(temp[2])){
				FACTORY="04";
			}
		}
	}
	
	/**
	 * 解析指定的文件名
	 * @param fileName
	 */
	public void parseMro() throws XMLStreamException, IOException{
		for(File file : files){
			XMLInputFactory factory = XMLInputFactory.newInstance();
			
		/** //处理特殊字符
			File tempFile = Tool.formatXML(file);
			tempFileList.add(tempFile);
			InputStreamReader isr = new InputStreamReader(new FileInputStream(tempFile), "UTF-8");
		**/
			InputStreamReader isr = new InputStreamReader(new FileInputStream(file), "UTF-8");
			
			XMLEventReader reader = factory.createXMLEventReader(isr);
			String ci = "";
			/**
			 * 字段数组的长度
			 */
			int arrayLength = 0;
			/**
			 * 是否是有效的MRO数据块
			 */
			boolean isMRO = false;
			/**
			 * 是否是有效的华为/中兴 MRO数据块
			 */
			boolean isObject = false;
			
			/*********覆盖评估及差值评估处理*******/
			/**
			 * 最大RSRP
			 */
			int MAX_RSRP = -9999;
			/**
			 * 主小区RSRP
			 */
			int S_RSRP = -9999;
			/**
			 * 邻区最强RSRP
			 */
			int MAX_N_RSRP = -9999;
			/*********覆盖评估及差值评估处理end*******/
			
			/*********重叠覆盖评估处理start*******/
			int overlaying = 0;
			OverLayingVO ol;
			/**
			 * 主小区RSRP大于-110标志
			 */
			boolean S_RSRP_110_FLAG = false;
			/**
			 * 主小区RSRP大于-100标志
			 */
			boolean S_RSRP_100_FLAG = false;
			/**
			 * 邻区电平转化功率毫瓦数
			 */
			double N_M_POWER = 0;
			
			/*********重叠覆盖评估处理end*******/
			
			/*********孤立小区处理start*******/
			/**
			 * 孤立小区分子
			 */
			int aloneCellNume = 0;
			/**
			 * 孤立小区分母
			 */
			int aloneCellDeno = 0;
			AloneCell ac;
			/*********孤立小区处理end*******/
			
			/*********二维四象限分析start*******/
			/**
			 * 是否是测量报告内第一条有效数据
			 */
			boolean isFirstSci = true;
			TwoFourVO tf;
			/*********二维四象限分析end*******/
			
			try{
				while(reader.hasNext()){
					XMLEvent event = reader.nextEvent();
					/**
					 * 是否所要解析的数据库标识
					 */
					if(event.isStartElement()){
						StartElement startEle = event.asStartElement();
						String qName = startEle.getName().toString();
						if("smr".equalsIgnoreCase(qName)){
							String smr = reader.getElementText().trim();
							String[] arr_smr = smr.split("\\s+");
							arrayLength = arr_smr.length;
							if(("MR.LteScRSRP".equalsIgnoreCase(arr_smr[0]) || "MR.LteScEarfcn".equalsIgnoreCase(arr_smr[0])) &&
									("MR.LteNcRSRP".equalsIgnoreCase(arr_smr[1]) || "MR.LteScRSRQ".equalsIgnoreCase(arr_smr[1]) || "MR.LteScPci".equalsIgnoreCase(arr_smr[1]))){
								isMRO = true;
							}
						}else if("object".equalsIgnoreCase(qName)){
							if(isMRO==true){
								String objectid = startEle.getAttributeByName(new QName("id")).getValue();
								ci = Tool.getCiByObjectId(objectid, FACTORY,arrayLength);
								if(objectid!=null && !"".equals(ci)){
									if(!objectid.endsWith(":7")){
										isObject = true;
									}
								}
							}
						}else if("v".equalsIgnoreCase(qName)){
							if(isMRO==true && isObject==true){
								String v = reader.getElementText().trim();
								String[] arr_v = v.split("\\s+");
								String s_rsrp = "";
								String n_rsrp = "";
								String n_earfcn = "";
								String n_pci = "";
								String n_key="";
								
								/**
								 * 二维四象限数据分析
								 */
								String s_rsrq = "NIL";
								String s_rttd = "NIL";
								String s_sinr_u = "NIL";
								
								if(FACTORY.equals("01")){
									if(arr_v.length==NSN2.NSN_SMR_LENGTH){
										s_rsrp = arr_v[NSN2.S_RSRP_INDEX];
										n_rsrp = arr_v[NSN2.N_RSRP_INDEX];
										n_earfcn = arr_v[NSN2.N_EARFCN_INDEX];
										n_pci = arr_v[NSN2.N_PCI_INDEX];
										
										s_rsrq = arr_v[NSN2.S_RSRQ_INDEX];
										s_rttd = arr_v[NSN2.S_RTTD_INDEX];
										s_sinr_u = arr_v[NSN2.S_SINR_U_INDEX];
									}else{
										s_rsrp = arr_v[NSN.S_RSRP_INDEX];
										n_rsrp = arr_v[NSN.N_RSRP_INDEX];
										n_earfcn = arr_v[NSN.N_EARFCN_INDEX];
										n_pci = arr_v[NSN.N_PCI_INDEX];
										
										s_rsrq = arr_v[NSN.S_RSRQ_INDEX];
										s_rttd = arr_v[NSN.S_RTTD_INDEX];
										s_sinr_u = arr_v[NSN.S_SINR_U_INDEX];
									}
								}else if(FACTORY.equals("02")){
									s_rsrp = arr_v[HUAWEI.S_RSRP_INDEX];
									n_rsrp = arr_v[HUAWEI.N_RSRP_INDEX];
									n_earfcn = arr_v[HUAWEI.N_EARFCN_INDEX];
									n_pci = arr_v[HUAWEI.N_PCI_INDEX];
									
									s_rsrq = arr_v[HUAWEI.S_RSRQ_INDEX];
									s_rttd = arr_v[HUAWEI.S_RTTD_INDEX];
									s_sinr_u = arr_v[HUAWEI.S_SINR_U_INDEX];
								}else if(FACTORY.equals("03")){
									if(arr_v.length==ZTE2.NSN_SMR_LENGTH){
										s_rsrp = arr_v[ZTE2.S_RSRP_INDEX];
										n_rsrp = arr_v[ZTE2.N_RSRP_INDEX];
										n_earfcn = arr_v[ZTE2.N_EARFCN_INDEX];
										n_pci = arr_v[ZTE2.N_PCI_INDEX];
									}else{
										s_rsrp = arr_v[ZTE.S_RSRP_INDEX];
										n_rsrp = arr_v[ZTE.N_RSRP_INDEX];
										n_earfcn = arr_v[ZTE.N_EARFCN_INDEX];
										n_pci = arr_v[ZTE.N_PCI_INDEX];
									}
								}else if(FACTORY.equals("04")){
									s_rsrp = arr_v[BE.S_RSRP_INDEX];
									n_rsrp = arr_v[BE.N_RSRP_INDEX];
									n_earfcn = arr_v[BE.N_EARFCN_INDEX];
									n_pci = arr_v[BE.N_PCI_INDEX];
								}
								if("NIL".equals(n_earfcn) || "NIL".equals(n_pci)){
									n_key = "NIL";
								}else{
									n_key = n_earfcn+"-"+n_pci;
								}
								
								/*********二维四象限分析start*******/
								if(isFirstSci){//多个邻区的情况，只统计第一个邻区的数据即可
									if(!s_rsrp.equals("NIL") && !s_rsrq.equals("NIL") 
											&& !s_rttd.equals("NIL") && !s_sinr_u.equals("NIL")){
										if(twoFourMap.containsKey(ci)){
											tf = twoFourMap.get(ci);
										}else{
											tf = new TwoFourVO();
											twoFourMap.put(ci, tf);
										}
										int i_s_rsrp = Integer.parseInt(s_rsrp);
										int i_s_rsrq = Integer.parseInt(s_rsrq);
										int i_s_rttd = Integer.parseInt(s_rttd);
										int i_s_sinr_u = Integer.parseInt(s_sinr_u);
										//rsrp-rsrq
										if(i_s_rsrp >= LteMroThresholdSet.TWO_FOUR_RSRP 
												&& i_s_rsrq >= LteMroThresholdSet.TWO_FOUR_RSRQ){
											tf.setRsrp_rsrq1(tf.getRsrp_rsrq1()+1);
										}
										if(i_s_rsrp >= LteMroThresholdSet.TWO_FOUR_RSRP 
												&& i_s_rsrq < LteMroThresholdSet.TWO_FOUR_RSRQ){
											tf.setRsrp_rsrq2(tf.getRsrp_rsrq2()+1);
										}
										if(i_s_rsrp < LteMroThresholdSet.TWO_FOUR_RSRP 
												&& i_s_rsrq >= LteMroThresholdSet.TWO_FOUR_RSRQ){
											tf.setRsrp_rsrq3(tf.getRsrp_rsrq3()+1);
										}
										if(i_s_rsrp < LteMroThresholdSet.TWO_FOUR_RSRP 
												&& i_s_rsrq < LteMroThresholdSet.TWO_FOUR_RSRQ){
											tf.setRsrp_rsrq4(tf.getRsrp_rsrq4()+1);
										}
										//rsrp-rttd
										if(i_s_rsrp >= LteMroThresholdSet.TWO_FOUR_RSRP 
												&& i_s_rttd >= LteMroThresholdSet.TWO_FOUR_RTTD){
											tf.setRsrp_rttd1(tf.getRsrp_rttd1()+1);
										}
										if(i_s_rsrp >= LteMroThresholdSet.TWO_FOUR_RSRP 
												&& i_s_rttd < LteMroThresholdSet.TWO_FOUR_RTTD){
											tf.setRsrp_rttd2(tf.getRsrp_rttd2()+1);
										}
										if(i_s_rsrp < LteMroThresholdSet.TWO_FOUR_RSRP 
												&& i_s_rttd >= LteMroThresholdSet.TWO_FOUR_RTTD){
											tf.setRsrp_rttd3(tf.getRsrp_rttd3()+1);
										}
										if(i_s_rsrp < LteMroThresholdSet.TWO_FOUR_RSRP 
												&& i_s_rttd < LteMroThresholdSet.TWO_FOUR_RTTD){
											tf.setRsrp_rttd4(tf.getRsrp_rttd4()+1);
										}
										//rsrp-sinr_u
										if(i_s_rsrp >= LteMroThresholdSet.TWO_FOUR_RSRP 
												&& i_s_sinr_u >= LteMroThresholdSet.TWO_FOUR_SINRU){
											tf.setRsrp_sinru1(tf.getRsrp_sinru1()+1);
										}
										if(i_s_rsrp >= LteMroThresholdSet.TWO_FOUR_RSRP 
												&& i_s_sinr_u < LteMroThresholdSet.TWO_FOUR_SINRU){
											tf.setRsrp_sinru2(tf.getRsrp_sinru2()+1);
										}
										if(i_s_rsrp < LteMroThresholdSet.TWO_FOUR_RSRP 
												&& i_s_sinr_u >= LteMroThresholdSet.TWO_FOUR_SINRU){
											tf.setRsrp_sinru3(tf.getRsrp_sinru3()+1);
										}
										if(i_s_rsrp < LteMroThresholdSet.TWO_FOUR_RSRP 
												&& i_s_sinr_u < LteMroThresholdSet.TWO_FOUR_SINRU){
											tf.setRsrp_sinru4(tf.getRsrp_sinru4()+1);
										}
										//rttd-rsrq
										if(i_s_rttd >= LteMroThresholdSet.TWO_FOUR_RTTD
												&& i_s_rsrq >= LteMroThresholdSet.TWO_FOUR_RSRQ){
											tf.setRttd_rsrq1(tf.getRttd_rsrq1()+1);
										}
										if(i_s_rttd >= LteMroThresholdSet.TWO_FOUR_RTTD
												&& i_s_rsrq < LteMroThresholdSet.TWO_FOUR_RSRQ){
											tf.setRttd_rsrq2(tf.getRttd_rsrq2()+1);
										}
										if(i_s_rttd < LteMroThresholdSet.TWO_FOUR_RTTD
												&& i_s_rsrq >= LteMroThresholdSet.TWO_FOUR_RSRQ){
											tf.setRttd_rsrq3(tf.getRttd_rsrq3()+1);
										}
										if(i_s_rttd < LteMroThresholdSet.TWO_FOUR_RTTD
												&& i_s_rsrq < LteMroThresholdSet.TWO_FOUR_RSRQ){
											tf.setRttd_rsrq4(tf.getRttd_rsrq4()+1);
										}
										//sinru-rsrq
										if(i_s_sinr_u >= LteMroThresholdSet.TWO_FOUR_SINRU
												&& i_s_rsrq >= LteMroThresholdSet.TWO_FOUR_RSRQ){
											tf.setSinru_rsrq1(tf.getSinru_rsrq1()+1);
										}
										if(i_s_sinr_u >= LteMroThresholdSet.TWO_FOUR_SINRU
												&& i_s_rsrq < LteMroThresholdSet.TWO_FOUR_RSRQ){
											tf.setSinru_rsrq2(tf.getSinru_rsrq2()+1);
										}
										if(i_s_sinr_u < LteMroThresholdSet.TWO_FOUR_SINRU
												&& i_s_rsrq >= LteMroThresholdSet.TWO_FOUR_RSRQ){
											tf.setSinru_rsrq3(tf.getSinru_rsrq3()+1);
										}
										if(i_s_sinr_u < LteMroThresholdSet.TWO_FOUR_SINRU
												&& i_s_rsrq < LteMroThresholdSet.TWO_FOUR_RSRQ){
											tf.setSinru_rsrq4(tf.getSinru_rsrq4()+1);
										}
										//rttd-sinru
										if(i_s_rttd >= LteMroThresholdSet.TWO_FOUR_RTTD
												&& i_s_sinr_u >= LteMroThresholdSet.TWO_FOUR_SINRU){
											tf.setRttd_sinru1(tf.getRttd_sinru1()+1);
										}
										if(i_s_rttd >= LteMroThresholdSet.TWO_FOUR_RTTD
												&& i_s_sinr_u < LteMroThresholdSet.TWO_FOUR_SINRU){
											tf.setRttd_sinru2(tf.getRttd_sinru2()+1);
										}
										if(i_s_rttd < LteMroThresholdSet.TWO_FOUR_RTTD
												&& i_s_sinr_u >= LteMroThresholdSet.TWO_FOUR_SINRU){
											tf.setRttd_sinru3(tf.getRttd_sinru3()+1);
										}
										if(i_s_rttd < LteMroThresholdSet.TWO_FOUR_RTTD
												&& i_s_sinr_u < LteMroThresholdSet.TWO_FOUR_SINRU){
											tf.setRttd_sinru4(tf.getRttd_sinru4()+1);
										}
										isFirstSci = false;
									}
								}
								/*********二维四象限分析end*******/
								
								/*********覆盖评估及差值评估处理start*******/
								//主小区RSRP
								if(!s_rsrp.equals("NIL")){
									int rsrp = Integer.parseInt(s_rsrp);
									if(S_RSRP==-9999){
										S_RSRP = rsrp;
									}
									if(rsrp>MAX_RSRP){
										MAX_RSRP=rsrp;
									}
								}
								//邻区RSRP
								if(!n_rsrp.equals("NIL")){
									int rsrp = Integer.parseInt(n_rsrp);
									if(rsrp>MAX_RSRP){
										MAX_RSRP=rsrp;
									}
									if(rsrp>MAX_N_RSRP){
										MAX_N_RSRP=rsrp;
									}
								}
								/*********覆盖评估及差值评估处理end*******/
								
								if(!s_rsrp.equals("NIL") && !n_rsrp.equals("NIL") && !n_key.equals("NIL")){
									int i_s_rsrp = Integer.parseInt(s_rsrp);
									int i_n_rsrp = Integer.parseInt(n_rsrp);
									/*********过覆盖分析处理start*******/
									if(i_s_rsrp - STATIC_DBM > LteMroThresholdSet.OVER_COVER_N_POWER){
										if(i_s_rsrp - i_n_rsrp < LteMroThresholdSet.DBM_DIFF_THRESHOLD1){
											if(overMap.containsKey(ci)){
												Map<String,Integer> vm  = overMap.get(ci);
												if(vm.containsKey(n_key)){
													vm.put(n_key, vm.get(n_key)+1);
												}else{
													vm.put(n_key, 1);
												}
											}else{
												Map<String,Integer> vm  = new HashMap<String,Integer>();
												vm.put(n_key, 1);
												overMap.put(ci, vm);
											}
										}
									}
									/*********过覆盖分析处理end*******/
									
									/**
									 * 主小区大于-100
									 */
									if(i_s_rsrp - STATIC_DBM > -100){
										S_RSRP_100_FLAG = true;
									}
									/**
									 * 主小区大于-110
									 */
									if(i_s_rsrp - STATIC_DBM > -110){
										S_RSRP_110_FLAG = true;
									}
									
									/*********重叠覆盖分析处理start*******/
									if(i_s_rsrp - STATIC_DBM > LteMroThresholdSet.NEIGHBOR_CELL_DBM){
										if(i_s_rsrp - i_n_rsrp < LteMroThresholdSet.DBM_DIFF_THRESHOLD2){
											//满足重叠覆盖条件，计数+1
											overlaying++;
											//将满足条件的邻区放入集合
											if(olCoupleMap.containsKey(ci)){
												Map<String,Integer> vm  = olCoupleMap.get(ci);
												if(vm.containsKey(n_key)){
													vm.put(n_key, vm.get(n_key)+1);
												}else{
													vm.put(n_key, 1);
												}
											}else{
												Map<String,Integer> vm  = new HashMap<String,Integer>();
												vm.put(n_key, 1);
												olCoupleMap.put(ci, vm);
											}
										}
										/**
										 * 重叠覆盖强度处理start
										 */
										N_M_POWER += Math.pow(10, (i_n_rsrp-STATIC_DBM)/10.0);
										/**
										 * 重叠覆盖强度处理end
										 */
									}
									/*********重叠覆盖分析处理end*******/
									
									/*********孙泽星需求孤立小区统计*******/
									if(i_s_rsrp - STATIC_DBM < -110){
										aloneCellDeno++;
										if(i_s_rsrp - i_n_rsrp > LteMroThresholdSet.DBM_DIFF_THRESHOLD1){
											aloneCellNume++;
										}
									}
								}
							}
						}
					}else if(event.isEndElement()){
						EndElement endEle = event.asEndElement();
						String qName = endEle.getName().toString();
						if("object".equalsIgnoreCase(qName)){
							if(!ci.equals("")){
								/**
								 * 覆盖评估计算
								 */
								if(MAX_RSRP!=-9999){
									int dbm = MAX_RSRP-STATIC_DBM;
									String str_dbm = dbm+"";
									if(coverMap.containsKey(ci)){
										Map<String,Integer> vm = coverMap.get(ci);
										if(vm.containsKey(str_dbm)){
											vm.put(str_dbm, vm.get(str_dbm)+1);
										}else{
											vm.put(str_dbm, 1);
										}
									}else{
										Map<String,Integer> vm = new HashMap<String,Integer>();
										vm.put(str_dbm, 1);
										coverMap.put(ci, vm);
									}
								}
								/**
								 * 差值评估计算
								 */
								if(S_RSRP!=-9999 && MAX_N_RSRP!=-9999){
									int diff_dbm = S_RSRP - MAX_N_RSRP;
									if(diffMap.containsKey(ci)){
										Map<Integer,Integer> vm = diffMap.get(ci);
										if(vm.containsKey(diff_dbm)){
											vm.put(diff_dbm, vm.get(diff_dbm)+1);
										}else{
											vm.put(diff_dbm, 1);
										}
									}else{
										Map<Integer,Integer> vm = new HashMap<Integer,Integer>();
										vm.put(diff_dbm, 1);
										diffMap.put(ci, vm);
									}
								}
								
								/**
								 * 重叠覆盖强度统计
								 */
								if(N_M_POWER!=0){
									double v = Math.round(S_RSRP - STATIC_DBM - 10*Math.log10(N_M_POWER));
									if(v > 20){
										v = 21;
									}else if(v < -20){
										v = -21;
									}
									if(powerMap.containsKey(ci)){
										Map<Double,Integer> vm = powerMap.get(ci);
										if(vm.containsKey(v)){
											vm.put(v, vm.get(v)+1);
										}else{
											vm.put(v, 1);
										}
									}else{
										Map<Double,Integer> vm = new HashMap<Double,Integer>();
										vm.put(v, 1);
										powerMap.put(ci, vm);
									}
								}
								
								/**
								 * 小区总采样点数，过覆盖评估统计等使用
								 */
								if(countMap.containsKey(ci)){
									countMap.put(ci, countMap.get(ci)+1);
								}else{
									countMap.put(ci, 1);
								}
								
								/**
								 * 主小区电平>=-110的小区总采样点数
								 */
								if(S_RSRP_110_FLAG){
									if(count110Map.containsKey(ci)){
										count110Map.put(ci, count110Map.get(ci)+1);
									}else{
										count110Map.put(ci, 1);
									}
								}
								
								/**
								 * 主小区电平>=-100的小区总采样点数
								 */
								if(S_RSRP_100_FLAG){
									if(count100Map.containsKey(ci)){
										count100Map.put(ci, count100Map.get(ci)+1);
									}else{
										count100Map.put(ci, 1);
									}
								}
								
								
								/**
								 * 重叠覆盖评估分析
								 */
								if(overlayingMap.containsKey(ci)){
									ol = overlayingMap.get(ci);
									if(overlaying>=LteMroThresholdSet.OVER_LAYING_NC_NUM){
										ol.setOverlaying(ol.getOverlaying()+1);
									}
									if(overlaying>=3){
										ol.setOverlaying3(ol.getOverlaying3()+1);
									}
									if(overlaying>=5){
										ol.setOverlaying5(ol.getOverlaying5()+1);
									}
									if(overlaying>=6){
										ol.setOverlaying6(ol.getOverlaying6()+1);
									}
								}else{
									ol = new OverLayingVO();
									if(overlaying>=LteMroThresholdSet.OVER_LAYING_NC_NUM){
										ol.setOverlaying(ol.getOverlaying()+1);
									}
									if(overlaying>=3){
										ol.setOverlaying3(ol.getOverlaying3()+1);
									}
									if(overlaying>=5){
										ol.setOverlaying5(ol.getOverlaying5()+1);
									}
									if(overlaying>=6){
										ol.setOverlaying6(ol.getOverlaying6()+1);
									}
									overlayingMap.put(ci, ol);
								}
								
								/**
								 * 孙泽星孤立小区统计
								 */
								if(aloneCellMap.containsKey(ci)){
									ac = aloneCellMap.get(ci);
									ac.setAloneCellDeno(ac.getAloneCellDeno()+aloneCellDeno);
									ac.setAloneCellNume(ac.getAloneCellNume()+aloneCellNume);
								}else{
									ac = new AloneCell();
									ac.setAloneCellDeno(aloneCellDeno);
									ac.setAloneCellNume(aloneCellNume);
									aloneCellMap.put(ci, ac);
								}
							}
							/**
							 * 遍历一个object完成，重置变量
							 */
							ci = "";
							arrayLength = 0;
							MAX_RSRP = -9999;
							S_RSRP = -9999;
							MAX_N_RSRP = -9999;
							overlaying = 0;
							aloneCellDeno = 0;
							aloneCellNume = 0;
							isFirstSci = true;
							S_RSRP_110_FLAG = false;
							S_RSRP_100_FLAG = false;
							N_M_POWER = 0;
							isObject = false;
						}else if("measurement".equalsIgnoreCase(qName)){
							isMRO = false;
						}
					}
				}
			}catch(Exception e){
				log.error(this,e);
				isFileCorrect.add(false);
				break;
			}
			isFileCorrect.add(true);
		}
	}
	
	/**
	 * 将孤立小区结果数据插入表中
	 */
	public void insertAloneCell(){
		Connection conn = null;
		PreparedStatement stmt = null;
		String static_sql = "INSERT INTO t_lte_mr_alonecell_h" +
				" (dtm_repdate,i_omc_id,i_enodeb_id,i_cell_id) " +
				" VALUES(to_date(?,'yyyy-mm-dd hh24:mi:ss'),?,?,?)";
		try {
			DataSource.init();
			conn = DataSource.getConnection();
			if(conn==null){
				System.out.println("connection is null, exit!");
				System.exit(0);
			}
			stmt = conn.prepareStatement(static_sql);
			if(!aloneCellMap.isEmpty()){
				Set<Map.Entry<String, AloneCell>> set = aloneCellMap.entrySet();
				for(Iterator<Map.Entry<String, AloneCell>> it = set.iterator();it.hasNext();){
			 		Map.Entry<String, AloneCell> entry = it.next();
			 		String ci = entry.getKey();
			 		AloneCell ac = entry.getValue();
			 		double d =  ac.getAloneCellNume()*1.0/ac.getAloneCellDeno();
			 		if(d > 0.5){
			 			stmt.setString(1, dtm_repdate);
				 		stmt.setString(2, omcid);
				 		stmt.setInt(3, Integer.parseInt(enodeb));
				 		stmt.setInt(4, Integer.parseInt(ci));
				 		stmt.addBatch();
			 		}
				}
				stmt.executeBatch();
//				log.info(dtm_repdate+" "+enodeb+"孤立小区入库数据："+dataNum);
			}else{
//				log.info(dtm_repdate+" "+enodeb+"孤立小区无符合条件数据！");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				stmt.close();
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}finally{
				stmt=null;
				conn=null;
			}
		}
	}
	
	/**
	 * 将二维四象限分析结果数据插入表中
	 */
	public void insertTwoFour(){
		Connection conn = null;
		PreparedStatement stmt = null;
		String static_sql = "INSERT INTO LTE_ETL.T_LTE_MR_TWO_FOUR_H" +
				"(DTM_REPDATE,I_OMC_ID,I_ENODEB_ID,I_CELL_ID," +
				"RSRP_RSRQ1,RSRP_RSRQ2,RSRP_RSRQ3,RSRP_RSRQ4,RSRP_RTTD1,RSRP_RTTD2,RSRP_RTTD3,RSRP_RTTD4,RSRP_SINRU1,RSRP_SINRU2,RSRP_SINRU3,RSRP_SINRU4," +
				"RTTD_RSRQ1,RTTD_RSRQ2,RTTD_RSRQ3,RTTD_RSRQ4,SINRU_RSRQ1,SINRU_RSRQ2,SINRU_RSRQ3,SINRU_RSRQ4,RTTD_SINRU1,RTTD_SINRU2,RTTD_SINRU3,RTTD_SINRU4) " +
				"VALUES(to_date(?,'yyyy-mm-dd hh24:mi:ss'),?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		try {
			DataSource.init();
			conn = DataSource.getConnection();
			if(conn==null){
				System.out.println("connection is null, exit!");
				System.exit(0);
			}
			stmt = conn.prepareStatement(static_sql);
			if(!twoFourMap.isEmpty()){
				Set<Map.Entry<String, TwoFourVO>> set = twoFourMap.entrySet();
				for(Iterator<Map.Entry<String, TwoFourVO>> it = set.iterator();it.hasNext();){
			 		Map.Entry<String, TwoFourVO> entry = it.next();
			 		String ci = entry.getKey();
			 		TwoFourVO tf = entry.getValue();
			 		stmt.setString(1, dtm_repdate);
			 		stmt.setString(2, omcid);
			 		stmt.setInt(3, Integer.parseInt(enodeb));
			 		stmt.setInt(4, Integer.parseInt(ci));
			 		stmt.setInt(5, tf.getRsrp_rsrq1());
			 		stmt.setInt(6, tf.getRsrp_rsrq2());
			 		stmt.setInt(7, tf.getRsrp_rsrq3());
			 		stmt.setInt(8, tf.getRsrp_rsrq4());
			 		stmt.setInt(9, tf.getRsrp_rttd1());
			 		stmt.setInt(10, tf.getRsrp_rttd2());
			 		stmt.setInt(11, tf.getRsrp_rttd3());
			 		stmt.setInt(12, tf.getRsrp_rttd4());
			 		stmt.setInt(13, tf.getRsrp_sinru1());
			 		stmt.setInt(14, tf.getRsrp_sinru2());
			 		stmt.setInt(15, tf.getRsrp_sinru3());
			 		stmt.setInt(16, tf.getRsrp_sinru4());
			 		stmt.setInt(17, tf.getRttd_rsrq1());
			 		stmt.setInt(18, tf.getRttd_rsrq2());
			 		stmt.setInt(19, tf.getRttd_rsrq3());
			 		stmt.setInt(20, tf.getRttd_rsrq4());
			 		stmt.setInt(21, tf.getSinru_rsrq1());
			 		stmt.setInt(22, tf.getSinru_rsrq2());
			 		stmt.setInt(23, tf.getSinru_rsrq3());
			 		stmt.setInt(24, tf.getSinru_rsrq4());
			 		stmt.setInt(25, tf.getRttd_sinru1());
			 		stmt.setInt(26, tf.getRttd_sinru2());
			 		stmt.setInt(27, tf.getRttd_sinru3());
			 		stmt.setInt(28, tf.getRttd_sinru4());
			 		stmt.addBatch();
				}
				stmt.executeBatch();
//				log.info(dtm_repdate+" "+enodeb+"二维四象限分析入库数据："+dataNum);
			}else{
//				log.info(dtm_repdate+" "+enodeb+"二维四象限分析无符合条件数据！");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				stmt.close();
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}finally{
				stmt=null;
				conn=null;
			}
		}
	}
	
	
	/**
	 * 将重叠覆盖评估结果数据插入表中
	 */
	public void insertOverLaying(){
		Connection conn = null;
		PreparedStatement stmt = null;
		String static_sql = "INSERT INTO T_lte_mr_overlaying_cell_h" +
				" (dtm_repdate,i_omc_id,i_enodeb_id,i_cell_id,n_all_point,N_overlaying_rate_fz,n_overlaying_for_3cell_fz,n_overlaying_for_5cell_fz,n_overlaying_for_6cell_fz) " +
				" VALUES(to_date(?,'yyyy-mm-dd hh24:mi:ss'),?,?,?,?,?,?,?,?)";
		try {
			DataSource.init();
			conn = DataSource.getConnection();
			if(conn==null){
				System.out.println("connection is null, exit!");
				System.exit(0);
			}
			stmt = conn.prepareStatement(static_sql);
			if(!overlayingMap.isEmpty()){
				Set<Map.Entry<String, OverLayingVO>> set = overlayingMap.entrySet();
				for(Iterator<Map.Entry<String, OverLayingVO>> it = set.iterator();it.hasNext();){
			 		Map.Entry<String, OverLayingVO> entry = it.next();
			 		String ci = entry.getKey();
			 		OverLayingVO ol = entry.getValue();
			 		int counts = count110Map.get(ci)==null?0:count110Map.get(ci);
			 		stmt.setString(1, dtm_repdate);
			 		stmt.setString(2, omcid);
			 		stmt.setInt(3, Integer.parseInt(enodeb));
			 		stmt.setInt(4, Integer.parseInt(ci));
			 		stmt.setInt(5, counts);
			 		stmt.setInt(6, ol.getOverlaying());
			 		stmt.setInt(7, ol.getOverlaying3());
			 		stmt.setInt(8, ol.getOverlaying5());
			 		stmt.setInt(9, ol.getOverlaying6());
			 		stmt.addBatch();
				}
				stmt.executeBatch();
//				log.info(dtm_repdate+" "+enodeb+"重叠覆盖评估入库数据："+dataNum);
			}else{
//				log.info(dtm_repdate+" "+enodeb+"重叠覆盖评估无符合条件数据！");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				stmt.close();
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}finally{
				stmt=null;
				conn=null;
			}
		}
	}
	
	/**
	 * 将过覆盖评估结果数据插入表中
	 */
	public void insertOverCoverAssess(){
		Connection conn = null;
		PreparedStatement stmt = null;
		String static_sql = "insert into t_lte_mr_overCover_cell_h" +
				"(dtm_repdate,i_omc_id,i_Enodeb_Id,i_Cell_Id,n_All_Point,earfcn,pci,n_neighbor_point) " +
				"values(to_date(?,'yyyy-mm-dd hh24:mi:ss'),?,?,?,?,?,?,?)";
		try {
			DataSource.init();
			conn = DataSource.getConnection();
			stmt = conn.prepareStatement(static_sql);
			if(!overMap.isEmpty()){
				Set<Map.Entry<String, Map<String,Integer>>> set = overMap.entrySet();
//				int dataNum = 0;
				for(Iterator<Map.Entry<String, Map<String,Integer>>> it = set.iterator();it.hasNext();){
			 		Map.Entry<String, Map<String,Integer>> entry = it.next();
			 		String ci = entry.getKey();
			 		Map<String,Integer> vm = entry.getValue();
			 		Set<Map.Entry<String, Integer>> vs = vm.entrySet();
			 		for(Iterator<Map.Entry<String, Integer>> vi = vs.iterator();vi.hasNext();){
			 			Map.Entry<String, Integer> ve = vi.next();
			 			String nep = ve.getKey();//邻区earfcn-pci
			 			String[] temp = nep.split("-");
			 			int nfz = ve.getValue();//邻区过覆盖分子
			 			int counts = 0;
			 			if(LteMroThresholdSet.OVER_COVER_N_POWER==-110){
			 				counts = count110Map.get(ci)==null?0:count110Map.get(ci);
			 			}else{
			 				counts = count100Map.get(ci)==null?0:count100Map.get(ci);
			 			}
			 			stmt.setString(1, dtm_repdate);
				 		stmt.setString(2, omcid);
				 		stmt.setInt(3, Integer.parseInt(enodeb));
				 		stmt.setInt(4, Integer.parseInt(ci));
				 		stmt.setInt(5, counts);
				 		stmt.setInt(6, Integer.parseInt(temp[0]));
				 		stmt.setInt(7, Integer.parseInt(temp[1]));
				 		stmt.setInt(8, nfz);
				 		
				 		stmt.addBatch();
			 		}
			 		stmt.executeBatch();
				}
//				log.info(dtm_repdate+" "+enodeb+"过覆盖评估入库数据："+dataNum);
			}else{
//				log.info(dtm_repdate+" "+enodeb+"过覆盖评估无符合条件数据！");
			}
		}catch (IOException e) {
			e.printStackTrace();
		}catch (SQLException e) {
			e.printStackTrace();
		}finally {
			try {
				stmt.close();
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}finally{
				stmt=null;
				conn=null;
			}
		}
	}
	
	/**
	 * 将重叠覆盖小区对明细插入表中
	 */
	public void insertOverlayingCellCouple(){
		Connection conn = null;
		PreparedStatement stmt = null;
		String static_sql = "INSERT INTO LTE_ETL.T_LTE_MR_OLC_H " +
				"(DTM_REPDATE,I_OMC_ID,I_ENODEB_ID,I_CELL_ID,N_ALL_POINT,EARFCN,PCI,N_NEIGHBOR_POINT) " +
				"VALUES(TO_DATE(?,'YYYY-MM-DD HH24:MI:SS'),?,?,?,?,?,?,?)";
		try {
			DataSource.init();
			conn = DataSource.getConnection();
			stmt = conn.prepareStatement(static_sql);
			if(!olCoupleMap.isEmpty()){
				Set<Map.Entry<String, Map<String,Integer>>> set = olCoupleMap.entrySet();
				for(Iterator<Map.Entry<String, Map<String,Integer>>> it = set.iterator();it.hasNext();){
			 		Map.Entry<String, Map<String,Integer>> entry = it.next();
			 		String ci = entry.getKey();
			 		Map<String,Integer> vm = entry.getValue();
			 		Set<Map.Entry<String, Integer>> vs = vm.entrySet();
			 		for(Iterator<Map.Entry<String, Integer>> vi = vs.iterator();vi.hasNext();){
			 			Map.Entry<String, Integer> ve = vi.next();
			 			String nep = ve.getKey();//邻区earfcn-pci
			 			String[] temp = nep.split("-");
			 			int nfz = ve.getValue();//邻区满足条件采样点数
			 			int counts = count110Map.get(ci);
			 			stmt.setString(1, dtm_repdate);
			 			stmt.setString(2, omcid);
			 			stmt.setInt(3, Integer.parseInt(enodeb));
			 			stmt.setInt(4, Integer.parseInt(ci));
			 			stmt.setInt(5, counts);
			 			stmt.setInt(6, Integer.parseInt(temp[0]));
			 			stmt.setInt(7, Integer.parseInt(temp[1]));
			 			stmt.setInt(8, nfz);
			 			stmt.addBatch();
			 		}
				}
				stmt.executeBatch();
//				log.info(dtm_repdate+" "+enodeb+"重叠覆盖小区对明细入库数据："+dataNum);
			}else{
//				log.info(dtm_repdate+" "+enodeb+"重叠覆盖小区对明细无符合条件数据！");
			}
		}catch (IOException e) {
			e.printStackTrace();
		}catch (SQLException e) {
			e.printStackTrace();
		}finally {
			try {
				stmt.close();
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}finally{
				stmt=null;
				conn=null;
			}
		}
	}
	
	
	/**
	 * 将重叠覆盖强度分析结果数据插入表中
	 */
	public void insertPowerAssess(){
		Connection conn = null;
		Statement stmt = null;
		String static_sql = "INSERT INTO T_LTE_MR_POWERASSESS_H" +
				" ( columns ) " +
				" VALUES(to_date('"+dtm_repdate+"','yyyy-mm-dd hh24:mi:ss'),sysdate,params)";
		
		try {
			DataSource.init();
			conn = DataSource.getConnection();
			if(conn==null){
				System.out.println("connection is null, exit!");
				System.exit(0);
			}
			stmt = conn.createStatement();
			if(!powerMap.isEmpty()){
				Set<Map.Entry<String, Map<Double,Integer>>> set = powerMap.entrySet();
//				int dataNum = 0;
				for(Iterator<Map.Entry<String, Map<Double,Integer>>> it = set.iterator();it.hasNext();){
			 		Map.Entry<String, Map<Double,Integer>> entry = it.next();
			 		String ci = entry.getKey();
			 		int counts = count110Map.get(ci);
			 		StringBuffer columns = new StringBuffer("dtm_repdate,dtm_insertdate,i_omc_id,i_enodeb_id,i_cell_id,");
			 		StringBuffer params = new StringBuffer("'"+omcid+"',"+enodeb+","+ci+",") ;
			 		Map<Double,Integer> vm = entry.getValue();
			 		Set<Map.Entry<Double, Integer>> vs = vm.entrySet();
			 		for(Iterator<Map.Entry<Double, Integer>> vi = vs.iterator();vi.hasNext();){
			 			Map.Entry<Double, Integer> ve = vi.next();
			 			int dbm = ve.getKey().intValue();
			 			int count = ve.getValue();
			 			columns.append("N_DBM"+dbm+",");
			 			params.append(count+",");
			 		}
			 		columns.append("N_RSRP_ALL");
			 		params.append(counts);
			 		String sql = static_sql;
			 		sql = sql.replaceAll("columns", columns.toString());
			 		sql = sql.replaceAll("params", params.toString());
			 		sql = sql.replaceAll("-", "_");
			 		stmt.execute(sql);
				}
//				log.info(dtm_repdate+" "+enodeb+"覆盖评估入库数据量："+dataNum);
			}else{
//				log.info(dtm_repdate+" "+enodeb+"覆盖评估无符合条件数据！");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}finally {
			try {
				stmt.close();
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}finally{
				stmt=null;
				conn=null;
			}
		}
	}
	
	
	/**
	 * 将覆盖评估结果数据插入表中
	 */
	public void insertCoverAssess(){
		Connection conn = null;
		Statement stmt = null;
		String static_sql = "INSERT INTO T_LTE_MR_COVERASSESS_H" +
				" ( columns ) " +
				" VALUES(to_date('"+dtm_repdate+"','yyyy-mm-dd hh24:mi:ss'),params)";
		
		try {
			DataSource.init();
			conn = DataSource.getConnection();
			if(conn==null){
				System.out.println("connection is null, exit!");
				System.exit(0);
			}
			stmt = conn.createStatement();
			if(!coverMap.isEmpty()){
				Set<Map.Entry<String, Map<String,Integer>>> set = coverMap.entrySet();
//				int dataNum = 0;
				for(Iterator<Map.Entry<String, Map<String,Integer>>> it = set.iterator();it.hasNext();){
			 		Map.Entry<String, Map<String,Integer>> entry = it.next();
			 		String ci = entry.getKey();
			 		int counts = countMap.get(ci);
			 		StringBuffer columns = new StringBuffer("dtm_repdate,i_omc_id,i_enodeb_id,i_cell_id,");
			 		StringBuffer params = new StringBuffer("'"+omcid+"',"+enodeb+","+ci+",") ;
			 		Map<String,Integer> vm = entry.getValue();
			 		Set<Map.Entry<String, Integer>> vs = vm.entrySet();
			 		for(Iterator<Map.Entry<String, Integer>> vi = vs.iterator();vi.hasNext();){
			 			Map.Entry<String, Integer> ve = vi.next();
			 			String dbm = ve.getKey();
			 			int count = ve.getValue();
			 			columns.append("N_RSRP"+dbm+",");
			 			params.append(count+",");
			 		}
			 		columns.append("N_RSRP_ALL");
			 		params.append(counts);
			 		String sql = static_sql;
			 		sql = sql.replaceAll("columns", columns.toString());
			 		sql = sql.replaceAll("params", params.toString());
			 		sql = sql.replaceAll("-", "_");
			 		stmt.execute(sql);
				}
//				log.info(dtm_repdate+" "+enodeb+"覆盖评估入库数据量："+dataNum);
			}else{
//				log.info(dtm_repdate+" "+enodeb+"覆盖评估无符合条件数据！");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}finally {
			try {
				stmt.close();
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}finally{
				stmt=null;
				conn=null;
			}
		}
	}
	
	/**
	 * 将差值评估结果插入表中
	 */
	public void insertDiffAssess(){
		Connection conn = null;
		Statement stmt = null;
		String static_sql = "INSERT INTO T_LTE_MR_DIFFASSESS_H" +
				" ( columns ) " +
				" VALUES(to_date('"+dtm_repdate+"','yyyy-mm-dd hh24:mi:ss'),params)";
		
		try {
			DataSource.init();
			conn = DataSource.getConnection();
			if(conn==null){
				System.out.println("connection is null, exit!");
				System.exit(0);
			}
			stmt = conn.createStatement();
//			int dataNum = 0;
			if(!diffMap.isEmpty()){
				Set<Map.Entry<String, Map<Integer,Integer>>> set = diffMap.entrySet();
				for(Iterator<Map.Entry<String, Map<Integer,Integer>>> it = set.iterator();it.hasNext();){
			 		Map.Entry<String, Map<Integer,Integer>> entry = it.next();
			 		String ci = entry.getKey();
			 		int counts = countMap.get(ci);
			 		StringBuffer columns = new StringBuffer("dtm_repdate,i_omc_id,i_enodeb_id,i_cell_id,");
			 		StringBuffer params = new StringBuffer("'"+omcid+"',"+enodeb+","+ci+",") ;
			 		Map<Integer,Integer> vm = entry.getValue();
			 		Set<Map.Entry<Integer, Integer>> vs = vm.entrySet();
			 		int MAX_DBM = -9999;
			 		int MIN_DBM = 9999;
			 		for(Iterator<Map.Entry<Integer, Integer>> vi = vs.iterator();vi.hasNext();){
			 			Map.Entry<Integer, Integer> ve = vi.next();
			 			int diff_dbm = ve.getKey();
			 			int count = ve.getValue();
			 			columns.append("N_RSRP"+diff_dbm+",");
			 			params.append(count+",");
			 			if(diff_dbm > MAX_DBM){
			 				MAX_DBM = diff_dbm;
			 			}
			 			if(diff_dbm < MIN_DBM){
			 				MIN_DBM = diff_dbm;
			 			}
			 		}
			 		columns.append("INTERVAL_START,INTERVAL_END,N_RSRP_ALL");
			 		params.append(MIN_DBM+",");
			 		params.append(MAX_DBM+",");
			 		params.append(counts);
			 		String sql = static_sql;
			 		sql = sql.replaceAll("columns", columns.toString());
			 		sql = sql.replaceAll("-", "_");
			 		sql = sql.replaceAll("params", params.toString());
			 		stmt.execute(sql);
				}
//				log.info(dtm_repdate+" "+enodeb+"差值评估入库数据量："+dataNum);
			}else{
//				log.info(dtm_repdate+" "+enodeb+"差值评估无符合条件据！");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally {
			try {
				stmt.close();
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}finally{
				stmt=null;
				conn=null;
			}
		}
	}
	/**
	 * 判断是否有文件没有出错
	 * @return
	 */
	public boolean isFilesNotAllError(){
		boolean flag = false;
		for(boolean b : isFileCorrect){
			flag = b||flag;
		}
		return flag;
	}
	
	/**
	 * 删除生成的临时文件
	 */
	public void deleteTempFiles(){
		for(File temp : tempFileList){
			temp.delete();
		}
	}
	
	public static void main(String[] args){
//		String s = "20140311120000,";
//		System.out.println(s.substring(0, s.length()-1));
	}
}
