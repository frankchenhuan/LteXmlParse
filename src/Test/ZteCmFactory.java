package Test;  

import java.io.IOException;   
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List; 
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder; 

import data.ReadProperties;
import ftp.FtpProcesser;
/**  *   * 
 * @author ��
 * * JDOM ���������XML�ĵ�  
 * *  
 *  */  
public class ZteCmFactory{   
	/****
	 * �����������ļ�
	 * @param fileName
	 * @param objectlist
	 */
	protected static final Log log = LogFactory.getLog(ZteCmFactory.class);	//������־�ļ�
	Connection conn=null;					//��������
	String dbUrl="jdbc:oracle:thin:@10.217.8.54:1521:nossdb2";	//���ݿ������ַ���
	String userName="lte_test";				//���ݿ��˻�
	String passWord="lte_000test";			//���ݿ��˻�����
	String dateFormat="yyyy-mm-dd";			//���ڸ�ʽ������
	String insertDate="";					//��;�������ݱ���DTM_REPDATE
	static String filePath="";				//�ļ���·��
	Integer insertCounts=10000;				//��¼������
	String config_FilePath="\\src\\rel\\Zte_Object.xml";		//��Ҫ���������
	@SuppressWarnings("unchecked")
	LinkedHashMap psMap=new LinkedHashMap();	//���ų����Ķ����SQL
	@SuppressWarnings("unchecked")
	Map fnMap=new LinkedHashMap();	//���ų�����Map
	//List objectlist=new ArrayList();			//���Ŷ���������������
	@SuppressWarnings("unchecked")
	Map objectMaps=new LinkedHashMap();		//���Ŷ���������������
	//�������˳��
	@SuppressWarnings("unchecked")
	Map mapsort=new LinkedHashMap();
	@SuppressWarnings("unchecked")
	public void parseZteXml(String fileName){
		SAXBuilder builder = new SAXBuilder();
		Document doc;
		try {
			doc = builder.build(fileName);
			Element root=doc.getRootElement(); //��ȡ��Ԫ��
			//��ó������ͺ��ļ�����
			Element li_fa=root.getChild("FileHeader");
			String date_time=li_fa.getChildText("DateTime");
			fnMap.put("DATETIME", date_time);
			Element li=root.getChild("Objects");
			String entryname=li.getChildText("ObjectType").toUpperCase().trim();		//��ö�����
			PreparedStatement ps=(PreparedStatement)psMap.get(entryname);				//���PS����	
			/****
			 * ��ö��������
			 */
			Element fieldName=li.getChild("FieldName");
			List lits=fieldName.getChildren("N");
			/****
			 * ���ʵ������б�
			 */
			Element fieldValue=li.getChild("FieldValue");
			List lit=fieldValue.getChildren("Cm");
			Set<String> objectkeys = objectMaps.keySet();			//�������Լ���
			for (Iterator its = objectkeys.iterator(); its.hasNext();) {
				String objectname = its.next().toString();
				if(entryname.equals(objectname)){					//�ҵ���Ӧ�Ķ���
					Map maps=(LinkedHashMap)objectMaps.get(entryname);
					Set<String> prokey = maps.keySet();								//��ö������Ե�����		
					for (Iterator itp = prokey.iterator(); itp.hasNext();) {
				        String objectPro = (String) itp.next();						//���Ա����Ա�
				        String str_value=maps.get(objectPro).toString();
				        String []bb=str_value.split("_");
				        for(int j=0;j<lits.size();j++){
				        	Element elements=(Element)lits.get(j); 
							String proName=elements.getValue().toUpperCase().trim();
							if(proName.equals(objectPro)){
								String str_Set=(j+1)+"_"+bb[1]+"_"+bb[2]+"_"+bb[0];
								mapsort.put(proName,str_Set);
								break; 
							}else{
								String str_Set="null_"+bb[1]+"_"+bb[2]+"_"+bb[0];
								mapsort.put(objectPro,str_Set);
							}
				        }
				    }
				 //��������������������
				 log.info("������:"+entryname);
				 log.info("��"+lit.size()+"������");
				 for(int t=0;t<lit.size();t++){
					 Element cm=(Element)lit.get(t);
					 List li_value=cm.getChildren("V");					 
					 Set<String> sortkeys = mapsort.keySet();
					 int counts=1;
					 for (Iterator itt = sortkeys.iterator(); itt.hasNext();) {
					        String object = (String) itt.next();
					        String[] aa=mapsort.get(object).toString().split("_");
					        if(aa[0].equals("null")){
					        	if(aa[2].equals("1")){
					        		if(aa[1].equals("int")){
					        			ps.setInt(Integer.parseInt(aa[3]),Integer.parseInt(fnMap.get(object).toString()));
					        		}else if(aa[1].equals("string")){
					        			ps.setString(Integer.parseInt(aa[3]),fnMap.get(object).toString());
					        		}else if(aa[1].equals("float")){
					        			ps.setFloat(Integer.parseInt(aa[3]),Float.parseFloat(fnMap.get(object).toString()));
					        		}else if(aa[1].equals("date")){
					        			ps.setDate(Integer.parseInt(aa[3]),java.sql.Date.valueOf(fnMap.get(object).toString()));
					        		}
					        	}else{
					        		if(aa[1].equals("int")){
						        		ps.setNull(Integer.parseInt(aa[3]), Types.INTEGER);
							        }else if(aa[1].equals("string")){
							        	ps.setString(Integer.parseInt(aa[3]),"");
							        }else if(aa[1].equals("float")){
							        	ps.setNull(Integer.parseInt(aa[3]),Types.FLOAT);
							        }else if(aa[1].equals("date")){
							        	ps.setNull(Integer.parseInt(aa[3]), Types.DATE);
							        }else if(aa[1].equals("null")){
							        	ps.setNull(Integer.parseInt(aa[3]), Types.NULL);
							        }
					        	}
					        }else{
					        	int lt=Integer.parseInt(aa[0]);
					        	Element value=(Element)li_value.get(lt-1);
							 	String v = value.getValue();
						        if(aa[1].equals("int")){
						        	if(v.replace("{","").replace("}","").length()>0){
						        		ps.setInt(Integer.parseInt(aa[3]),Integer.parseInt(v));
						        	}else{
						        		ps.setNull(Integer.parseInt(aa[3]), Types.INTEGER);
						        	}
						        }else if(aa[1].equals("string")){
						        	if(v.replace("{","").replace("}","").length()>0){
						        		ps.setString(Integer.parseInt(aa[3]),v.toString());
						        	}else{
						        		ps.setString(Integer.parseInt(aa[3]),"");
						        	}
						        }else if(aa[1].equals("float")){
						        	if(v.replace("{","").replace("}","").length()>0){
						        		ps.setFloat(Integer.parseInt(aa[3]),Float.parseFloat(v));
						        	}else{
						        		ps.setNull(Integer.parseInt(aa[3]),Types.FLOAT);
						        	}
						        }else if(aa[1].equals("date")){
						        	if(v.replace("{","").replace("}","").length()>0){
						        		ps.setDate(Integer.parseInt(aa[3]),java.sql.Date.valueOf(v));
						        	}else{
						        		ps.setNull(Integer.parseInt(aa[3]),Types.DATE);
						        	}
						        }else if(aa[1].equals("null")){
						        	ps.setNull(Integer.parseInt(aa[3]), Types.NULL);
						        }
					        }
					    } 
					 ps.addBatch();
					 counts++;
					 if(counts>=insertCounts){
						ps.executeBatch();
						counts=0;
					 	}
					}
				}
			}
			mapsort.clear();
		} catch (JDOMException e) {
			mapsort.clear();
			log.error("����ʵ���ļ�����",e);
			e.printStackTrace();
		} catch (Exception e) {
			mapsort.clear();
			e.printStackTrace();
			log.error("����ʵ���ļ�����",e);
		}
	}
	/*****
	 * ������˵Ķ����б���
	 * 
	 */
	@SuppressWarnings("unchecked")
	public  void  parseZtePro(){
		SAXBuilder builder = new SAXBuilder();
		Document doc;
		try {
			doc = builder.build(config_FilePath);
			Element root=doc.getRootElement(); //��ȡ��Ԫ��
			List li=root.getChildren("Object");
			for(int i=0;i<li.size();i++){
				Element el=(Element)li.get(i);
				Map map=new LinkedHashMap();
				Element fieldName=el.getChild("FieldName");
				List ln=fieldName.getChildren("N");
				for(int j=0;j<ln.size();j++){
					Element en=(Element)ln.get(j);
					map.put(en.getValue().toUpperCase().trim(),en.getAttributeValue("i")+"_"+en.getAttributeValue("type")+"_"+en.getAttributeValue("flag"));
				}
				objectMaps.put(el.getChildText("ObjectType").toUpperCase().trim(), map);
			}
			/****
			 * ����LINKEDHASHMAP
			 */
//			for(int k=0;k<list.size();k++){
//				Map maps=(LinkedHashMap)list.get(k);
//				Set<String> key = maps.keySet();
//				int z=0;
//			    for (Iterator it = key.iterator(); it.hasNext();) {
//			        String s = (String) it.next();
//			        System.out.println("�±꣺"+z+"  KEY:"+s.toUpperCase()+"****"+"VAULE:"+maps.get(s));
//			        z++;
//			    }
//			}	
		} catch (JDOMException e) {
			e.printStackTrace();
			log.error("JDOM��������XML����", e);
		} catch (IOException e) {
			e.printStackTrace();
			log.error("��ȡ����XML�ļ�����", e);
		}
	}
	/***
	 * ���ݳ��Ҷ���������ļ����һ�������SQL�ļ���
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public  void  parseZteSqlMap(){
		SAXBuilder builder = new SAXBuilder();
		Document doc;
		try {
			doc = builder.build(config_FilePath);
			Element root=doc.getRootElement(); //��ȡ��Ԫ��
			List li=root.getChildren("Object");
			for(int i=0;i<li.size();i++){
				Element el=(Element)li.get(i);
				PreparedStatement ps = conn.prepareStatement(el.getChildText("Sql").toString());
				psMap.put(el.getChildText("ObjectType").toUpperCase().trim(),ps);
			}
		} catch (JDOMException e) {
			log.error("JDOM��������XML����", e);
			e.printStackTrace();
		} catch (IOException e) {
			log.error("��ȡ����XML�ļ�����", e);
			e.printStackTrace();
		}catch(SQLException e){
			log.error("����PreparedStatement����������", e);
			e.printStackTrace();
		}
	}
	/****
	 * ��ȡ�����ļ�
	 */
	public void	ReadPro(String omcid){
		try {
			InputStream in =ReadProperties.class.getClassLoader().getResourceAsStream("config.properties");
			Properties prop = new Properties(); 
			prop.load(in);
			String dburl = prop.getProperty("dburl");
			String username = prop.getProperty("username");
			String password = prop.getProperty("password");
			String config_filepath=prop.getProperty(omcid);
			String insertcounts=prop.getProperty("insertcounts");
			String dateformat=prop.getProperty("dateformat");
			if(dburl.length()>0){
				dbUrl=dburl;
			}
			if(username.length()>0){
				userName=username;
			}
			if(password.length()>0){
				passWord=password;
			}
			if(config_filepath.length()>0){
				config_FilePath=config_filepath;
			}
			if(insertcounts.length()>0){
				insertCounts=Integer.parseInt(insertcounts);
			}
			if(dateformat.length()>0){
				dateFormat=dateformat;
			}
			log.info("��ȡ�����ļ����");
		} catch (Exception e) {
			log.error("���������ļ�����", e);
		}
	}
	/***
	 * ����ORALCE��������
	 */
	 public void openConn() {  
		    try {  
		        Class.forName("oracle.jdbc.driver.OracleDriver").newInstance();  
		        conn = DriverManager.getConnection(dbUrl, userName, passWord); 
		        log.info("���ݿ����ӳɹ�");
		    } catch (Exception e) {
		    	log.error("��������ʧ��", e);
		        e.printStackTrace();  
		    }  
		 }
	 /****
	  * �ر�ORALCE���ݿ�����
	  */
	@SuppressWarnings("unchecked")
	public void closeConn() {  
			 try {
				Set<String> key = psMap.keySet();
				    for (Iterator it = key.iterator(); it.hasNext();) {
				        String s = (String) it.next();
				        PreparedStatement ps=(PreparedStatement)psMap.get(s);	
				        ps.executeBatch();
				        ps.close();
				    }
				 conn.close();
				 log.info("���ݿ����ӹرճɹ�");
			  } catch (Exception e) {
				 log.error("���ӹرմ���", e);
				 e.printStackTrace();  
			}  
		 } 
	/****
	 * �Զ���������ʱ��
	 */
	@SuppressWarnings("unchecked")
	public void autoDate(List li){
		if(li.size()<4){
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			Calendar cal = Calendar.getInstance();
			cal.add(Calendar.DATE, -1);
			insertDate=sdf.format(cal.getTime());
			filePath=li.get(1).toString();
			fnMap.put("DTM_REPDATE", insertDate.toString());
			fnMap.put("I_FACTORY_ID",li.get(0).toString());
			fnMap.put("I_OMC_ID",li.get(2));
		}else{
			filePath=li.get(1).toString();
			fnMap.put("DTM_REPDATE", li.get(3).toString());
			fnMap.put("I_FACTORY_ID",li.get(0).toString());
			fnMap.put("I_OMC_ID",li.get(2));
		}
		
	}
	@SuppressWarnings("unchecked")
	public  void zteResolve(List argList){
		ZteCmFactory test=new ZteCmFactory();
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//�������ڸ�ʽ
		test.ReadPro(argList.get(2).toString());	
		test.autoDate(argList);
		FtpProcesser fp=new FtpProcesser();
		List li=fp.showAllFiles(filePath);			//�����ļ���
		log.info("���������ĵ���ʼ�ռ�Ϊ"+df.format(new Date()));
		test.openConn();				//�����ݿ�����
		test.parseZtePro();
		test.parseZteSqlMap();
		if(li.size()>0){
			for(int i=0;i<li.size();i++){
				test.parseZteXml(li.get(i).toString());
				log.info(li.get(i).toString());
			}
		}
		test.closeConn();
		log.info("���������ĵ�����ʱ��Ϊ"+df.format(new Date()));
	}
} 