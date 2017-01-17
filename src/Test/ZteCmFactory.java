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
 * @author 马健
 * * JDOM 生成与解析XML文档  
 * *  
 *  */  
public class ZteCmFactory{   
	/****
	 * 生成批处理文件
	 * @param fileName
	 * @param objectlist
	 */
	protected static final Log log = LogFactory.getLog(ZteCmFactory.class);	//创建日志文件
	Connection conn=null;					//数据连接
	String dbUrl="jdbc:oracle:thin:@10.217.8.54:1521:nossdb2";	//数据库连接字符串
	String userName="lte_test";				//数据库账户
	String passWord="lte_000test";			//数据库账户密码
	String dateFormat="yyyy-mm-dd";			//日期格式的设置
	String insertDate="";					//用途插入数据表中DTM_REPDATE
	static String filePath="";				//文件夹路径
	Integer insertCounts=10000;				//记录插入数
	String config_FilePath="\\src\\rel\\Zte_Object.xml";		//需要传入的数据
	@SuppressWarnings("unchecked")
	LinkedHashMap psMap=new LinkedHashMap();	//放着常量的对象和SQL
	@SuppressWarnings("unchecked")
	Map fnMap=new LinkedHashMap();	//放着常量的Map
	//List objectlist=new ArrayList();			//放着对象的属性与对象名
	@SuppressWarnings("unchecked")
	Map objectMaps=new LinkedHashMap();		//放着对象的属性与对象名
	//遍历后的顺序
	@SuppressWarnings("unchecked")
	Map mapsort=new LinkedHashMap();
	@SuppressWarnings("unchecked")
	public void parseZteXml(String fileName){
		SAXBuilder builder = new SAXBuilder();
		Document doc;
		try {
			doc = builder.build(fileName);
			Element root=doc.getRootElement(); //获取根元素
			//获得厂家类型和文件日期
			Element li_fa=root.getChild("FileHeader");
			String date_time=li_fa.getChildText("DateTime");
			fnMap.put("DATETIME", date_time);
			Element li=root.getChild("Objects");
			String entryname=li.getChildText("ObjectType").toUpperCase().trim();		//获得对象名
			PreparedStatement ps=(PreparedStatement)psMap.get(entryname);				//获得PS对象	
			/****
			 * 获得对象的属性
			 */
			Element fieldName=li.getChild("FieldName");
			List lits=fieldName.getChildren("N");
			/****
			 * 获得实体对象列表
			 */
			Element fieldValue=li.getChild("FieldValue");
			List lit=fieldValue.getChildren("Cm");
			Set<String> objectkeys = objectMaps.keySet();			//对象属性集合
			for (Iterator its = objectkeys.iterator(); its.hasNext();) {
				String objectname = its.next().toString();
				if(entryname.equals(objectname)){					//找到对应的对象
					Map maps=(LinkedHashMap)objectMaps.get(entryname);
					Set<String> prokey = maps.keySet();								//获得对象属性的名称		
					for (Iterator itp = prokey.iterator(); itp.hasNext();) {
				        String objectPro = (String) itp.next();						//属性遍历对比
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
				 //遍历对象数组进行批入库
				 log.info("对象名:"+entryname);
				 log.info("有"+lit.size()+"个对象");
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
			log.error("解析实体文件错误",e);
			e.printStackTrace();
		} catch (Exception e) {
			mapsort.clear();
			e.printStackTrace();
			log.error("解析实体文件错误",e);
		}
	}
	/*****
	 * 获得中兴的对象列表常量
	 * 
	 */
	@SuppressWarnings("unchecked")
	public  void  parseZtePro(){
		SAXBuilder builder = new SAXBuilder();
		Document doc;
		try {
			doc = builder.build(config_FilePath);
			Element root=doc.getRootElement(); //获取根元素
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
			 * 遍历LINKEDHASHMAP
			 */
//			for(int k=0;k<list.size();k++){
//				Map maps=(LinkedHashMap)list.get(k);
//				Set<String> key = maps.keySet();
//				int z=0;
//			    for (Iterator it = key.iterator(); it.hasNext();) {
//			        String s = (String) it.next();
//			        System.out.println("下标："+z+"  KEY:"+s.toUpperCase()+"****"+"VAULE:"+maps.get(s));
//			        z++;
//			    }
//			}	
		} catch (JDOMException e) {
			e.printStackTrace();
			log.error("JDOM解析对象XML出错", e);
		} catch (IOException e) {
			e.printStackTrace();
			log.error("读取对象XML文件出错", e);
		}
	}
	/***
	 * 根据厂家对象的配置文件获得一个对象和SQL的集合
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public  void  parseZteSqlMap(){
		SAXBuilder builder = new SAXBuilder();
		Document doc;
		try {
			doc = builder.build(config_FilePath);
			Element root=doc.getRootElement(); //获取根元素
			List li=root.getChildren("Object");
			for(int i=0;i<li.size();i++){
				Element el=(Element)li.get(i);
				PreparedStatement ps = conn.prepareStatement(el.getChildText("Sql").toString());
				psMap.put(el.getChildText("ObjectType").toUpperCase().trim(),ps);
			}
		} catch (JDOMException e) {
			log.error("JDOM解析对象XML出错", e);
			e.printStackTrace();
		} catch (IOException e) {
			log.error("读取对象XML文件出错", e);
			e.printStackTrace();
		}catch(SQLException e){
			log.error("创建PreparedStatement对象发生错误", e);
			e.printStackTrace();
		}
	}
	/****
	 * 读取配置文件
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
			log.info("读取配置文件完毕");
		} catch (Exception e) {
			log.error("加载配置文件错误", e);
		}
	}
	/***
	 * 创建ORALCE数据连接
	 */
	 public void openConn() {  
		    try {  
		        Class.forName("oracle.jdbc.driver.OracleDriver").newInstance();  
		        conn = DriverManager.getConnection(dbUrl, userName, passWord); 
		        log.info("数据库连接成功");
		    } catch (Exception e) {
		    	log.error("建立连接失败", e);
		        e.printStackTrace();  
		    }  
		 }
	 /****
	  * 关闭ORALCE数据库连接
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
				 log.info("数据库连接关闭成功");
			  } catch (Exception e) {
				 log.error("连接关闭错误", e);
				 e.printStackTrace();  
			}  
		 } 
	/****
	 * 自动设置数据时间
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
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
		test.ReadPro(argList.get(2).toString());	
		test.autoDate(argList);
		FtpProcesser fp=new FtpProcesser();
		List li=fp.showAllFiles(filePath);			//遍历文件夹
		log.info("解析数据文档开始日间为"+df.format(new Date()));
		test.openConn();				//打开数据库连接
		test.parseZtePro();
		test.parseZteSqlMap();
		if(li.size()>0){
			for(int i=0;i<li.size();i++){
				test.parseZteXml(li.get(i).toString());
				log.info(li.get(i).toString());
			}
		}
		test.closeConn();
		log.info("解析数据文档结束时间为"+df.format(new Date()));
	}
} 