package Test;


import java.io.IOException;   
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List; 
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder; 
/****
 * 此类用于测试诺西的LTE配置数据
 * @author 马健
 *测试文件名：CMCC-ENB-NRM-V2.0.0-20131108-0200P00.xml
 */
public class NsCmTestFactory {
	/****
	 * 解析诺西的LTE的配置文件
	 * @param fileName
	 */
	@SuppressWarnings("unchecked")
	public void parseNsXml(String fileName){
		SAXBuilder builder = new SAXBuilder();		//创建一个SAX构造器
		Document doc;								//生成一个文档对象
		try {
			doc = builder.build(fileName);
			Element root=doc.getRootElement(); //获取根元素
			List li=root.getChildren("Objects");
			System.out.println("此文档中有"+li.size()+"个不同对象");
			System.out.println("***************************************");
			System.out.println("对象名明细如下：");
			for(int i=0;i<li.size();i++){
				Element el_Object=(Element)li.get(i);
				System.out.println("对象第"+(i+1)+"个"+el_Object.getChildText("ObjectType"));
				System.out.println("详细属性如下：");
				//用于提取对象的属性
				Element el_Pro=el_Object.getChild("FieldName");
				List lt=el_Pro.getChildren("N");
				//用于提取对象的值
				Element el_Value=el_Object.getChild("FieldValue");
				List tl=el_Value.getChildren("Cm");
				System.out.println("对象"+el_Object.getChildText("ObjectType")+"实体对象有："+tl.size()+"个");
				//下面的循环用于遍历对象的属性
				for(int j=0;j<lt.size();j++){
					Element elements=(Element)lt.get(j); 
					System.out.println("属性"+(j+1)+":"+elements.getValue());
				}
				//下面用于遍历实体对象
				for(int k=0;k<tl.size();k++){
					Element element=(Element)tl.get(k); 
					List lit=element.getChildren("V");
				//下面用于遍历实体对象的值明细
					 for(int j=0;j<lit.size();j++){
						 Element elementt=(Element)lit.get(j); 
						 System.out.println("V值"+(j+1)+":"+elementt.getValue());
					 }
				}
			}
			}catch (JDOMException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}
	public static void main(String[] args){
		NsCmTestFactory test=new NsCmTestFactory();
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss S");//设置日期格式
		System.out.println("解析数据文档开始日间为"+df.format(new Date()));
		test.parseNsXml("CMCC-ENB-NRM-V2.0.0-20131108-0200P00.xml");
		System.out.println("解析数据文档结束时间为"+df.format(new Date()));
	}
}
