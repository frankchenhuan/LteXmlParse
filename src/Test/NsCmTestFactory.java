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
 * �������ڲ���ŵ����LTE��������
 * @author ��
 *�����ļ�����CMCC-ENB-NRM-V2.0.0-20131108-0200P00.xml
 */
public class NsCmTestFactory {
	/****
	 * ����ŵ����LTE�������ļ�
	 * @param fileName
	 */
	@SuppressWarnings("unchecked")
	public void parseNsXml(String fileName){
		SAXBuilder builder = new SAXBuilder();		//����һ��SAX������
		Document doc;								//����һ���ĵ�����
		try {
			doc = builder.build(fileName);
			Element root=doc.getRootElement(); //��ȡ��Ԫ��
			List li=root.getChildren("Objects");
			System.out.println("���ĵ�����"+li.size()+"����ͬ����");
			System.out.println("***************************************");
			System.out.println("��������ϸ���£�");
			for(int i=0;i<li.size();i++){
				Element el_Object=(Element)li.get(i);
				System.out.println("�����"+(i+1)+"��"+el_Object.getChildText("ObjectType"));
				System.out.println("��ϸ�������£�");
				//������ȡ���������
				Element el_Pro=el_Object.getChild("FieldName");
				List lt=el_Pro.getChildren("N");
				//������ȡ�����ֵ
				Element el_Value=el_Object.getChild("FieldValue");
				List tl=el_Value.getChildren("Cm");
				System.out.println("����"+el_Object.getChildText("ObjectType")+"ʵ������У�"+tl.size()+"��");
				//�����ѭ�����ڱ������������
				for(int j=0;j<lt.size();j++){
					Element elements=(Element)lt.get(j); 
					System.out.println("����"+(j+1)+":"+elements.getValue());
				}
				//�������ڱ���ʵ�����
				for(int k=0;k<tl.size();k++){
					Element element=(Element)tl.get(k); 
					List lit=element.getChildren("V");
				//�������ڱ���ʵ������ֵ��ϸ
					 for(int j=0;j<lit.size();j++){
						 Element elementt=(Element)lit.get(j); 
						 System.out.println("Vֵ"+(j+1)+":"+elementt.getValue());
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
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss S");//�������ڸ�ʽ
		System.out.println("���������ĵ���ʼ�ռ�Ϊ"+df.format(new Date()));
		test.parseNsXml("CMCC-ENB-NRM-V2.0.0-20131108-0200P00.xml");
		System.out.println("���������ĵ�����ʱ��Ϊ"+df.format(new Date()));
	}
}
