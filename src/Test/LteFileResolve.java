package Test;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
/*****
 * ����LTE���õ�������
 * @author Administrator
 *
 */
public class LteFileResolve {
	protected static final Log log = LogFactory.getLog(LteFileResolve.class);	//������־�ļ�
	/**������������
	 * @param args
	 */
	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		List li=new ArrayList();							//�����б�
		if(args.length<3){
			log.info("����Ĳ�����������");
		}else{
			if(args.length==3){								//�������������ң��ļ�·����OMCID
				li.add(args[0]);					
				li.add(args[1]);
				li.add(args[2].toUpperCase().trim());
			}else if(args.length==4){						////�������������ң��ļ�·����OMCID,���ڣ����ڲ��ɣ�
				li.add(args[0]);
				li.add(args[1]);
				li.add(args[2].toUpperCase().trim());
				li.add(args[3]);
			}
			if(args[0].equals("03")){							//�����볧��IDΪ03ʱִ�����˵Ľ���
				ZteCmFactory zteparse=new ZteCmFactory();
				zteparse.zteResolve(li);
			}else if(args[0].equals("01")){						//�����볧��Ϊ01ʱִ��ŵ���Ľ���
				NsCmFactory nsparse=new NsCmFactory();
				nsparse.nsResolve(li);
			}
		}
//		li.add("01");
//		li.add("d:\\aaa");
//		li.add("NX_OMC01");
//		li.add("2013-12-11");
//		NsCmFactory nsparse=new NsCmFactory();
//		nsparse.nsResolve(li);
		//�������
	}
}
