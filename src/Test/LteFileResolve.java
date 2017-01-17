package Test;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
/*****
 * 解析LTE配置的主程序
 * @author Administrator
 *
 */
public class LteFileResolve {
	protected static final Log log = LogFactory.getLog(LteFileResolve.class);	//创建日志文件
	/**解析的主程序
	 * @param args
	 */
	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		List li=new ArrayList();							//参数列表
		if(args.length<3){
			log.info("输入的参数个数不对");
		}else{
			if(args.length==3){								//三个参数：厂家，文件路径，OMCID
				li.add(args[0]);					
				li.add(args[1]);
				li.add(args[2].toUpperCase().trim());
			}else if(args.length==4){						////三个参数：厂家，文件路径，OMCID,日期（用于补采）
				li.add(args[0]);
				li.add(args[1]);
				li.add(args[2].toUpperCase().trim());
				li.add(args[3]);
			}
			if(args[0].equals("03")){							//当输入厂家ID为03时执行中兴的解析
				ZteCmFactory zteparse=new ZteCmFactory();
				zteparse.zteResolve(li);
			}else if(args[0].equals("01")){						//当输入厂家为01时执行诺西的解析
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
		//程序结束
	}
}
