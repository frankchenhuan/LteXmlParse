package mr.mro;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
/**
 * lte-MRO�ļ�������ڳ���
 * @author Administrator
 *
 */
public class MroParseMain {
	/**
	 * Ŀ¼
	 */
	private String rootDir;
	private String omcid;
	
	public MroParseMain(String dir,String omcid){
		this.rootDir = dir;
		this.omcid = omcid;
	}
	
	/**
	 * �ϲ�ͬenodebͬСʱ��MRO�����ļ�,���ö��̴߳����ļ�����
	 * @param path
	 */
	public void fireInTheHole(){
		if(rootDir!=null && !"".equals(rootDir)){
			File omcFile = new File(rootDir);
			if(omcFile.isDirectory()){
				File[] files = omcFile.listFiles();
				Map<String,List<File>> fileMap = new HashMap<String,List<File>>();
				for(File fi : files){
					String name = fi.getName();
					if(name.indexOf("MRO")!=-1){
						String s = name.substring(0, name.indexOf(".")-4);
						if(fileMap.containsKey(s)){
							List<File> fileList = fileMap.get(s);
							fileList.add(fi);
						}else{
							List<File> fileList = new ArrayList<File>();
							fileList.add(fi);
							fileMap.put(s, fileList);
						}
					}
				}
				MroParseThread mp = new MroParseThread(fileMap,omcid);
				Thread t = new Thread(mp);
				t.start();
			}
		}
	}
	
	public static void main(String[] args){
		if(args.length<0){
			System.out.println("�������㣡");
			System.exit(0);
		}
		
		MroParseMain md = new MroParseMain(args[0], args[1]);
		try {
			md.fireInTheHole();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
