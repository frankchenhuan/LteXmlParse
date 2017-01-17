package mr.mro;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.stream.XMLStreamException;

/**
 * 实现lTE-MRO处理的多线程并发处理功能
 * @author Administrator
 *
 */
public class MroParseThread implements Runnable {
	
	private Map<String,List<File>> fileMap = new HashMap<String,List<File>>();
	private String omcid;
	
	public MroParseThread(Map<String,List<File>> fileMap,String omcid){
		this.fileMap = fileMap;
		this.omcid = omcid;
	}
	
	private void parseMroFiles(){
		if(!fileMap.isEmpty()){
			Set<Map.Entry<String, List<File>>> entrySet = fileMap.entrySet();
			for(Iterator<Map.Entry<String, List<File>>> it = entrySet.iterator();it.hasNext(); ){
				Map.Entry<String, List<File>> entry = it.next();
				System.out.println(entry.getKey());
				List<File> files = entry.getValue();
				List<File> tempFiles = new ArrayList<File>();//临时文件集合,用来删除
				List<File> fileList = new ArrayList<File>();//临时文件集合，用来处理
				for(File file : files){
					//xml文件直接添加
					if(file.getName().toLowerCase().endsWith("xml")){
						fileList.add(file);
					}else if(file.getName().toLowerCase().endsWith("gz")){//压缩文件处理后添加
						try {
							File tempFile = Tool.unGZip(file);
							if(tempFile.getName().toLowerCase().endsWith("xml")){
								tempFiles.add(tempFile);
								fileList.add(tempFile);
							}
						} catch (IOException e) {
							e.printStackTrace();
						}
					}else if(file.getName().toLowerCase().endsWith("zip")){
						try {
							List<File> zipfiles = Tool.unZip(file);
							for(File f : zipfiles){
								if(f.getName().equalsIgnoreCase("xml")){
									tempFiles.add(f);
									fileList.add(f);
								}
							}
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
				
				try {
					MroParse mp = new MroParse(fileList,omcid);
					mp.parseMro();
					if(mp.isFilesNotAllError()){
						mp.insertCoverAssess();
						mp.insertDiffAssess();
						mp.insertOverLaying();
						mp.insertOverCoverAssess();
						mp.insertAloneCell();
						mp.insertTwoFour();
						mp.insertPowerAssess();
						mp.insertOverlayingCellCouple();
						mp.deleteTempFiles();
					}
				} catch (XMLStreamException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
				
				//删除临时文件
				for(File f : tempFiles){
					f.delete();
				}
			}
		}
	}
	
	@Override
	public void run() {
		parseMroFiles();
	}
}
