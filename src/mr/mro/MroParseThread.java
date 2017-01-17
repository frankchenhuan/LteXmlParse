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
 * ʵ��lTE-MRO����Ķ��̲߳���������
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
				List<File> tempFiles = new ArrayList<File>();//��ʱ�ļ�����,����ɾ��
				List<File> fileList = new ArrayList<File>();//��ʱ�ļ����ϣ���������
				for(File file : files){
					//xml�ļ�ֱ�����
					if(file.getName().toLowerCase().endsWith("xml")){
						fileList.add(file);
					}else if(file.getName().toLowerCase().endsWith("gz")){//ѹ���ļ���������
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
				
				//ɾ����ʱ�ļ�
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
