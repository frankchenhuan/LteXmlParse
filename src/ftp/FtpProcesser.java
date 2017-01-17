package ftp;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import Test.ZteCmFactory;
import sun.net.TelnetInputStream;
import sun.net.ftp.FtpClient;

/********
 * ��������FTP���Ӳ���
 * 
 * @author �?
 * 
 */
public class FtpProcesser {
	/**
	 * ����ftp������
	 * 
	 * @param ip
	 * @param port
	 * @param user
	 * @param pwd
	 * @return
	 * @throws Exception
	 */
	// String ip;
	// int port;
	// String user;
	// String pwd;
	// String remotePath;
	// String localPath;
	static FtpClient ftpClient;
	protected static final Log log = LogFactory.getLog(FtpProcesser.class);	//������־�ļ�
	public static boolean connectServer(String ip, int port, String user,
			String pwd) throws Exception {
		boolean isSuccess = false;
		try {
			ftpClient = new FtpClient();
			ftpClient.openServer(ip, port);
			ftpClient.login(user, pwd);
			isSuccess = true;
		} catch (Exception ex) {
			throw new Exception("����FTP������ʧ��:" + ex.getMessage());
		}
		return isSuccess;
	}
	/*****
	 * �����ļ�����������ļ�
	 * @param dir
	 * @throws Exception
	 *	File root = new File("c:");
	 *	showAllFiles(root);
	 */
	@SuppressWarnings("unchecked")
	public List showAllFiles(String filepath) {
		List filelist=new ArrayList();
		File dir=new File(filepath);
		System.out.println(filepath);
		try {
			File[] fs = dir.listFiles();
			if(fs.length==0){
				log.error("Ŀ¼��û���ļ�,Ŀ¼·����"+filepath);
			}else{
				for (int i = 0; i < fs.length; i++) {
					//System.out.println(fs[i].getAbsolutePath());
					filelist.add(fs[i].getAbsolutePath().toString());
					//System.out.println(fs[i].getName());
				}
			}
		} catch (Exception e) {
			log.error("�������·������ȷ!",e);
		}
		return filelist;
	}
	/***
	 * չʾ�ļ����е��ļ���ϸ
	 * @param li
	 */
	@SuppressWarnings("unchecked")
	public void showFileName(String filePath,List li){
		if(li.size()>0){
			System.out.println("�ļ���·��Ϊ:"+filePath+"��,��"+li.size()+"���ļ�");
			for(int i=0;i<li.size();i++){
				System.out.println(li.get(i).toString());
			}
		}else{
			System.out.println("�ļ�����û���ļ�,��˲�·��");
		}
	}
	/**
	 * �����ļ�
	 * 
	 * @param remotePath
	 * @param localPath
	 * @param filename
	 * @throws Exception
	 */
	public static void downloadFile(String ip, int port, String user,
			String pwd, String remotePath, String localPath, String filename)
			throws Exception {
		try {
			if (connectServer(ip, port, user, pwd)) {
				// System.out.println("�����ļ���" remotePath filename "��ʼ....");
				if (remotePath.length() != 0)
					ftpClient.cd(remotePath);
				ftpClient.binary();
				TelnetInputStream is = ftpClient.get(filename);
				File file_out = new File(localPath + File.separator + filename);
				FileOutputStream os = new FileOutputStream(file_out);
				byte[] bytes = new byte[1024];
				int c;
				while ((c = is.read(bytes)) != -1) {
					os.write(bytes, 0, c);
				}
				// System.out.println("�����ļ�:" remotePath filename "�����....");
				is.close();
				os.close();
				ftpClient.closeServer();
			}
		} catch (Exception ex) {
			throw new Exception("�����ļ�ʧ�ܣ�" + ex.getMessage());
		}
	}
}
