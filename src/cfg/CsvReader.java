package cfg;

import java.io.BufferedReader;  
import java.io.FileInputStream;  
import java.io.IOException;  
import java.io.InputStreamReader;  
import java.util.ArrayList;  
import java.util.List;  
import java.util.regex.Matcher;  
import java.util.regex.Pattern;  
  
  
public class CsvReader {  
  
    private InputStreamReader fr = null;  
    private BufferedReader br = null;  
  
    public CsvReader(String f) throws IOException {  
        fr = new InputStreamReader(new FileInputStream(f));  
    }  
  
    /** 
     * ����csv�ļ� ��һ��list�� ÿ����Ԫ��Ϊһ��String���ͼ�¼��ÿһ��Ϊһ��list�� �ٽ����е��зŵ�һ����list�� 
     */  
    public static List<List<String>> readCSVFile(String filename) throws IOException {
    	InputStreamReader fr = new InputStreamReader(new FileInputStream(filename));  
    	BufferedReader br = new BufferedReader(fr);  
        String rec = null;// һ��  
        String str;// һ����Ԫ��  
        List<List<String>> listFile = new ArrayList<List<String>>();  
        try {  
            // ��ȡһ��  
            while ((rec = br.readLine()) != null) {  
                Pattern pCells = Pattern  
                        .compile("(\"[^\"]*(\"{2})*[^\"]*\")*[^,]*,");  
                Matcher mCells = pCells.matcher(rec);  
                List<String> cells = new ArrayList<String>();// ÿ�м�¼һ��list  
                // ��ȡÿ����Ԫ��  
                while (mCells.find()) {  
                    str = mCells.group();  
                    str = str.replaceAll(  
                            "(?sm)\"?([^\"]*(\"{2})*[^\"]*)\"?.*,", "$1");  
                    str = str.replaceAll("(?sm)(\"(\"))", "$2");  
                    cells.add(str);  
                }  
                listFile.add(cells);  
            }  
        } catch (Exception e) {  
            e.printStackTrace();  
        } finally {  
            if (fr != null) {  
                fr.close();  
            }  
            if (br != null) {  
                br.close();  
            }  
        }  
        return listFile;  
    }  
  
    public static void main(String[] args) throws Throwable {  
    	CsvReader test = new CsvReader("D:/test.csv");  
        List<List<String>> csvList = test.readCSVFile("D:/test.csv");          
    }  
  
}  
