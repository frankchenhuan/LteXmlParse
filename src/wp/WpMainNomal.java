package wp;

import java.io.IOException;

import javax.xml.stream.XMLStreamException;

import cfg.Config;

public class WpMainNomal {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String conf_file = args[0];
		String filepath = args[1];
		String outpath=args[2];
		Config.init(conf_file);
		WpParseNomal wpn = new WpParseNomal();
		try {
			wpn.parsePathAllXml(filepath,outpath);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}

	}

}
