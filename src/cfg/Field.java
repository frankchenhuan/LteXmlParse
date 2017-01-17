package cfg;

public class Field {
	private String name;
	private int index;
	private String type;
	private int flag;
	private String format;
	private String tablecolumn;
	private int length;
	private String regex;
	private String split;
	
	/**
	 * ºöÂÔ´óÐ¡Ð´
	 * */
	private boolean ignoreCase=true;
	
	public String getSplit() {
		return split;
	}
	public void setSplit(String split) {
		this.split = split;
	}

	public String getRegex() {
		return regex;
	}
	public void setRegex(String regex) {
		this.regex = regex;
	}
	public int getLength() {
		return length;
	}
	public void setLength(int length) {
		this.length = length;
	}
	public String getTablecolumn() {
		return tablecolumn;
	}
	public void setTablecolumn(String tablecolumn) {
		this.tablecolumn = tablecolumn;
	}
	public String getFormat() {
		return format;
	}
	public void setFormat(String format) {
		this.format = format;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}

	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public int getFlag() {
		return flag;
	}
	public void setFlag(int flag) {
		this.flag = flag;
	}
	public int getIndex() {
		return index;
	}
	public void setIndex(int index) {
		this.index = index;
	}
	public boolean isIgnoreCase() {
		return ignoreCase;
	}
	public void setIgnoreCase(boolean ignoreCase) {
		this.ignoreCase = ignoreCase;
	}
}
