package wp;

import java.util.ArrayList;
import java.util.List;

public class WpTargetName {
	public WpTargetName(String targetName,String typeName,String clmName) {
		this.targetName=targetName;
		this.typeName=typeName;
		this.clmName=clmName;
		
	}

	String targetName;
	String typeName;
	String clmName;
	List<String> otherObjs=new ArrayList<String>();
	public String getClmName() {
		return clmName;
	}
	public void setClmName(String clmName) {
		this.clmName = clmName;
	}
	public String getTargetName() {
		return targetName;
	}
	public void setTargetName(String targetName) {
		this.targetName = targetName;
	}
	public String getTypeName() {
		return typeName;
	}
	public void setTypeName(String typeName) {
		this.typeName = typeName;
	}
	
	public void addOtherObj(String s)
	{
		this.otherObjs.add(s);
	}
	public List<String> getOtherObjs() {
		return otherObjs;
	}
	
}
