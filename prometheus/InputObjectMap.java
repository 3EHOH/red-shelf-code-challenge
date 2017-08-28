

public class InputObjectMap {
	
	String objectName;
	ConcurrentHashMap<String, InputMapEntry> fieldMappings = new ConcurrentHashMap<String, InputMapEntry>();
	ArrayList<Condition> exclusions = new ArrayList<Condition>();
	
	
	public String getObjectName() {
		return objectName;
	}
	public void setObjectName(String objectName) {
		this.objectName = objectName;
	}
	public ConcurrentHashMap<String, InputMapEntry> getFieldMappings() {
		return fieldMappings;
	}
	public void setFieldMappings(ConcurrentHashMap<String, InputMapEntry> fieldMappings) {
		this.fieldMappings = fieldMappings;
	}
	
	public void addExclusion (Condition c) {
		exclusions.add(c);
	}
	public ArrayList<Condition> getExclusions () {
		return exclusions;
	}
	

}
