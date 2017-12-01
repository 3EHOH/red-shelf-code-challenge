




public class MappingRequest {

	
	private int sequence;
	private String jobUid;
	private String status;
	private String objectName;
	private String mapName;
	private String collectionName;
	private String fileName;
	private ArrayList<String> input;
	
	
	public int getSequence() {
		return sequence;
	}
	public void setSequence(int sequence) {
		this.sequence = sequence;
	}
	public String getJobUid() {
		return jobUid;
	}
	public void setJobUid(String jobUid) {
		this.jobUid = jobUid;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public String getObjectName() {
		return objectName;
	}
	public void setObjectName(String objectName) {
		this.objectName = objectName;
	}
	public String getMapName() {
		return mapName;
	}
	public void setMapName(String mapName) {
		this.mapName = mapName;
	}
	public String getCollectionName() {
		return collectionName;
	}
	public void setCollectionName(String collectionName) {
		this.collectionName = collectionName;
	}
	public String getFileName() {
		return fileName;
	}
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	public ArrayList<String> getInput() {
		return input;
	}
	public void setInput(ArrayList<String> input) {
		this.input = input;
	}
	public void addToInput (String s) {
		if ( input == null )
			input = new ArrayList<String>();
		input.add(s);
	}
	
	
	
	public static final String FR_STATUS_FILEEND = "<fe<";
	public static final String FR_STATUS_CONTINUE = "<ok<";
	
	
	
	@Override
    public boolean equals(Object o) {
		
		boolean bR = true;
        
		if (  !  (o instanceof MappingRequest) ) 
			bR = false;
		else {
			MappingRequest _o = (MappingRequest) o;
			bR = bR && StringCheck(_o.getJobUid(), this.getJobUid());
			bR = bR && StringCheck(_o.getObjectName(), this.getObjectName());
			bR = bR && StringCheck(_o.getMapName(), this.getMapName());
			bR = bR && StringCheck(_o.getCollectionName(), this.getCollectionName());
			bR = bR && StringCheck(_o.getFileName(), this.getFileName());
		}
		
		return bR;
        
    }
	
	private boolean StringCheck (String s1, String s2) {
		if (s1 == null  && s2 == null)
			return true;
		if (s1 == null  && s2 != null)
			return false;
		if (s1 != null && s2 == null)
			return false;
		return s1.equals(s2);
	}

	
}
