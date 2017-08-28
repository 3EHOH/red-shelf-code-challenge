

public class TriggerCodeMetaData extends EpisodeCodeBase implements Serializable {
	
	
	private static final long serialVersionUID = 1L;
	
	
	private String qualifying_diagnosis;

	public String getQualifying_diagnosis() {
		return qualifying_diagnosis;
	}
	public void setQualifying_diagnosis(String qualifying_diagnosis) {
		this.qualifying_diagnosis = qualifying_diagnosis;
	}

	public String toString() {
		return super.toString();
	}

}
