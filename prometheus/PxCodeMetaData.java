

public class PxCodeMetaData extends EpisodeCodeBase implements Serializable {
	

	private static final long serialVersionUID = -9037681850609267887L;
	
	
	private boolean core;
	private boolean sufficient;
	private boolean pas;
	private String betos_category;
	
	public boolean isCore() {
		return core;
	}
	public void setCore(boolean core) {
		this.core = core;
	}
	public boolean isSufficient() {
		return sufficient;
	}
	public void setSufficient(boolean sufficient) {
		this.sufficient = sufficient;
	}
	public boolean isPas() {
		return pas;
	}
	public void setPas(boolean pas) {
		this.pas = pas;
	}
	public String getBetos_category() {
		return betos_category;
	}
	public void setBetos_category(String betos_category) {
		this.betos_category = betos_category;
	}

}
