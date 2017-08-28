



@Entity
@Table(name="`group`", indexes = {
        @Index(columnList = "ID", name = "PRIMARY")
		}
)




public class GroupDefinition {
	
	
	@Id
	private String ID; 
	private String NAME;
	private String DESCRIPTION;
	private String MDC_CATEGORY;
	private String TYPE_ID;
	private Integer INDEX;
	
	
	
	public String getID() {
		return ID;
	}
	public void setID(String iD) {
		ID = iD;
	}
	public String getNAME() {
		return NAME;
	}
	public void setNAME(String nAME) {
		NAME = nAME;
	}
	public String getDESCRIPTION() {
		return DESCRIPTION;
	}
	public void setDESCRIPTION(String dESCRIPTION) {
		DESCRIPTION = dESCRIPTION;
	}
	public String getMDC_CATEGORY() {
		return MDC_CATEGORY;
	}
	public void setMDC_CATEGORY(String mDC_CATEGORY) {
		MDC_CATEGORY = mDC_CATEGORY;
	}
	public String getTYPE_ID() {
		return TYPE_ID;
	}
	public void setTYPE_ID(String tYPE_ID) {
		TYPE_ID = tYPE_ID;
	}
	public Integer getINDEX() {
		return INDEX;
	}
	public void setINDEX(Integer iNDEX) {
		INDEX = iNDEX;
	}
	
	
	

}
