


@Entity
@Table(name="episode_association", indexes = {
        @Index(columnList = "ID", name = "PRIMARY"),
        @Index(columnList = "PRIMARY_EPISODE_ID", name = "fk_EPISODE_ASSOCIATION_EPISODE1"),
        @Index(columnList = "SECONDARY_EPISODE_ID", name = "fk_EPISODE_ASSOCIATION_EPISODE2"),
		}
)


public class AssociationDefinition {
	
	
	
	@Id
	private String ID; 
	private String ASSOCIATION; 
	private String LEVEL; 
	private String PRIMARY_EPISODE_ID; 
	private String SECONDARY_EPISODE_ID; 
	private String START_DAY; 
	private String END_DAY; 
	private String SUBSIDIARY_TO_PROC;
	
	
	public String getID() {
		return ID;
	}
	public void setID(String iD) {
		ID = iD;
	}
	public String getASSOCIATION() {
		return ASSOCIATION;
	}
	public void setASSOCIATION(String aSSOCIATION) {
		ASSOCIATION = aSSOCIATION;
	}
	public String getLEVEL() {
		return LEVEL;
	}
	public void setLEVEL(String lEVEL) {
		LEVEL = lEVEL;
	}
	public String getPRIMARY_EPISODE_ID() {
		return PRIMARY_EPISODE_ID;
	}
	public void setPRIMARY_EPISODE_ID(String pRIMARY_EPISODE_ID) {
		PRIMARY_EPISODE_ID = pRIMARY_EPISODE_ID;
	}
	public String getSECONDARY_EPISODE_ID() {
		return SECONDARY_EPISODE_ID;
	}
	public void setSECONDARY_EPISODE_ID(String sECONDARY_EPISODE_ID) {
		SECONDARY_EPISODE_ID = sECONDARY_EPISODE_ID;
	}
	public String getSTART_DAY() {
		return START_DAY;
	}
	public void setSTART_DAY(String sTART_DAY) {
		START_DAY = sTART_DAY;
	}
	public String getEND_DAY() {
		return END_DAY;
	}
	public void setEND_DAY(String eND_DAY) {
		END_DAY = eND_DAY;
	}
	public String getSUBSIDIARY_TO_PROC() {
		return SUBSIDIARY_TO_PROC;
	}
	public void setSUBSIDIARY_TO_PROC(String sUBSIDIARY_TO_PROC) {
		SUBSIDIARY_TO_PROC = sUBSIDIARY_TO_PROC;
	}

	
	
	
}
