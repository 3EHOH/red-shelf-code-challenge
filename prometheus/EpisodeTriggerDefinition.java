




@Entity
@Table(name="episode_trigger", indexes = {
        @Index(columnList = "EPISODE_ID", name = "EPISODE_ID")
		}
)



public class EpisodeTriggerDefinition implements Serializable {
	
	@Transient
	transient private static final long serialVersionUID = 1L;
	
	
	@Id
	private String EPISODE_ID;
	@Id
	private String FACILITY_TYPE; 
	private String PX_CODE_POSITION; 
	private String SERVICE_TYPE; 
	private String LOGICAL_OPERATOR; 
	private String DX_CODE_POSITION; 
	private String DUAL_SERVICE_TYPE; 
	private Integer DUAL_SERVICE_MIN; 
	private Integer DUAL_SERVICE_MAX; 
	private Integer END_OF_STUDY; 
	private Integer REQ_CONF_CLAIM;
	
	
	public String getEPISODE_ID() {
		return EPISODE_ID;
	}
	public void setEPISODE_ID(String ePISODE_ID) {
		EPISODE_ID = ePISODE_ID;
	}
	public String getFACILITY_TYPE() {
		return FACILITY_TYPE;
	}
	public void setFACILITY_TYPE(String fACILITY_TYPE) {
		FACILITY_TYPE = fACILITY_TYPE;
	}
	public String getPX_CODE_POSITION() {
		return PX_CODE_POSITION;
	}
	public void setPX_CODE_POSITION(String pX_CODE_POSITION) {
		PX_CODE_POSITION = pX_CODE_POSITION;
	}
	public String getSERVICE_TYPE() {
		return SERVICE_TYPE;
	}
	public void setSERVICE_TYPE(String sERVICE_TYPE) {
		SERVICE_TYPE = sERVICE_TYPE;
	}
	public String getLOGICAL_OPERATOR() {
		return LOGICAL_OPERATOR;
	}
	public void setLOGICAL_OPERATOR(String lOGICAL_OPERATOR) {
		LOGICAL_OPERATOR = lOGICAL_OPERATOR;
	}
	public String getDX_CODE_POSITION() {
		return DX_CODE_POSITION;
	}
	public void setDX_CODE_POSITION(String dX_CODE_POSITION) {
		DX_CODE_POSITION = dX_CODE_POSITION;
	}
	public String getDUAL_SERVICE_TYPE() {
		return DUAL_SERVICE_TYPE;
	}
	public void setDUAL_SERVICE_TYPE(String dUAL_SERVICE_TYPE) {
		DUAL_SERVICE_TYPE = dUAL_SERVICE_TYPE;
	}
	public Integer getDUAL_SERVICE_MIN() {
		return DUAL_SERVICE_MIN;
	}
	public void setDUAL_SERVICE_MIN(Integer dUAL_SERVICE_MIN) {
		DUAL_SERVICE_MIN = dUAL_SERVICE_MIN;
	}
	public Integer getDUAL_SERVICE_MAX() {
		return DUAL_SERVICE_MAX;
	}
	public void setDUAL_SERVICE_MAX(Integer dUAL_SERVICE_MAX) {
		DUAL_SERVICE_MAX = dUAL_SERVICE_MAX;
	}
	public Integer getEND_OF_STUDY() {
		return END_OF_STUDY;
	}
	public void setEND_OF_STUDY(Integer eND_OF_STUDY) {
		END_OF_STUDY = eND_OF_STUDY;
	}
	public Integer getREQ_CONF_CLAIM() {
		return REQ_CONF_CLAIM;
	}
	public void setREQ_CONF_CLAIM(Integer rEQ_CONF_CLAIM) {
		REQ_CONF_CLAIM = rEQ_CONF_CLAIM;
	}
	
	

}
