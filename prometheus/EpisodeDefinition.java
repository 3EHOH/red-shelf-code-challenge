



@Entity
@Table(name="episode", indexes = {
        @Index(columnList = "EPISODE_ID", name = "ID_UNIQUE")
		}
)


public class EpisodeDefinition {
	
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Basic(optional = false)
	@Column(name="EPISODE_ID")
	private String EPISODE_ID;
	
	private String NAME; 
	private String TYPE;
	private String STATUS;
	private String DESCRIPTION;
	private Date CREATED_DATE;
	private Date MODIFIED_DATE;
	private String USERS_USER_ID;
	private String MDC_CATEGORY;
	private Integer PARM_SET;
	private Integer TRIGGER_TYPE;
	private Integer TRIGGER_NUMBER;
	private Integer SEPARATION_MIN;
	private Integer SEPARATION_MAX;
	private Integer BOUND_OFFSET;
	private Integer BOUND_LENGTH;
	private Integer CONDITION_MIN;
	private String VERSION;
	private Integer END_OF_STUDY;
	
	
	public String getEPISODE_ID() {
		return EPISODE_ID;
	}
	public void setEPISODE_ID(String ePISODE_ID) {
		EPISODE_ID = ePISODE_ID;
	}
	public String getNAME() {
		return NAME;
	}
	public void setNAME(String nAME) {
		NAME = nAME;
	}
	public String getTYPE() {
		return TYPE;
	}
	public void setTYPE(String tYPE) {
		TYPE = tYPE;
	}
	public String getSTATUS() {
		return STATUS;
	}
	public void setSTATUS(String sTATUS) {
		STATUS = sTATUS;
	}
	public String getDESCRIPTION() {
		return DESCRIPTION;
	}
	public void setDESCRIPTION(String dESCRIPTION) {
		DESCRIPTION = dESCRIPTION;
	}
	public Date getCREATED_DATE() {
		return CREATED_DATE;
	}
	public void setCREATED_DATE(Date cREATED_DATE) {
		CREATED_DATE = cREATED_DATE;
	}
	public Date getMODIFIED_DATE() {
		return MODIFIED_DATE;
	}
	public void setMODIFIED_DATE(Date mODIFIED_DATE) {
		MODIFIED_DATE = mODIFIED_DATE;
	}
	public String getUSERS_USER_ID() {
		return USERS_USER_ID;
	}
	public void setUSERS_USER_ID(String uSERS_USER_ID) {
		USERS_USER_ID = uSERS_USER_ID;
	}
	public String getMDC_CATEGORY() {
		return MDC_CATEGORY;
	}
	public void setMDC_CATEGORY(String mDC_CATEGORY) {
		MDC_CATEGORY = mDC_CATEGORY;
	}
	public Integer getPARM_SET() {
		return PARM_SET;
	}
	public void setPARM_SET(Integer pARM_SET) {
		PARM_SET = pARM_SET;
	}
	public Integer getTRIGGER_TYPE() {
		return TRIGGER_TYPE;
	}
	public void setTRIGGER_TYPE(Integer tRIGGER_TYPE) {
		TRIGGER_TYPE = tRIGGER_TYPE;
	}
	public Integer getTRIGGER_NUMBER() {
		return TRIGGER_NUMBER;
	}
	public void setTRIGGER_NUMBER(Integer tRIGGER_NUMBER) {
		TRIGGER_NUMBER = tRIGGER_NUMBER;
	}
	public Integer getSEPARATION_MIN() {
		return SEPARATION_MIN;
	}
	public void setSEPARATION_MIN(Integer sEPARATION_MIN) {
		SEPARATION_MIN = sEPARATION_MIN;
	}
	public Integer getSEPARATION_MAX() {
		return SEPARATION_MAX;
	}
	public void setSEPARATION_MAX(Integer sEPARATION_MAX) {
		SEPARATION_MAX = sEPARATION_MAX;
	}
	public Integer getBOUND_OFFSET() {
		return BOUND_OFFSET;
	}
	public void setBOUND_OFFSET(Integer bOUND_OFFSET) {
		BOUND_OFFSET = bOUND_OFFSET;
	}
	public Integer getBOUND_LENGTH() {
		return BOUND_LENGTH;
	}
	public void setBOUND_LENGTH(Integer bOUND_LENGTH) {
		BOUND_LENGTH = bOUND_LENGTH;
	}
	public Integer getCONDITION_MIN() {
		return CONDITION_MIN;
	}
	public void setCONDITION_MIN(Integer cONDITION_MIN) {
		CONDITION_MIN = cONDITION_MIN;
	}
	public String getVERSION() {
		return VERSION;
	}
	public void setVERSION(String vERSION) {
		VERSION = vERSION;
	}
	public Integer getEND_OF_STUDY() {
		return END_OF_STUDY;
	}
	public void setEND_OF_STUDY(Integer eND_OF_STUDY) {
		END_OF_STUDY = eND_OF_STUDY;
	}
	
	
	

}
