



@Entity
@Table(name="code_icd10", indexes = {
        @Index(columnList = "VALUE,TYPE_ID,MULTUM_CATEGORY", name = "PRIMARY")
		}
)



public class Code10Definition implements Serializable {
	
	@Transient
	transient private static final long serialVersionUID = 1L;
	
	
	@Id
	private String VALUE;
	@Id
	private String TYPE_ID;
	@Id
	private String MULTUM_CATEGORY;
	private String DESCRIPTION;
	private String DESCRIPTION_LONG;
	private String GROUP_ID;
	private String BETOS_CATEGORY;
	private String MDC_CATEGORY;
	private String CCS_CATEGORY;
	private String CCS_TYPE_ID;
	private String CC_CATEGORY;
	private Date CREATED_DATE;
	private Date MODIFIED_DATE;
	private Double COST;
	private Double FREQUENCY;
	private String EM;
	
	
	public String getVALUE() {
		return VALUE;
	}
	public void setVALUE(String vALUE) {
		VALUE = vALUE;
	}
	public String getTYPE_ID() {
		return TYPE_ID;
	}
	public void setTYPE_ID(String tYPE_ID) {
		TYPE_ID = tYPE_ID;
	}
	public String getMULTUM_CATEGORY() {
		return MULTUM_CATEGORY;
	}
	public void setMULTUM_CATEGORY(String mULTUM_CATEGORY) {
		MULTUM_CATEGORY = mULTUM_CATEGORY;
	}
	public String getDESCRIPTION() {
		return DESCRIPTION;
	}
	public void setDESCRIPTION(String dESCRIPTION) {
		DESCRIPTION = dESCRIPTION;
	}
	public String getDESCRIPTION_LONG() {
		return DESCRIPTION_LONG;
	}
	public void setDESCRIPTION_LONG(String dESCRIPTION_LONG) {
		DESCRIPTION_LONG = dESCRIPTION_LONG;
	}
	public String getGROUP_ID() {
		return GROUP_ID;
	}
	public void setGROUP_ID(String gROUP_ID) {
		GROUP_ID = gROUP_ID;
	}
	public String getBETOS_CATEGORY() {
		return BETOS_CATEGORY;
	}
	public void setBETOS_CATEGORY(String bETOS_CATEGORY) {
		BETOS_CATEGORY = bETOS_CATEGORY;
	}
	public String getMDC_CATEGORY() {
		return MDC_CATEGORY;
	}
	public void setMDC_CATEGORY(String mDC_CATEGORY) {
		MDC_CATEGORY = mDC_CATEGORY;
	}
	public String getCCS_CATEGORY() {
		return CCS_CATEGORY;
	}
	public void setCCS_CATEGORY(String cCS_CATEGORY) {
		CCS_CATEGORY = cCS_CATEGORY;
	}
	public String getCCS_TYPE_ID() {
		return CCS_TYPE_ID;
	}
	public void setCCS_TYPE_ID(String cCS_TYPE_ID) {
		CCS_TYPE_ID = cCS_TYPE_ID;
	}
	public String getCC_CATEGORY() {
		return CC_CATEGORY;
	}
	public void setCC_CATEGORY(String cC_CATEGORY) {
		CC_CATEGORY = cC_CATEGORY;
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
	public Double getCOST() {
		return COST;
	}
	public void setCOST(Double cOST) {
		COST = cOST;
	}
	public Double getFREQUENCY() {
		return FREQUENCY;
	}
	public void setFREQUENCY(Double fREQUENCY) {
		FREQUENCY = fREQUENCY;
	}
	public String getEM() {
		return EM;
	}
	public void setEM(String eM) {
		EM = eM;
	}
	public static long getSerialversionuid() {
		return serialVersionUID;
	}
	
	
	
}
