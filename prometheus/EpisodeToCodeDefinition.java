





@Entity
@Table(name="episode_to_code", indexes = {
        @Index(columnList = "EPISODE_ID,FUNCTION_ID,CODE_VALUE,CODE_TYPE_ID,CODE_MULTUM_CATEGORY", name = "PRIMARY")
		}
)
//@AssociationOverrides({
//    @AssociationOverride(name = "primaryKey.user", 
 //       joinColumns = {@JoinColumn(name = "CODE_VALUE"),@JoinColumn(name = "CODE_TYPE_ID"),@JoinColumn(name = "CODE_MULTUM_CATEGORY")})})//,
   // @AssociationOverride(name = "primaryKey.group", 
   //     joinColumns = @JoinColumn(name = "GROUP_ID")) })






public class EpisodeToCodeDefinition implements Serializable {
	
	@Transient
	transient private static final long serialVersionUID = 1L;
	
	// composite-id key
	/*
    private CodeKey key = new CodeKey();

	@EmbeddedId
    public CodeKey getPrimaryKey() {
        return key;
    }
    */

	
	@Id
	private String EPISODE_ID;
	@Id
	private String FUNCTION_ID;
	@Id
	private String CODE_VALUE; 
	@Id
	private String CODE_TYPE_ID; 
	@Id
	private String CODE_MULTUM_CATEGORY; 
 
	private String SUBTYPE_ID; 
	private String COMPLICATION; 
	private String SUFFICIENT; 
	private String CORE; 
	private String VETTED; 
	private String PAS; 
	private String CORE_QUANTITY; 
	private String IS_POS; 
	private String QUALIFYING_DIAGNOSIS; 
	private String RX_FUNCTION;
	
	/*
	@Transient
	private Set<CodeDefinition> codes = new HashSet<CodeDefinition>();
	*/
	
	public String getEPISODE_ID() {
		return EPISODE_ID;
	}
	public void setEPISODE_ID(String ePISODE_ID) {
		EPISODE_ID = ePISODE_ID;
	}
	public String getFUNCTION_ID() {
		return FUNCTION_ID;
	}
	public void setFUNCTION_ID(String fUNCTION_ID) {
		FUNCTION_ID = fUNCTION_ID;
	}
	public String getCODE_VALUE() {
		return CODE_VALUE;
	}
	public void setCODE_VALUE(String cODE_VALUE) {
		CODE_VALUE = cODE_VALUE;
	}
	public String getCODE_TYPE_ID() {
		return CODE_TYPE_ID;
	}
	public void setCODE_TYPE_ID(String cODE_TYPE_ID) {
		CODE_TYPE_ID = cODE_TYPE_ID;
	}
	public String getCODE_MULTUM_CATEGORY() {
		return CODE_MULTUM_CATEGORY;
	}
	public void setCODE_MULTUM_CATEGORY(String cODE_MULTUM_CATEGORY) {
		CODE_MULTUM_CATEGORY = cODE_MULTUM_CATEGORY;
	}
	public String getSUBTYPE_ID() {
		return SUBTYPE_ID;
	}
	public void setSUBTYPE_ID(String sUBTYPE_ID) {
		SUBTYPE_ID = sUBTYPE_ID;
	}
	public String getCOMPLICATION() {
		return COMPLICATION;
	}
	public void setCOMPLICATION(String cOMPLICATION) {
		COMPLICATION = cOMPLICATION;
	}
	public String getSUFFICIENT() {
		return SUFFICIENT;
	}
	public void setSUFFICIENT(String sUFFICIENT) {
		SUFFICIENT = sUFFICIENT;
	}
	public String getCORE() {
		return CORE;
	}
	public void setCORE(String cORE) {
		CORE = cORE;
	}
	public String getVETTED() {
		return VETTED;
	}
	public void setVETTED(String vETTED) {
		VETTED = vETTED;
	}
	public String getPAS() {
		return PAS;
	}
	public void setPAS(String pAS) {
		PAS = pAS;
	}
	public String getCORE_QUANTITY() {
		return CORE_QUANTITY;
	}
	public void setCORE_QUANTITY(String cORE_QUANTITY) {
		CORE_QUANTITY = cORE_QUANTITY;
	}
	public String getIS_POS() {
		return IS_POS;
	}
	public void setIS_POS(String iS_POS) {
		IS_POS = iS_POS;
	}
	public String getQUALIFYING_DIAGNOSIS() {
		return QUALIFYING_DIAGNOSIS;
	}
	public void setQUALIFYING_DIAGNOSIS(String qUALIFYING_DIAGNOSIS) {
		QUALIFYING_DIAGNOSIS = qUALIFYING_DIAGNOSIS;
	}
	public String getRX_FUNCTION() {
		return RX_FUNCTION;
	}
	public void setRX_FUNCTION(String rX_FUNCTION) {
		RX_FUNCTION = rX_FUNCTION;
	}
	public static long getSerialversionuid() {
		return serialVersionUID;
	}
	
	//@OneToMany(mappedBy = "primaryKey.user", 
   //         cascade = CascadeType.ALL)

	//@OneToMany(cascade=CascadeType.ALL, mappedBy="employer")
	/*
	@OneToMany(fetch = FetchType.EAGER, mappedBy="episodetocodedefinition")
    @JoinTable(
            name="`code`",
            		joinColumns = {@JoinColumn(name = "CODE_VALUE", referencedColumnName="VALUE"),
            		@JoinColumn(name = "CODE_TYPE_ID", referencedColumnName="TYPE_ID"),
            		@JoinColumn(name = "CODE_MULTUM_CATEGORY", referencedColumnName="MULTUM_CATEGORY")} //,
            //inverseJoinColumns = @JoinColumn( name="PART_ID")
    )
	public Set<CodeDefinition> getCodes() {
		return codes;
	}
	public void setCodes(Set<CodeDefinition> codes) {
		this.codes = codes;
	}
	public void addCode(CodeDefinition code) {
		this.codes.add(code);
	}
	*/
	

}
