




@Entity
@Table(name = "processJobStep")

@org.hibernate.annotations.GenericGenerator( name="hibernate-native", strategy="native")
/*
@org.hibernate.annotations.GenericGenerator( name="sequence-style-generator",
	strategy="org.hibernate.id.enhanced.SequenceStyleGenerator",
	parameters = {
		@Parameter(name = "optimizer", value = "pooled"),
		@Parameter(name = "increment_size", value = "1")}
) 
*/


public class ProcessJobStep {
	
	
	// default constructor for Hibernate
	public ProcessJobStep () {}
	
	public ProcessJobStep (long jobUid, int sequence, String stepName, String description) {
		this.jobUid = jobUid;
		this.sequence = sequence;
		this.stepName = stepName;
		this.description = description;
	}
	
	
	@Id
	@GeneratedValue(generator="hibernate-native")
	//@GeneratedValue(generator = "sequence-style-generator")
	@Basic(optional = false)
	@Column(name="uid")
	private long uid;
	/*
	@Id @GeneratedValue
	@Column(name = "uid")
	private long uid;
	*/
	
	private long jobUid;
	private int sequence;
	private String stepName;
	private String description;
	private String status;
	private Date updated;
	@Type(type = "blob")
	private Object report;
	private Date stepStart;
	private Date stepEnd;
	private Long targetCount;
	private String commStatus;
	
	
	public long getUid() {
		return uid;
	}
	public void setUid(long uid) {
		this.uid = uid;
	}
	public long getJobUid() {
		return jobUid;
	}
	public void setJobUid(long jobUid) {
		this.jobUid = jobUid;
	}
	public int getSequence() {
		return sequence;
	}
	public void setSequence(int sequence) {
		this.sequence = sequence;
	}
	public String getStepName() {
		return stepName;
	}
	public void setStepName(String stepName) {
		this.stepName = stepName;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public Date getUpdated() {
		return updated;
	}

	public void setUpdated(Date updated) {
		this.updated = updated;
	}

	public Object getReport() {
		return report;
	}
	public void setReport(Object report) {
		this.report = report;
	}
	
	
	public Date getStepStart() {
		return stepStart;
	}

	public void setStepStart(Date stepStart) {
		this.stepStart = stepStart;
	}

	public Date getStepEnd() {
		return stepEnd;
	}

	public void setStepEnd(Date stepEnd) {
		this.stepEnd = stepEnd;
	}

	public Long getTargetCount() {
		return targetCount;
	}

	public void setTargetCount(Long targetCount) {
		this.targetCount = targetCount;
	}


	public String getCommStatus() {
		return commStatus;
	}

	public void setCommStatus(String commStatus) {
		this.commStatus = commStatus;
	}


	// canonical step statuses
	public static final String STATUS_ENTERED  = "Entered";
	public static final String STATUS_READY    = "Ready";
	public static final String STATUS_RETRY    = "Retry";
	public static final String STATUS_CLEANING = "Cleaning";
	public static final String STATUS_ACTIVE   = "Active";
	public static final String STATUS_STOPPED  = "Stopped";
	public static final String STATUS_CANCEL   = "Cancelled";
	public static final String STATUS_FAILED   = "Failed";
	public static final String STATUS_COMPLETE = "Complete";
	

	// canonical step names
	public static final String STEP_SETUP  = "setup";
	public static final String STEP_ANALYZE  = "analyze";
	public static final String STEP_INPUTSTORE  = "inputstore";
	public static final String STEP_SELECTMAP  = "selectmap";
	public static final String STEP_SCHEMACREATE  = "schemacreate";
	public static final String STEP_MAP  = "map";
	public static final String STEP_POSTMAP  = "postmap";
	public static final String STEP_POSTMAP_REPORT  = "postmapreport";
	public static final String STEP_NORMALIZATION  = "normalization";
	public static final String STEP_POSTNORMALIZATION = "postnormalization";
	public static final String STEP_POSTNORMALIZATION_REPORT  = "postnormalizationreport";
	//public static final String STEP_VALIDATE  = "validate";
	public static final String STEP_CONSTRUCT  = "construct";
	public static final String STEP_CONSTRUCTION_REPORT  = "postconstructionreport";
	public static final String STEP_EPISODE_DEDUPE = "epidedupe";
	public static final String STEP_PROVIDER_ATTRIBUTION = "providerattribution";
	public static final String STEP_COST_ROLLUPS  = "costrollups";
	public static final String STEP_FILTERED_ROLLUPS  = "filteredrollups";
	public static final String STEP_MASTER_UNFILTERED_RA_SA  = "masterunfiltered_ra_sa";
	public static final String STEP_RA_SA_MODEL  = "rasamodel";
	public static final String STEP_RED  = "red";
	public static final String STEP_RES  = "res";
	public static final String STEP_SAVINGS_SUMMARY = "savingsummary";
	public static final String STEP_CORE_SERVICES = "coreservices";
	public static final String STEP_PAC_ANALYSIS = "pacanalysis";
	public static final String STEP_PAS_ANALYSIS = "pasanalysis";
	public static final String STEP_MATERNITY = "maternity";

	public static final String STEP_ARCHIVE  = "archive";
	public static final String STEP_CLEANOUT  = "cleanout";
	

}
