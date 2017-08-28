






@Entity
@Table(name="jobStepQueue", indexes = {
        @Index(columnList = "jobUid", name = "user_jobuid_hidx")
		}
)
@DynamicUpdate

@org.hibernate.annotations.GenericGenerator( name="hibernate-native", strategy="native")
/*
@org.hibernate.annotations.GenericGenerator( name="sequence-style-generator",
	strategy="org.hibernate.id.enhanced.SequenceStyleGenerator",
	parameters = {
		@Parameter(name = "optimizer", value = "pooled"),
		@Parameter(name = "increment_size", value = "1")}
) 
*/


public class JobStepQueue {
	
	
	// default constructor for Hibernate
	public JobStepQueue () {}
	
	public JobStepQueue (String member_id) {
		this.member_id = member_id;
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
	private String stepName;
	private String member_id;
	private String status;
	private Date updated;
	@Type(type = "blob")
	private Object report;
	
	
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
	public String getStepName() {
		return stepName;
	}
	public void setStepName(String stepName) {
		this.stepName = stepName;
	}
	public String getMember_id() {
		return member_id;
	}
	public void setMember_id(String member_id) {
		this.member_id = member_id;
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
	
	
	public final static String STATUS_READY = "Ready";
	public final static String STATUS_ACTIVE = "Active";
	public final static String STATUS_INVALID = "Invalid";
	public final static String STATUS_FAILED = "Failed";
	public final static String STATUS_COMPLETE = "Complete";
	

}
