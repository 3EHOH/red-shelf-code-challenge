



@Entity
@Table(name = "processReport")

@org.hibernate.annotations.GenericGenerator( name="hibernate-native", strategy="native")
/*
@org.hibernate.annotations.GenericGenerator( name="sequence-style-generator",
	strategy="org.hibernate.id.enhanced.SequenceStyleGenerator",
	parameters = {
		@Parameter(name = "optimizer", value = "pooled"),
		@Parameter(name = "increment_size", value = "1")}
) 
*/

public class ProcessReport {
	
	
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
	private String reportName;
	@Type(type = "blob")
	private Blob report;
	
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
	
	public String getReportName() {
		return reportName;
	}
	public void setReportName(String reportName) {
		this.reportName = reportName;
	}

	public Blob getReport() {
		return report;
	}
	public void setReport(Blob report) {
		this.report = report;
	}
	

}
