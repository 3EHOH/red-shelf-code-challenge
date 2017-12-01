




@Entity
@Table(name="run")
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

public class DataRun {
	
	@Id
	@GeneratedValue(generator="hibernate-native")
	//@GeneratedValue(generator = "sequence-style-generator")
	@Basic(optional = false)
	@Column(name="uid")
	private long uid;
	/*
	@Id @GeneratedValue
	@Column(name = "uid")
	private int uid;
	*/
	private String submitter_name;
	private Date submitted_date;
	private String run_name;
	private Date data_start_date;
	private Date data_end_date;
	private Date data_latest_begin_date;
	private long enrolled_population;
	private Date run_date;
	private Date data_approved_date;
	private String metadata_version;
	private int metadata_custom;
	private int episode_count;
	private BigDecimal episode_cost;
	private BigDecimal unassigned_cost;
	private long jobUid;
	

	public long getUid() {
		return uid;
	}
	public void setUid(long uid) {
		this.uid = uid;
	}
	public String getSubmitter_name() {
		return submitter_name;
	}
	public void setSubmitter_name(String submitter_name) {
		this.submitter_name = submitter_name;
	}
	public Date getSubmitted_date() {
		return submitted_date;
	}
	public void setSubmitted_date(Date submitted_date) {
		this.submitted_date = submitted_date;
	}
	public String getRun_name() {
		return run_name;
	}
	public void setRun_name(String run_name) {
		this.run_name = run_name;
	}
	public Date getData_start_date() {
		return data_start_date;
	}
	public void setData_start_date(Date data_start_date) {
		this.data_start_date = data_start_date;
	}
	public Date getData_end_date() {
		return data_end_date;
	}
	public void setData_end_date(Date data_end_date) {
		this.data_end_date = data_end_date;
	}
	public Date getData_latest_begin_date() {
		return data_latest_begin_date;
	}
	public void setData_latest_begin_date(Date data_latest_begin_date) {
		this.data_latest_begin_date = data_latest_begin_date;
	}
	public long getEnrolled_population() {
		return enrolled_population;
	}
	public void setEnrolled_population(long enrolled_population) {
		this.enrolled_population = enrolled_population;
	}
	public Date getRun_date() {
		return run_date;
	}
	public void setRun_date(Date run_date) {
		this.run_date = run_date;
	}
	public Date getData_approved_date() {
		return data_approved_date;
	}
	public void setData_approved_date(Date data_approved_date) {
		this.data_approved_date = data_approved_date;
	}
	public String getMetadata_version() {
		return metadata_version;
	}
	public void setMetadata_version(String metadata_version) {
		this.metadata_version = metadata_version;
	}
	public int getMetadata_custom() {
		return metadata_custom;
	}
	public void setMetadata_custom(int metadata_custom) {
		this.metadata_custom = metadata_custom;
	}
	public int getEpisode_count() {
		return episode_count;
	}
	public void setEpisode_count(int episode_count) {
		this.episode_count = episode_count;
	}
	public BigDecimal getEpisode_cost() {
		return episode_cost;
	}
	public void setEpisode_cost(BigDecimal episode_cost) {
		this.episode_cost = episode_cost;
	}
	public BigDecimal getUnassigned_cost() {
		return unassigned_cost;
	}
	public void setUnassigned_cost(BigDecimal unassigned_cost) {
		this.unassigned_cost = unassigned_cost;
	}
	public long getJobUid() {
		return jobUid;
	}
	public void setJobUid(long jobUid) {
		this.jobUid = jobUid;
	}

}
