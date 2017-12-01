



@Entity
@Table(name = "processJobParameters")

@org.hibernate.annotations.GenericGenerator( name="hibernate-native", strategy="native")
/*
@org.hibernate.annotations.GenericGenerator( name="sequence-style-generator",
	strategy="org.hibernate.id.enhanced.SequenceStyleGenerator",
	parameters = {
		@Parameter(name = "optimizer", value = "pooled"),
		@Parameter(name = "increment_size", value = "1")}
) 
*/

public class ProcessJobParameter {
	
	
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
	private String p_key;
	private String p_value;
	
	
	
	public long getJobUid() {
		return jobUid;
	}
	public void setJobUid(long jobUid) {
		this.jobUid = jobUid;
	}
	public long getUid() {
		return uid;
	}
	public void setUid(long uid) {
		this.uid = uid;
	}
	public String getP_key() {
		return p_key;
	}
	public void setP_key(String p_key) {
		this.p_key = p_key;
	}
	public String getP_value() {
		return p_value;
	}
	public void setP_value(String p_value) {
		this.p_value = p_value;
	}
	
	
	

}
