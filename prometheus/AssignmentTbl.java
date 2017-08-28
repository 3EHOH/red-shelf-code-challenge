


@Entity
@Table(name = "assignment")

@org.hibernate.annotations.GenericGenerator( name="hibernate-native", strategy="native")
/*
@org.hibernate.annotations.GenericGenerator( name="sequence-style-generator",
	strategy="org.hibernate.id.enhanced.SequenceStyleGenerator",
	parameters = {
		@Parameter(name = "optimizer", value = "pooled"),
		@Parameter(name = "increment_size", value = "1")}
) 
*/

public class AssignmentTbl {
	
	@Id
	@GeneratedValue(generator="hibernate-native")
	//@GeneratedValue(generator = "sequence-style-generator")
	@Basic(optional = false)
	@Column(name="id")
	private long id;
	
	@Column(name = "member_id")
	private String member_id;
	
	@Column(name = "master_claim_id")
	private String master_claim_id;
	
	@Column(name = "master_episode_id")
	private String master_episode_id;
	
	@Column(name = "claim_source")
	private String claim_source;
	
	@Column(name = "assigned_type")
	private String assigned_type;
	
	@Column(name = "assigned_count")
	private int assigned_count;
	
	@Column(name = "rule")
	private String rule;
	
	@Column(name = "pac")
	private int pac;
	
	@Column(name = "pac_type")
	private String pac_type;
	
	@Column(name = "episode_period")
	private int episode_period;
	
	@Column(name = "IP_period")
	private int IP_period;
	
	
	public Long getId() {
		return id;
	}
	public void setId(Long id) {
		this.id = id;
	}
	
	public String getMember_id() {
		return member_id;
	}
	public void setMember_id(String member_id) {
		this.member_id = member_id;
	}
	public String getMaster_claim_id() {
		return master_claim_id;
	}
	public void setMaster_claim_id(String master_claim_id) {
		this.master_claim_id = master_claim_id;
	}
	public String getMaster_episode_id() {
		return master_episode_id;
	}
	public void setMaster_episode_id(String master_episode_id) {
		this.master_episode_id = master_episode_id;
	}
	public String getClaim_source() {
		return claim_source;
	}
	public void setClaim_source(String claim_source) {
		this.claim_source = claim_source;
	}
	public String getAssigned_type() {
		return assigned_type;
	}
	public void setAssigned_type(String assigned_type) {
		this.assigned_type = assigned_type;
	}
	public int getAssigned_count() {
		return assigned_count;
	}
	public void setAssigned_count(int assigned_count) {
		this.assigned_count = assigned_count;
	}
	public String getRule() {
		return rule;
	}
	public void setRule(String rule) {
		this.rule = rule;
	}
	public int getPac() {
		return pac;
	}
	public void setPac(int pac) {
		this.pac = pac;
	}
	public String getPac_type() {
		return pac_type;
	}
	public void setPac_type(String pac_type) {
		this.pac_type = pac_type;
	}
	public int getEpisode_period() {
		return episode_period;
	}
	public void setEpisode_period(int episode_period) {
		this.episode_period = episode_period;
	}
	public int getIP_period() {
		return IP_period;
	}
	public void setIP_period(int iP_period) {
		IP_period = iP_period;
	}
	

}
