



@Entity
@Table(name="episode", indexes = {
        @Index(columnList = "member_id", name = "user_memberid_hidx")
		}
)

@org.hibernate.annotations.GenericGenerator( name="hibernate-native", strategy="native")
/*
@org.hibernate.annotations.GenericGenerator( name="sequence-style-generator",
	strategy="org.hibernate.id.enhanced.SequenceStyleGenerator",
	parameters = {
		@Parameter(name = "optimizer", value = "pooled"),
		@Parameter(name = "increment_size", value = "1")}
) 
*/

public class EpisodeTbl {
	
	@Id
	@GeneratedValue(generator="hibernate-native")
	//@GeneratedValue(generator = "sequence-style-generator")
	@Basic(optional = false)
	@Column(name="id")
	private long id;
	/*
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Basic(optional = false)
	@Column(name = "id",unique=true,nullable=false)
	private long id;
	*/
	
	@Column(name = "member_id")
	private String member_id;
	
	@Column(name = "claim_id")
	private String claim_id;
	
	@Column(name = "claim_line_id")
	private String claim_line_id;
	
	@Column(name = "master_episode_id")
	private String master_episode_id;
	
	@Column(name = "master_claim_id")
	private String master_claim_id;
	
	@Column(name = "episode_id")
	private String episode_id;
	
	@Column(name = "episode_type")
	private String episode_type;
	
	@Column(name = "episode_begin_date")
	private Date episode_begin_date;
	
	@Column(name = "episode_end_date")
	private Date episode_end_date;
	
	@Column(name = "episode_length_days")
	private int episode_length_days;
	
	@Column(name = "trig_begin_date")
	private Date trig_begin_date;
	
	@Column(name = "trig_end_date")
	private Date trig_end_date;
	
	@Column(name = "associated")
	private int associated;
	
	@Column(name = "association_count")
	private int association_count;
	
	@Column(name = "association_level")
	private int association_level;
	
	@Column(name = "truncated")
	private int truncated;
/*	
	@Column(name = "attrib_cost_physician")
	private String attrib_cost_physician;

	@Column(name = "attrib_cost_facility")
	private String attrib_cost_facility;
	
	@Column(name = "attrib_visit_physician")
	private String attrib_visit_physician;
	
	@Column(name = "attrib_visit_facility")
	private String attrib_visit_facility;
*/	
	@Column(name = "attrib_default_physician")
	private String attrib_default_physician;
	
	@Column(name = "attrib_default_facility")
	private String attrib_default_facility;
	
	
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
	public String getClaim_id() {
		return claim_id;
	}
	public void setClaim_id(String claim_id) {
		this.claim_id = claim_id;
	}
	public String getClaim_line_id() {
		return claim_line_id;
	}
	public void setClaim_line_id(String claim_line_id) {
		this.claim_line_id = claim_line_id;
	}
	public String getMaster_episode_id() {
		return master_episode_id;
	}
	public void setMaster_episode_id(String master_episode_id) {
		this.master_episode_id = master_episode_id;
	}
	public String getMaster_claim_id() {
		return master_claim_id;
	}
	public void setMaster_claim_id(String master_claim_id) {
		this.master_claim_id = master_claim_id;
	}
	public String getEpisode_id() {
		return episode_id;
	}
	public void setEpisode_id(String episode_id) {
		this.episode_id = episode_id;
	}
	public String getEpisode_type() {
		return episode_type;
	}
	public void setEpisode_type(String episode_type) {
		this.episode_type = episode_type;
	}
	public Date getEpisode_begin_date() {
		return episode_begin_date;
	}
	public void setEpisode_begin_date(Date episode_begin_date) {
		this.episode_begin_date = episode_begin_date;
	}
	public Date getEpisode_end_date() {
		return episode_end_date;
	}
	public void setEpisode_end_date(Date episode_end_date) {
		this.episode_end_date = episode_end_date;
	}
	public int getEpisode_length_days() {
		return episode_length_days;
	}
	public void setEpisode_length_days(int episode_length_days) {
		this.episode_length_days = episode_length_days;
	}
	public Date getTrig_begin_date() {
		return trig_begin_date;
	}
	public void setTrig_begin_date(Date trig_begin_date) {
		this.trig_begin_date = trig_begin_date;
	}
	public Date getTrig_end_date() {
		return trig_end_date;
	}
	public void setTrig_end_date(Date trig_end_date) {
		this.trig_end_date = trig_end_date;
	}
	public int getAssociated() {
		return associated;
	}
	public void setAssociated(int associated) {
		this.associated = associated;
	}
	public int getAssociation_count() {
		return association_count;
	}
	public void setAssociation_count(int association_count) {
		this.association_count = association_count;
	}
	public int getAssociation_level() {
		return association_level;
	}
	public void setAssociation_level(int association_level) {
		this.association_level = association_level;
	}
	public int getTruncated() {
		return truncated;
	}
	public void setTruncated(int truncated) {
		this.truncated = truncated;
	}
/*	public String getAttrib_cost_physician() {
		return attrib_cost_physician;
	}
	public void setAttrib_cost_physician(String attrib_cost_physician) {
		this.attrib_cost_physician = attrib_cost_physician;
	}
	public String getAttrib_cost_facility() {
		return attrib_cost_facility;
	}
	public void setAttrib_cost_facility(String attrib_cost_facility) {
		this.attrib_cost_facility = attrib_cost_facility;
	}
	public String getAttrib_visit_physician() {
		return attrib_visit_physician;
	}
	public void setAttrib_visit_physician(String attrib_visit_physician) {
		this.attrib_visit_physician = attrib_visit_physician;
	}
	public String getAttrib_visit_facility() {
		return attrib_visit_facility;
	}
	public void setAttrib_visit_facility(String attrib_visit_facility) {
		this.attrib_visit_facility = attrib_visit_facility;
	}
*/
	public String getAttrib_default_physician() {
		return attrib_default_physician;
	}
	public void setAttrib_default_physician(String attrib_default_physician) {
		this.attrib_default_physician = attrib_default_physician;
	}
	public String getAttrib_default_facility() {
		return attrib_default_facility;
	}
	public void setAttrib_default_facility(String attrib_default_facility) {
		this.attrib_default_facility = attrib_default_facility;
	}


}
