



@Entity
@Table(name = "triggers")

@org.hibernate.annotations.GenericGenerator( name="hibernate-native", strategy="native")
/*
@org.hibernate.annotations.GenericGenerator( name="sequence-style-generator",
	strategy="org.hibernate.id.enhanced.SequenceStyleGenerator",
	parameters = {
		@Parameter(name = "optimizer", value = "pooled"),
		@Parameter(name = "increment_size", value = "1")}
) 
*/

public class TriggerTbl {
	
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
	
	@Column(name = "trig_begin_date")
	private Date trig_begin_date;
	
	@Column(name = "trig_end_date")
	private Date trig_end_date;
	
	@Column(name = "episode_begin_date")
	private Date episode_begin_date;
	
	@Column(name = "episode_end_date")
	private Date episode_end_date;
	
	@Column(name = "orig_episode_begin_date")
	private Date orig_episode_begin_date;
	
	@Column(name = "orig_episode_end_date")
	private Date orig_episode_end_date;
	
	@Column(name = "look_back")
	private String look_back;
	
	@Column(name = "look_ahead")
	private String look_ahead;
	
	@Column(name = "req_conf_claim")
	private int req_conf_claim;
	
	@Column(name = "conf_claim_id")
	private String conf_claim_id;
	
	@Column(name = "conf_claim_line_id")
	private String conf_claim_line_id;
	
	@Column(name = "min_code_separation")
	private String min_code_separation;
	
	@Column(name = "max_code_separation")
	private String max_code_separation;
	
	@Column(name = "trig_by_episode_id")
	private String trig_by_episode_id;
	
	@Column(name = "trig_by_master_episode_id")
	private String trig_by_master_episode_id;
	
	@Column(name = "dx_pass_code")
	private String dx_pass_code;
	
	@Column(name = "px_pass_code")
	private String px_pass_code;
	
	@Column(name = "em_pass_code")
	private String em_pass_code;
	
	@Column(name = "conf_dx_pass_code")
	private String conf_dx_pass_code;
	
	@Column(name = "conf_px_pass_code")
	private String conf_px_pass_code;
	
	@Column(name = "conf_em_pass_code")
	private String conf_em_pass_code;
	
	@Column(name = "dropped")
	private int dropped;
	
	@Column(name = "truncated")
	private int truncated;
	
	@Column(name = "win_claim_id")
	private String win_claim_id;
	
	private String win_master_claim_id;
	
	
	
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
	public Date getOrig_episode_begin_date() {
		return orig_episode_begin_date;
	}
	public void setOrig_episode_begin_date(Date orig_episode_begin_date) {
		this.orig_episode_begin_date = orig_episode_begin_date;
	}
	public Date getOrig_episode_end_date() {
		return orig_episode_end_date;
	}
	public void setOrig_episode_end_date(Date orig_episode_end_date) {
		this.orig_episode_end_date = orig_episode_end_date;
	}
	public String getLook_back() {
		return look_back;
	}
	public void setLook_back(String look_back) {
		this.look_back = look_back;
	}
	public String getLook_ahead() {
		return look_ahead;
	}
	public void setLook_ahead(String look_ahead) {
		this.look_ahead = look_ahead;
	}
	public int getReq_conf_claim() {
		return req_conf_claim;
	}
	public void setReq_conf_claim(int req_conf_claim) {
		this.req_conf_claim = req_conf_claim;
	}
	public String getConf_claim_id() {
		return conf_claim_id;
	}
	public void setConf_claim_id(String conf_claim_id) {
		this.conf_claim_id = conf_claim_id;
	}
	public String getConf_claim_line_id() {
		return conf_claim_line_id;
	}
	public void setConf_claim_line_id(String conf_claim_line_id) {
		this.conf_claim_line_id = conf_claim_line_id;
	}
	public String getMin_code_separation() {
		return min_code_separation;
	}
	public void setMin_code_separation(String min_code_separation) {
		this.min_code_separation = min_code_separation;
	}
	public String getMax_code_separation() {
		return max_code_separation;
	}
	public void setMax_code_separation(String max_code_separation) {
		this.max_code_separation = max_code_separation;
	}
	public String getTrig_by_episode_id() {
		return trig_by_episode_id;
	}
	public void setTrig_by_episode_id(String trig_by_episode_id) {
		this.trig_by_episode_id = trig_by_episode_id;
	}
	public String getTrig_by_master_episode_id() {
		return trig_by_master_episode_id;
	}
	public void setTrig_by_master_episode_id(String trig_by_master_episode_id) {
		this.trig_by_master_episode_id = trig_by_master_episode_id;
	}
	public String getDx_pass_code() {
		return dx_pass_code;
	}
	public void setDx_pass_code(String dx_pass_code) {
		this.dx_pass_code = dx_pass_code;
	}
	public String getPx_pass_code() {
		return px_pass_code;
	}
	public void setPx_pass_code(String px_pass_code) {
		this.px_pass_code = px_pass_code;
	}
	public String getEm_pass_code() {
		return em_pass_code;
	}
	public void setEm_pass_code(String em_pass_code) {
		this.em_pass_code = em_pass_code;
	}
	public String getConf_dx_pass_code() {
		return conf_dx_pass_code;
	}
	public void setConf_dx_pass_code(String conf_dx_pass_code) {
		this.conf_dx_pass_code = conf_dx_pass_code;
	}
	public String getConf_px_pass_code() {
		return conf_px_pass_code;
	}
	public void setConf_px_pass_code(String conf_px_pass_code) {
		this.conf_px_pass_code = conf_px_pass_code;
	}
	public String getConf_em_pass_code() {
		return conf_em_pass_code;
	}
	public void setConf_em_pass_code(String conf_em_pass_code) {
		this.conf_em_pass_code = conf_em_pass_code;
	}
	public int getDropped() {
		return dropped;
	}
	public void setDropped(int dropped) {
		this.dropped = dropped;
	}
	public int getTruncated() {
		return truncated;
	}
	public void setTruncated(int truncated) {
		this.truncated = truncated;
	}
	public String getWin_claim_id() {
		return win_claim_id;
	}
	public void setWin_claim_id(String win_claim_id) {
		this.win_claim_id = win_claim_id;
	}
	public String getWin_master_claim_id() {
		return win_master_claim_id;
	}
	public void setWin_master_claim_id(String win_master_claim_id) {
		this.win_master_claim_id = win_master_claim_id;
	}

}
