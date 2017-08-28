

public class EpisodeMetaData implements Serializable {
	
	/**
   * 
   */
	private static final long serialVersionUID = -9037681850609267887L;
  
  
	private String episode_id;
	private String episode_acronym;
	private String episode_name;
	private String episode_type;
	private Date episode_modified_date;
	private String episode_version;
	private int post_claim_assignment;
	private int look_back;
	private String look_ahead;
	private int condition_min;
	private ArrayList<TriggerConditionMetaData> trigger_conditions;
	private ArrayList<TriggerCodeMetaData> trigger_codes;
	private ArrayList<DxCodeMetaData> dx_codes;
	ArrayList<PxCodeMetaData> px_codes;
	private ArrayList<ComplicationCodeMetaData> complication_codes;
	private ArrayList<AssociationMetaData> associations;
	private HashMap<String,RxCodeMetaData> rx_codes;
	private ArrayList<SubTypeCodeMetaData> sub_type_codes;
	// changes to original
	private HashMap<String, HashMap<String, ArrayList<TriggerCodeMetaData>>> trigger_code_by_ep;
	private HashMap<String, TriggerConditionMetaData> trigger_conditions_by_fac;
	private HashMap<String, TriggerConditionMetaData> trigger_conditions_ep_trig;
	
	


	public EpisodeMetaData () {
		trigger_conditions = new ArrayList<TriggerConditionMetaData>();
		trigger_codes = new ArrayList<TriggerCodeMetaData>();
		dx_codes = new ArrayList<DxCodeMetaData>();
		px_codes = new ArrayList<PxCodeMetaData>();
		complication_codes = new ArrayList<ComplicationCodeMetaData>();
		associations = new ArrayList<AssociationMetaData>();
		rx_codes = new HashMap<String,RxCodeMetaData>();
		sub_type_codes = new ArrayList<SubTypeCodeMetaData>();
		trigger_code_by_ep = new HashMap<String, HashMap<String, ArrayList<TriggerCodeMetaData>>>();
		trigger_conditions_by_fac = new HashMap<String, TriggerConditionMetaData>();
		trigger_conditions_ep_trig = new HashMap<String, TriggerConditionMetaData>();
	}
	
	
	public String getEpisode_id() {
		return episode_id;
	}
	public void setEpisode_id(String episode_id) {
		this.episode_id = episode_id;
	}
	public String getEpisode_acronym() {
		return episode_acronym;
	}
	public void setEpisode_acronym(String episode_acronym) {
		this.episode_acronym = episode_acronym;
	}
	public String getEpisode_name() {
		return episode_name;
	}
	public void setEpisode_name(String episode_name) {
		this.episode_name = episode_name;
	}
	public String getEpisode_type() {
		return episode_type;
	}
	public void setEpisode_type(String episode_type) {
		this.episode_type = episode_type;
	}
	public Date getEpisode_modified_date() {
		return episode_modified_date;
	}
	public void setEpisode_modified_date(Date episode_modified_date) {
		this.episode_modified_date = episode_modified_date;
	}
	public String getEpisode_version() {
		return episode_version;
	}
	public void setEpisode_version(String episode_version) {
		this.episode_version = episode_version;
	}
	public int getPost_claim_assignment() {
		return post_claim_assignment;
	}
	public void setPost_claim_assignment(int post_claim_assignment) {
		this.post_claim_assignment = post_claim_assignment;
	}
	public int getLook_back() {
		return look_back;
	}
	public void setLook_back(int look_back) {
		this.look_back = look_back;
	}
	public String getLook_ahead() {
		return look_ahead;
	}
	public void setLook_ahead(String look_ahead) {
		this.look_ahead = look_ahead;
	}
	public List<TriggerConditionMetaData> getTrigger_conditions() {
		return trigger_conditions;
	}
	public void setTrigger_conditions(ArrayList<TriggerConditionMetaData> trigger_conditions) {
		this.trigger_conditions = trigger_conditions;
	}
	public void addTrigger_condition (TriggerConditionMetaData trigger_condition) {
		trigger_conditions.add(trigger_condition);
	}
	public int getCondition_min() {
		return condition_min;
	}
	public void setCondition_min(int condition_min) {
		this.condition_min = condition_min;
	}
	public List<TriggerCodeMetaData> getTrigger_codes() {
		return trigger_codes;
	}
	public void setTrigger_codes(ArrayList<TriggerCodeMetaData> trigger_codes) {
		this.trigger_codes = trigger_codes;
	}
	public List<DxCodeMetaData> getDx_codes() {
		return dx_codes;
	}
	public void setDx_codes(ArrayList<DxCodeMetaData> dx_codes) {
		this.dx_codes = dx_codes;
	}
	public List<PxCodeMetaData> getPx_codes() {
		return px_codes;
	}
	public void setPx_codes(ArrayList<PxCodeMetaData> px_codes) {
		this.px_codes = px_codes;
	}
	public List<ComplicationCodeMetaData> getComplication_codes() {
		return complication_codes;
	}
	public void setComplication_codes(ArrayList<ComplicationCodeMetaData> complication_codes) {
		this.complication_codes = complication_codes;
	}
	public List<AssociationMetaData> getAssociations() {
		return associations;
	}
	public void setAssociations(ArrayList<AssociationMetaData> associations) {
		this.associations = associations;
	}
	//public List<RxCodeMetaData> getRx_codes() {
	//	return rx_codes;
	//}
	public Map<String, RxCodeMetaData> getRx_codes() {
	  return rx_codes;
	  
	}
	public void setRx_codes(HashMap<String,RxCodeMetaData> rx_codes) {
		this.rx_codes = rx_codes;
	}
	public List<SubTypeCodeMetaData> getSub_type_codes() {
		return sub_type_codes;
	}
	public void setSub_type_codes(ArrayList<SubTypeCodeMetaData> sub_type_codes) {
		this.sub_type_codes = sub_type_codes;
	}
	public HashMap<String, HashMap<String, ArrayList<TriggerCodeMetaData>>> getTrigger_code_by_ep() {
		return trigger_code_by_ep;
	}
	public void setTrigger_code_by_ep(
			HashMap<String, HashMap<String, ArrayList<TriggerCodeMetaData>>> trigger_code_by_ep) {
		this.trigger_code_by_ep = trigger_code_by_ep;
	}
	public HashMap<String, TriggerConditionMetaData> getTrigger_conditions_by_fac() {
		return trigger_conditions_by_fac;
	}
	public void setTrigger_conditions_by_fac(
			HashMap<String, TriggerConditionMetaData> trigger_conditions_by_fac) {
		this.trigger_conditions_by_fac = trigger_conditions_by_fac;
	}
	public HashMap<String, TriggerConditionMetaData> getTrigger_conditions_ep_trig() {
		return trigger_conditions_ep_trig;
	}
	public void setTrigger_conditions_ep_trig(
			HashMap<String, TriggerConditionMetaData> trigger_conditions_ep_trig) {
		this.trigger_conditions_ep_trig = trigger_conditions_ep_trig;
	}
	
	public String toString() {
		return this.episode_id + ":" + this.episode_name;
	}

}
