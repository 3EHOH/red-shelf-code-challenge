
public class CodeQueryResult {
	
	private String function_id; 
	private String code_id;     
	private String type_id;
	private String specific_type_id = "";
	private String code_name;
	private String group_id;
	private String group_name;
	private String sub_type_group_id;
	private Integer complication_type;
	private Boolean core;
	private Boolean sufficient;
	private Boolean pas;
	private String rx_assignment;
	private String betos_category;
	private Boolean qualifying_diagnosis;

	
	public String getFunction_id() {
		return function_id;
	}
	public void setFunction_id(String function_id) {
		this.function_id = function_id;
	}
	public String getCode_id() {
		return code_id;
	}
	public void setCode_id(String code_id) {
		this.code_id = code_id;
	}
	public String getType_id() {
		return type_id;
	}
	public void setType_id(String type_id) {
		this.type_id = type_id;
	}
	public String getSpecific_type_id() {
		return specific_type_id;
	}
	public void setSpecific_type_id(String specific_type_id) {
		this.specific_type_id = specific_type_id;
	}
	public String getCode_name() {
		return code_name;
	}
	public void setCode_name(String code_name) {
		this.code_name = code_name;
	}
	public String getGroup_id() {
		return group_id;
	}
	public void setGroup_id(String group_id) {
		this.group_id = group_id;
	}
	public String getGroup_name() {
		return group_name;
	}
	public void setGroup_name(String group_name) {
		this.group_name = group_name;
	}
	public String getSub_type_group_id() {
		return sub_type_group_id;
	}
	public void setSub_type_group_id(String sub_type_group_id) {
		this.sub_type_group_id = sub_type_group_id;
	}
	public Integer getComplication_type() {
		return complication_type;
	}
	public void setComplication_type(Integer o) {
		this.complication_type = o;
	}
	public Boolean getCore() {
		return core;
	}
	public void setCore(Boolean o) {
		this.core = o;
	}
	public Boolean getSufficient() {
		return sufficient;
	}
	public void setSufficient(Boolean o) {
		this.sufficient = o;
	}
	public Boolean getPas() {
		return pas;
	}
	public void setPas(Boolean o) {
		this.pas = o;
	}
	public String getRx_assignment() {
		return rx_assignment;
	}
	public void setRx_assignment(String rx_assignment) {
		this.rx_assignment = rx_assignment;
	}
	public String getBetos_category() {
		return betos_category;
	}
	public void setBetos_category(String betos_category) {
		this.betos_category = betos_category;
	}
	public Boolean getQualifying_diagnosis() {
		return qualifying_diagnosis;
	}
	public void setQualifying_diagnosis(Boolean o) {
		this.qualifying_diagnosis = o;
	}
	
}
