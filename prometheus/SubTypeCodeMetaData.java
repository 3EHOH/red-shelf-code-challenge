

public class SubTypeCodeMetaData extends EpisodeCodeBase implements Serializable {
	
	
	private static final long serialVersionUID = 1L;
	
	
	private String sub_type_group_id;
	private String sub_type_group_name;
	
	public String getSub_type_group_id() {
		return sub_type_group_id;
	}
	public void setSub_type_group_id(String sub_type_group_id) {
		this.sub_type_group_id = sub_type_group_id;
	}
	public String getSub_type_group_name() {
		return sub_type_group_name;
	}
	public void setSub_type_group_name(String sub_type_group_name) {
		this.sub_type_group_name = sub_type_group_name;
	}

}
