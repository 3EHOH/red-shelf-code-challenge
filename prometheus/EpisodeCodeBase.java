

public abstract class EpisodeCodeBase implements Serializable {
	
	
	private static final long serialVersionUID = -9037681850609267887L;
	
	String code_id;
	String type_id;
	String specific_type_id;
	//String code_name;
	String group_id;
	//String group_name;
	String multum_code_category;
	
	public String getCode_id() {
		return code_id;
	}
	public void setCode_id(String code_id) {
		this.code_id = code_id.trim();
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
	//public String getCode_name() {
	//	return code_name;
	//}
	//public void setCode_name(String code_name) {
	//	this.code_name = code_name;
	//}
	public String getGroup_id() {
		return group_id;
	}
	public void setGroup_id(String group_id) {
		this.group_id = group_id;
	}
	//public String getGroup_name() {
	//	return group_name;
	//}
	//public void setGroup_name(String group_name) {
	//	this.group_name = group_name;
	//}

	
	public String getMultum_code_category() {
		return multum_code_category;
	}
	public void setMultum_code_category(String multum_code_category) {
		this.multum_code_category = multum_code_category;
	}
	
	public String toString () {
		String s = "ecmd";
		String sx;
		Field[] f =  this.getClass().getSuperclass().getDeclaredFields();
		for (int i=0; i < f.length; i++) {
			try {
				if (f[i].get(this) == null)
					continue;
				else if (f[i].getType().equals(String.class)) {
					sx = (String) f[i].get(this);
					if (sx.length() > 0)
					{
						s = s + "," + f[i].getName() + "=" + sx;
					}
				}	
			} 
			catch (IllegalArgumentException | IllegalAccessException e) {
				// do nothing
			}
		}
		return s;
	}
	
}
