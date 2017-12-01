



@Embeddable
public class CodeKey implements Serializable {
	
	
	private String value;
	private String type_id;
	private String multum_category;
	
	
	@ManyToOne(cascade = CascadeType.ALL)
	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	@ManyToOne(cascade = CascadeType.ALL)
	public String getType_id() {
		return type_id;
	}

	public void setType_id(String type_id) {
		this.type_id = type_id;
	}

	@ManyToOne(cascade = CascadeType.ALL)
	public String getMultum_category() {
		return multum_category;
	}

	public void setMultum_category(String multum_category) {
		this.multum_category = multum_category;
	}

	//StringBuilder sb = new StringBuilder();
	
	/*
	public String toString() {
		sb.setLength(0);
		return sb.append(value).append('|').append(type_id).append('|').append(multum_category).toString();
	}
	*/
	
	private static final long serialVersionUID = 1L;

}
