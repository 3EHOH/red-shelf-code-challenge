


@Entity
@Table(name = "provider_specialty")

@org.hibernate.annotations.GenericGenerator( name="hibernate-native", strategy="native")
/*
@org.hibernate.annotations.GenericGenerator( name="sequence-style-generator",
	strategy="org.hibernate.id.enhanced.SequenceStyleGenerator",
	parameters = {
		@Parameter(name = "optimizer", value = "pooled"),
		@Parameter(name = "increment_size", value = "1")}
) 
*/

public class ProviderSpecialty {
	
	
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
	@Column(name="id", table="provider_specialty")
	private int id;
	*/
	
	private Long provider_uid;
	private String specialty_id;
	private String code_source;
	
	
	public long getId() {
		return id;
	}
	public void setId(long id) {
		this.id = id;
	}
	public Long getProvider_id() {
		return provider_uid;
	}
	public void setProvider_id(Long provider_uid) {
		this.provider_uid = provider_uid;
	}
	public String getSpecialty_id() {
		return specialty_id;
	}
	public void setSpecialty_id(String specialty_id) {
		this.specialty_id = specialty_id;
	}
	public String getCode_source() {
		return code_source;
	}
	public void setCode_source(String code_source) {
		this.code_source = code_source;
	}
	
	
	

}
