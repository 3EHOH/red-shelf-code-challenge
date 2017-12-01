



@Entity
@Table(name="code", indexes = {
        @Index(columnList = "u_c_id", name = "u_c_id"),
        @Index(columnList = "code_value", name = "code_value"),
        @Index(columnList = "nomen", name = "nomen")
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

public class MedCode implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -2972016702156885354L;
	

	public MedCode () {}

	public MedCode (String function, String code_value, String nomen) {
		this.function_code = function;
		this.code_value = code_value;
		this.nomen = nomen;
	}
	
	@Id
	@GeneratedValue(generator="hibernate-native")
	//@GeneratedValue(generator = "sequence-style-generator")
	@Basic(optional = false)
	@Column(name="id")
	private long id;
	
	
	public Long getId() {
		return id;
	}
	public void setId(Long id) {
		this.id = id;
	}
	
	
	
	
	long u_c_id;
	
	//(name="master_claim_id")
	//private String master_claim_id;
	
	private String function_code;
	
	@Column(name="code_value")
	private String code_value;
	
	@Column(name="nomen")
	private String nomen;
	
	@Column(name="principal")
	private int principal;
	
	
	public long getU_c_id() {
		return u_c_id;
	}
	public void setU_c_id(long u_c_id) {
		this.u_c_id = u_c_id;
	}
	
	public String getFunction_code() {
		return function_code;
	}
	public void setFunction_code(String function_code) {
		this.function_code = function_code;
	}
	
	//public String getMaster_claim_id() {
	//	return master_claim_id;
	//}
	//public void setMaster_claim_id(String master_claim_id) {
	//	this.master_claim_id = master_claim_id;
	//}
	
	public String getCode_value() {
		return code_value;
	}
	public void setCode_value(String code_value) {
		this.code_value = code_value;
	}
	public String getNomen() {
		return nomen;
	}
	public void setNomen(String nomen) {
		this.nomen = nomen;
	}
	public int getPrincipal() {
		return principal;
	}
	public void setPrincipal(int principal) {
		this.principal = principal;
	}	
	
}
