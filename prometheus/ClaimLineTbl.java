




@Entity
@Table(name = "claim_line")

@org.hibernate.annotations.GenericGenerator( name="hibernate-native", strategy="native")
/*
@org.hibernate.annotations.GenericGenerator( name="sequence-style-generator",
	strategy="org.hibernate.id.enhanced.SequenceStyleGenerator",
	parameters = {
		@Parameter(name = "optimizer", value = "pooled"),
		@Parameter(name = "increment_size", value = "1")}
) 
*/

public class ClaimLineTbl {
	
	@Id
	@GeneratedValue(generator="hibernate-native")
	//@GeneratedValue(generator = "sequence-style-generator")
	@Basic(optional = false)
	@Column(name="id")
	/*
	@Id
	@Column(name = "id")
	*/
	long u_c_id;
	
	private String member_id;
	
	private BigDecimal allowed_amt;
	private Date begin_date;
	private Date end_date;
	private String claim_line_type_code;

	private String type_of_bill;
	private String facility_type_code;		
	
	
	public ClaimLineTbl() {
		
	}

	public String getClaim_line_type_code() {
		return claim_line_type_code;
	}
	public void setClaim_line_type_code(String val) {
		this.claim_line_type_code = val;
	}
	
	public String getType_of_bill() {
		return type_of_bill;
	}
	public void setType_of_bill(String val) {
		this.type_of_bill = val;
	}

	public String getFacility_type_code() {
		return facility_type_code;
	}
	public void setFacility_type_code(String val) {
		this.facility_type_code = val;
	}
	
	public String getMember_id() {
		return member_id;
	}
	public void setMember_id(String member_id) {
		this.member_id = member_id;
	}
	public BigDecimal getAllowed_amt() {
		return allowed_amt;
	}
	public void setAllowed_amt(BigDecimal allowed_amt) {
		this.allowed_amt = allowed_amt;
	}
	public Date getBegin_date() {
		return begin_date;
	}
	public void setBegin_date(Date begin_date) {
		this.begin_date = begin_date;
	}
	public Date getEnd_date() {
		return end_date;
	}
	public void setEnd_date(Date end_date) {
		this.end_date = end_date;
	}
	
	
 
	

}
