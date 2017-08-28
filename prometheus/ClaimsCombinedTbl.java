



@Entity
@Table(name = "claims_combined")

@org.hibernate.annotations.GenericGenerator( name="hibernate-native", strategy="native")
/*
@org.hibernate.annotations.GenericGenerator( name="sequence-style-generator",
	strategy="org.hibernate.id.enhanced.SequenceStyleGenerator",
	parameters = {
		@Parameter(name = "optimizer", value = "pooled"),
		@Parameter(name = "increment_size", value = "1")}
) 
*/


public class ClaimsCombinedTbl {
	
	
	@Id
	@GeneratedValue(generator="hibernate-native")
	//@GeneratedValue(generator = "sequence-style-generator")
	@Basic(optional = false)
	@Column(name="id")
	private long id;
	
	private String master_claim_id;
	private String member_id;
	private BigDecimal allowed_amt;
	private int assigned_count;
	private String code_source;
	private Date begin_date;
	private Date end_date;

	private String claim_line_type_code;
	
	public ClaimsCombinedTbl() {
		
		allowed_amt = new BigDecimal("0");
		
	}
	
	public String getMaster_claim_id() {
		return master_claim_id;
	}
	public void setMaster_claim_id(String master_claim_id) {
		this.master_claim_id = master_claim_id;
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
	public int getAssigned_count() {
		return assigned_count;
	}
	public void setAssigned_count(int assigned_count) {
		this.assigned_count = assigned_count;
	}
	public String getCode_source() {
		return code_source;
	}
	public void setCode_source(String code_source) {
		this.code_source = code_source;
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

	public String getClaim_line_type_code() {
		return claim_line_type_code;
	}

	public void setClaim_line_type_code(String claim_line_type_code) {
		this.claim_line_type_code = claim_line_type_code;
	}
	

}
