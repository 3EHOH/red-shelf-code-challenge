



@Entity
@Table(name="memberValidationReport")

@org.hibernate.annotations.GenericGenerator( name="hibernate-native", strategy="native")
/*
@org.hibernate.annotations.GenericGenerator( name="sequence-style-generator",
	strategy="org.hibernate.id.enhanced.SequenceStyleGenerator",
	parameters = {
		@Parameter(name = "optimizer", value = "pooled"),
		@Parameter(name = "increment_size", value = "1")}
) 
*/

public class PerMemberReport {
	
	
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
	@Column(name="id")
	private int id;
	*/
	
	private long jobUid;
	private String member_id;
	
	private int IPClaimCount = 0;
	private int OPClaimCount = 0;
	private int PBClaimCount = 0;
	private int RxClaimCount = 0;
	
	private BigDecimal IPClaimTotalAmount = new BigDecimal(0);
	private BigDecimal OPClaimTotalAmount = new BigDecimal(0);
	private BigDecimal PBClaimTotalAmount = new BigDecimal(0);
	private BigDecimal RXClaimTotalAmount = new BigDecimal(0);
	
	private boolean bypass = false;
	private String authorizer;
	private Date authorizeDate;
	
	public long getId() {
		return id;
	}
	public void setId(long id) {
		this.id = id;
	}
	public long getJobUid() {
		return jobUid;
	}
	public void setJobUid(long jobUid) {
		this.jobUid = jobUid;
	}
	public String getMember_id() {
		return member_id;
	}
	public void setMember_id(String member_id) {
		this.member_id = member_id;
	}
	public int getIPClaimCount() {
		return IPClaimCount;
	}
	public void setIPClaimCount(int iPClaimCount) {
		IPClaimCount = iPClaimCount;
	}
	public int getOPClaimCount() {
		return OPClaimCount;
	}
	public void setOPClaimCount(int oPClaimCount) {
		OPClaimCount = oPClaimCount;
	}
	public int getPBClaimCount() {
		return PBClaimCount;
	}
	public void setPBClaimCount(int pBClaimCount) {
		PBClaimCount = pBClaimCount;
	}
	public int getRxClaimCount() {
		return RxClaimCount;
	}
	public void setRxClaimCount(int rxClaimCount) {
		RxClaimCount = rxClaimCount;
	}
	public BigDecimal getIPClaimTotalAmount() {
		return IPClaimTotalAmount;
	}
	public void setIPClaimTotalAmount(BigDecimal iPClaimTotalAmount) {
		IPClaimTotalAmount = iPClaimTotalAmount;
	}
	public BigDecimal getOPClaimTotalAmount() {
		return OPClaimTotalAmount;
	}
	public void setOPClaimTotalAmount(BigDecimal oPClaimTotalAmount) {
		OPClaimTotalAmount = oPClaimTotalAmount;
	}
	public BigDecimal getPBClaimTotalAmount() {
		return PBClaimTotalAmount;
	}
	public void setPBClaimTotalAmount(BigDecimal pBClaimTotalAmount) {
		PBClaimTotalAmount = pBClaimTotalAmount;
	}
	public BigDecimal getRXClaimTotalAmount() {
		return RXClaimTotalAmount;
	}
	public void setRXClaimTotalAmount(BigDecimal rXClaimTotalAmount) {
		RXClaimTotalAmount = rXClaimTotalAmount;
	}
	public boolean isBypass() {
		return bypass;
	}
	public void setBypass(boolean bypass) {
		this.bypass = bypass;
	}
	public String getAuthorizer() {
		return authorizer;
	}
	public void setAuthorizer(String authorizer) {
		this.authorizer = authorizer;
	}
	public Date getAuthorizeDate() {
		return authorizeDate;
	}
	public void setAuthorizeDate(Date authorizeDate) {
		this.authorizeDate = authorizeDate;
	}
	
	

}
