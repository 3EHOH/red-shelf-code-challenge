



@Entity
@Table(name="member", indexes = {
        @Index(columnList = "member_id", name = "user_memberid_hidx")
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

//org.hibernate.id.enhanced.SequenceStyleGenerator  


public class PlanMember {
	
	
	@Id
	@GeneratedValue(generator="hibernate-native")
	//@GeneratedValue(generator = "sequence-style-generator")
	
	@Basic(optional = false)
	@Column(name="id")
	long id;

	String member_id;
	String gender_code;
	String race_code;
	
	@Transient
	char hispanic_indicator;
	@Transient
	char disability_indicator_flag;
	@Transient
	char primary_insurance_indicator;
	@Transient
	Double member_deductible;
	
	Integer dual_eligible;
	
	String zip_code;
	// do we want address for localization?
	Integer birth_year;
	Integer age;
	Date date_of_death;
	/*
	 * documentation states enforced id is for forced attribution
	 * pcp id is just that
	 */
	String enforced_provider_id;
	String primary_care_provider_id;
	String primary_care_provider_npi;
	Date pcp_effective_date;
	
	String insurance_type;
	
	String plan_id;
	
	String aco_id;
	String aco_name;
	
	

	public String getInsurance_type() {
		return insurance_type;
	}

	public void setInsurance_type(String insurance_type) {
		this.insurance_type = insurance_type;
	}

	public char getHispanic_indicator() {
		return hispanic_indicator;
	}

	public char getPrimary_insurance_indicator() {
		return primary_insurance_indicator;
	}



	transient List<ClaimServLine> svcLines = new ArrayList<ClaimServLine>();
	transient List<ClaimRx> rxClaims = new ArrayList<ClaimRx>();
	transient List<Enrollment> enrollments;
	
	@Transient
	ErrorManager errMgr;
	
	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public String getMember_id() {
		return member_id;
	}

	public void setMember_id(String member_id) {
		this.member_id = member_id;
	}

	public String getGender_code() {
		return gender_code;
	}

	public void setGender_code(String gender_code) {
		if (gender_code != null  &&  gender_code.length() > 1)
			this.gender_code = gender_code.substring(0, 2);
		else
			this.gender_code = gender_code;
	}

	public String getRace_code() {
		return race_code;
	}

	public void setRace_code(String race_code) {
		if (race_code != null  &&  race_code.length() > 1)
			this.race_code = race_code.substring(0, 2);
		else
			this.race_code = race_code;
	}

	public char isHispanic_indicator() {
		return hispanic_indicator;
	}

	public void setHispanic_indicator(char hispanic_indicator) {
		this.hispanic_indicator = hispanic_indicator;
	}

	public char isDisability_indicator_flag() {
		return disability_indicator_flag;
	}

	public void setDisability_indicator_flag(char disability_indicator_flag) {
		this.disability_indicator_flag = disability_indicator_flag;
	}

	public char isPrimary_insurance_indicator() {
		return primary_insurance_indicator;
	}

	public void setPrimary_insurance_indicator(char primary_insurance_indicator) {
		this.primary_insurance_indicator = primary_insurance_indicator;
	}

	public Double getMember_deductible() {
		return member_deductible;
	}

	public void setMember_deductible(Double member_deductible) {
		this.member_deductible = member_deductible;
	}
	
	public int getAdmit_type_code() {
		return dual_eligible == null ? 0 : dual_eligible;
	}
	public void setDual_eligible(Integer dual_eligible) {
		this.dual_eligible = dual_eligible;
	}
	public void setDual_eligible(int dual_eligible) {
		this.dual_eligible = dual_eligible;
	}

	public String getPrimary_care_provider_npi() {
		return primary_care_provider_npi;
	}

	public void setPrimary_care_provider_npi(String primary_care_provider_npi) {
		this.primary_care_provider_npi = primary_care_provider_npi;
	}

	public String getZip_code() {
		return zip_code;
	}

	public void setZip_code(String zip_code) {
		this.zip_code = zip_code;
	}

	public Integer getBirth_year() {
		return birth_year;
	}

	public void setBirth_year(Integer birth_year) {
		this.birth_year = birth_year;
	}

	public String getEnforced_provider_id() {
		return enforced_provider_id;
	}

	public void setEnforced_provider_id(String enforced_provider_id) {
		this.enforced_provider_id = enforced_provider_id;
	}

	public String getPrimary_care_provider_id() {
		return primary_care_provider_id;
	}

	public void setPrimary_care_provider_id(String primary_care_provider_id) {
		this.primary_care_provider_id = primary_care_provider_id;
	}

	public Date getPcp_effective_date() {
		return pcp_effective_date;
	}

	public void setPcp_effective_date(Date pcp_effective_date) {
		this.pcp_effective_date = pcp_effective_date;
	}

	public Date getDate_of_death() {
		return date_of_death;
	}

	public void setDate_of_death(Date date_of_death) {
		this.date_of_death = date_of_death;
	}

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
	}

	public String getPlan_id() {
		return plan_id;
	}

	public void setPlan_id(String plan_id) {
		this.plan_id = plan_id;
	}

	public String getAco_id() {
		return aco_id;
	}

	public void setAco_id(String aco_id) {
		this.aco_id = aco_id;
	}

	public String getAco_name() {
		return aco_name;
	}

	public void setAco_name(String aco_name) {
		this.aco_name = aco_name;
	}

	public void addClaimServiceLine(ClaimServLine c)
	{
		svcLines.add(c);
	}
	
	public void addRxClaim(ClaimRx c)
	{
		rxClaims.add(c);
	}
	
	public List<Enrollment> getEnrollment()
	{
		return enrollments;
	}
	public void setEnrollment(List<Enrollment> e) {
		enrollments = e;
	}
	
	
	
	
	public void setDOBYearFromString(String s) {
		Date dTest;
		Calendar dobcal = Calendar.getInstance();
		try {
			dTest = DateUtil.doParse("DOB", s);
			dobcal = Calendar.getInstance();
			dobcal.setTime(dTest);
		    setBirth_year(dobcal.get(Calendar.YEAR));
		}
		catch (NumberFormatException e) {
			//log.error("Rejecting plan member " + member.getMember_id() + " for invalid PCP effective date");
			//doErrorReporting("pm104", col_value, member);
			//bResult = false;
		} catch (ParseException e) {
			//log.error("Rejecting plan member " + member.getMember_id() + " for invalid PCP effective date");
			//doErrorReporting("pm104", col_value, member);
			//bResult = false;
		}
		catch (NullPointerException e) {
			//log.error("Rejecting plan member " + member.getMember_id() + " for invalid PCP effective date");
			//doErrorReporting("pm104", col_value, member);
			//bResult = false;
		}
	}
	
	
	
	public boolean isValid(ErrorManager errMgr) {
		this.errMgr = errMgr;
		return isValid();
	}
	
	
	public boolean isValid() {

		boolean bResult = true;
		
		// gender code M or F
		if (this.gender_code == null) {
			doErrorReporting("pm102", this.gender_code, this);
			bResult = false;
		}
		else if (this.gender_code == null || this.gender_code.equalsIgnoreCase("M")  || this.gender_code.equalsIgnoreCase("F"))
			{}
		else {
			doErrorReporting("pm102", this.gender_code, this);
			bResult = false;
		}
		
		// valid race code
		if (this.race_code == null  ||  this.race_code.trim().isEmpty())
		{}
		else if( !LookUpTables.isRaceValid(this.race_code)){
			doErrorReporting("pm103", this.race_code, this);
			bResult = false;
		}
		
		return bResult;
	
	}
	
	public List<ClaimServLine> getServiceLines() {
		return svcLines;
	}
	

	public void setServiceLines(ArrayList<ClaimServLine> svcLines) {
		this.svcLines = svcLines;
	}

	
	public List<ClaimRx> getRxClaims() {
		return rxClaims;
	}
	
	public void setRxClaims(List<ClaimRx> c)
	{
		rxClaims = c;
	}
	
	public List<ClaimRx> loadRxClaimsFromList (List<ClaimRx> rxClaims)
	{
		List<ClaimRx> rxReturn = new ArrayList<ClaimRx>();
		for (ClaimRx c : rxClaims) {
			if (c.getMember_id().equals(this.member_id))
				rxReturn.add(c);
		}
		return rxReturn;
	}
	
	
	private void doErrorReporting (String id, String errValue, PlanMember plm) {
		if(errMgr != null)
			errMgr.issueError(id, errValue, plm);
	}
	
	

	public String toString () {
		String s = "memb";
		String sx;
		Field[] f =  this.getClass().getDeclaredFields();
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
