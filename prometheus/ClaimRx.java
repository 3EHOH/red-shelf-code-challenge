



@Entity

/*
@Table(name="claim_line_rx", indexes = {
        @Index(columnList = "member_id", name = "user_memberid_hidx")
		}
)
*/

@Table(name="claims_combined", indexes = {
        @Index(columnList = "member_id", name = "user_memberid_hidx")
		}
)
@SecondaryTable(name="claim_line_rx", indexes = {
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

public class ClaimRx {
	
	@Id
	@GeneratedValue(generator="hibernate-native")
	//@GeneratedValue(generator = "sequence-style-generator")
	@Basic(optional = false)
	@Column(name="id", table="claims_combined")
	private long id;
	
	/*
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Basic(optional = false)
	@Column(name="id", table="claims_combined")
	int id;
	*/
	
	
	transient int u_c_id;
	
	public int getU_c_id() {
		return u_c_id;
	}
	public void setU_c_id(int u_c_id) {
		this.u_c_id = u_c_id;
	}
	
	// new claim storage list for table output... EC does NOT yet use this, need both for now...
	@Transient
	transient List<MedCode> med_codes;

	@Column(table="claim_line_rx")
	String master_claim_id;
	@Column(table="claim_line_rx", name="member_id")
	String member_id;
	@Column(table="claim_line_rx")
	Double allowed_amt;
	String claim_line_type_code="RX";
	//@Column(name="begin_date")
	@Column(table="claim_line_rx")
	Date rx_fill_date;
	
	
	@Column(table="claim_line_rx")
	String claim_id;
	
	@Column(table="claim_line_rx")
	Double real_allowed_amt;
	@Column(table="claim_line_rx")
	Double proxy_allowed_amt;
	@Column(table="claim_line_rx")
	Double charge_amt;
	@Column(table="claim_line_rx")
	Double paid_amt;
	@Column(table="claim_line_rx")
	Double prepaid_amt;
	@Column(table="claim_line_rx")
	Double copay_amt;
	@Column(table="claim_line_rx")
	Double coinsurance_amt;
	@Column(table="claim_line_rx")
	Double deductible_amt;
	@Column(table="claim_line_rx")
	String drug_nomen;
	@Column(table="claim_line_rx")
	String drug_code;
	@Column(table="claim_line_rx")
	String drug_name;
	@Column(table="claim_line_rx")
	String builder_match_code;
	@Column(table="claim_line_rx")
	Integer days_supply_amt;
	@Column(table="claim_line_rx")
	Double quantityDispensed;
	

	transient boolean isAssigned;
	transient int assignedCount;
	
	@Column(name="line_counter", table="claim_line_rx")
	Integer lineCounter;
	@Column(table="claim_line_rx")
	String sequence_key;
	@Column(table="claim_line_rx")
	String final_version_flag;
	@Column(table="claim_line_rx")
	String claim_encounter_flag;
	@Column(table="claim_line_rx")
	String orig_adj_rev;
	@Column(table="claim_line_rx")
	String genericDrugIndicator;
	@Column(table="claim_line_rx")
	String prescribing_provider_id;
	@Column(name="prescribing_provider_npi", table="claim_line_rx")
	String prescribing_provider_NPI;
	@Column(name="prescribing_provider_dea", table="claim_line_rx")
	String prescribing_provider_DEA;
	@Column(table="claim_line_rx")
	String national_pharmacy_Id;
	@Column(table="claim_line_rx")
	String pharmacy_zip_code;
	@Column(table="claim_line_rx")
	String insurance_product;
	@Column(table="claim_line_rx")
	String plan_id;
	
	

	transient ErrorManager errMgr;
	


	// DUPLICATE VARS FOR SECOND TABLE INSERT...
	@Column(table="claims_combined", name="master_claim_id")
	String tbl_master_claim_id;
	@Column(table="claims_combined", name="member_id")
	String tbl_member_id;
	@Column(table="claims_combined", name="allowed_amt")
	Double tbl_allowed_amt;
	@Column(table="claims_combined", name="begin_date")
	Date tbl_rx_fill_date;
		
	
	public String getTbl_master_claim_id() {
		this.setTbl_master_claim_id(this.getMaster_claim_id());
		return tbl_master_claim_id;
	}
	public void setTbl_master_claim_id(String tbl_master_claim_id) {
		this.tbl_master_claim_id = tbl_master_claim_id;
	}
	public String getTbl_member_id() {
		return tbl_member_id;
	}
	public void setTbl_member_id(String tbl_member_id) {
		this.tbl_member_id = tbl_member_id;
	}
	public Double getTbl_allowed_amt() {
		return tbl_allowed_amt == null ? 0.0d : tbl_allowed_amt;
	}
	public void setTbl_allowed_amt(Double tbl_allowed_amt) {
		this.tbl_allowed_amt = tbl_allowed_amt;
	}
	public void setTbl_allowed_amt(double tbl_allowed_amt) {
		this.tbl_allowed_amt = tbl_allowed_amt;
	}
	public Date getTbl_rx_fill_date() {
		return tbl_rx_fill_date;
	}
	public void setTbl_rx_fill_date(Date tbl_rx_fill_date) {
		this.tbl_rx_fill_date = tbl_rx_fill_date;
	}

	// End of table duplicates...

	
	
	
	
	
	
	public ClaimRx() {
		isAssigned=false;
		assignedCount = 0;
		
		charge_amt = 0d;
		coinsurance_amt=0d;
		copay_amt=0d;
		days_supply_amt=0;
		deductible_amt=0d;
		paid_amt=0d;
		prepaid_amt=0d;
		quantityDispensed=0d;
		
		med_codes = new ArrayList<MedCode>();
		
	}
	
	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}
	
	public String getClaim_line_type_code() {
		return claim_line_type_code;
	}
	
	public List<MedCode> getMed_codes() {
		return med_codes;
	}
	public void setMed_codes(List<MedCode> med_codes) {
		this.med_codes = med_codes;
	}
	
	public String getMaster_claim_id() {
		return master_claim_id;
	}
	public void setMaster_claim_id(String master_claim_id) {
		this.master_claim_id = master_claim_id;
		this.tbl_master_claim_id = master_claim_id;
	}
	
	public String getMember_id() {
		return member_id;
	}
	public void setMember_id(String member_id) {
		this.member_id = member_id;
		this.setMaster_claim_id(this.getMember_id() + "_"+ this.getClaim_id() + "_1");
		this.setTbl_member_id(this.getMember_id());
	}
	public double getAllowed_amt() {
		return allowed_amt == null ? 0.0d : allowed_amt;
	}
	public Double getAllowed_amtD() {
		return allowed_amt;
	}
	public void setAllowed_amt(Double allowed_amt) {
		this.allowed_amt = allowed_amt;
		this.setTbl_allowed_amt(this.getAllowed_amt());
	}
	public void setAllowed_amt(double allowed_amt) {
		this.allowed_amt = allowed_amt;
		this.setTbl_allowed_amt(this.getAllowed_amt());
	}
	
	
	public Double getReal_allowed_amt() {
		return real_allowed_amt;
	}
	public void setReal_allowed_amt(Double real_allowed_amt) {
		this.real_allowed_amt = real_allowed_amt;
	}
	public Double getProxy_allowed_amt() {
		return proxy_allowed_amt;
	}
	public void setProxy_allowed_amt(Double proxy_allowed_amt) {
		this.proxy_allowed_amt = proxy_allowed_amt;
	}
	
	
	public double getCharge_amt() {
		return charge_amt == null ? 0.0d : charge_amt;
	}

	public void setCharge_amt(Double charge_amt) {
		this.charge_amt = charge_amt;
	}
	public void setCharge_amt(double charge_amt) {
		this.charge_amt = charge_amt;
	}

	public String getDrug_nomen() {
		return drug_nomen;
	}
	public void setDrug_nomen(String drug_nomen) {
		this.drug_nomen = drug_nomen;
	}
	public String getDrug_code() {
		return drug_code;
	}
	public void setDrug_code(String drug_code) {
		this.drug_code = drug_code;
		/*
		this.builder_match_code = DrugCodeManager.getCodeFromNDC(this.drug_code);
		addMed_codes( this.builder_match_code, "RX", 1);
		*/
		addMed_codes( this.drug_code, "RX", 1);
	}
	
	public String getBuilder_match_code() {
		return builder_match_code;
	}

	public void setBuilder_match_code(String builder_match_code) {
		this.builder_match_code = builder_match_code;
	}

	public int getDays_supply_amt() {
		return days_supply_amt;
	}
	public void setDays_supply_amt(Integer days_supply_amt) {
		this.days_supply_amt = days_supply_amt;
	}
	public void setDays_supply_amt(int days_supply_amt) {
		this.days_supply_amt = days_supply_amt;
	}
	
	public Date getRx_fill_date() {
		return rx_fill_date;
	}
	public void setRx_fill_date(Date rx_fill_date) {
		this.rx_fill_date = rx_fill_date;
		this.setTbl_rx_fill_date(this.getRx_fill_date());
	}
	
	public boolean isAssigned() {
		return isAssigned;
	}
	public void setAssigned(boolean isAssigned) {
		this.isAssigned = isAssigned;
	}
	
	public int getAssignedCount() {
		return assignedCount;
	}

	public void setAssignedCount(int assignedCount) {
		this.assignedCount = assignedCount;
	}
	
	
	public String getClaim_id() {
		return claim_id;
	}

	public void setClaim_id(String claim_id) {
		this.claim_id = claim_id;
		this.setMaster_claim_id(this.getMember_id() + "_"+ this.getClaim_id() + "_1");
	}

	public int getLineCounter() {
		return lineCounter == null ? 0 : lineCounter;
	}

	public void setLineCounter(int lineCounter) {
		this.lineCounter = lineCounter;
	}

	public String getSequence_key() {
		return sequence_key;
	}

	public void setSequence_key(String sequence_key) {
		this.sequence_key = sequence_key;
	}

	public String getOrig_adj_rev() {
		return orig_adj_rev;
	}

	public void setOrig_adj_rev(String orig_adj_rev) {
		this.orig_adj_rev = orig_adj_rev;
	}

	public String getDrug_name() {
		return drug_name;
	}

	public void setDrug_name(String drug_name) {
		this.drug_name = drug_name;
	}

	public String getGenericDrugIndicator() {
		return genericDrugIndicator;
	}

	public void setGenericDrugIndicator(String genericDrugIndicator) {
		this.genericDrugIndicator = genericDrugIndicator;
	}

	public double getQuantityDispensed() {
		return quantityDispensed == null ? 0.0d : quantityDispensed;
	}

	public void setQuantityDispensed(Double quantityDispensed) {
		this.quantityDispensed = quantityDispensed;
	}
	public void setQuantityDispensed(double quantityDispensed) {
		this.quantityDispensed = quantityDispensed;
	}
	public void setQuantityDispensed(Integer quantityDispensed) {
		this.quantityDispensed = new Double(quantityDispensed);
	}
	public void setQuantityDispensed(int quantityDispensed) {
		this.quantityDispensed = new Double(quantityDispensed);
	}

	public double getPaid_amt() {
		return paid_amt == null ? 0.0d : paid_amt;
	}
	public Double getPaid_amtD() {
		return paid_amt;
	}

	public void setPaid_amt(Double paid_amt) {
		this.paid_amt = paid_amt;
	}
	public void setPaid_amt(double paid_amt) {
		this.paid_amt = paid_amt;
	}

	public double getPrepaid_amt() {
		return prepaid_amt == null ? 0.0d : prepaid_amt;
	}
	public Double getPrepaid_amtD() {
		return prepaid_amt;
	}

	public void setPrepaid_amt(Double prepaid_amt) {
		this.prepaid_amt = prepaid_amt;
	}
	public void setPrepaid_amt(double prepaid_amt) {
		this.prepaid_amt = prepaid_amt;
	}

	public double getCopay_amt() {
		return copay_amt == null ? 0.0d : copay_amt;
	}
	public Double getCopay_amtD() {
		return copay_amt;
	}

	public void setCopay_amt(Double copay_amt) {
		this.copay_amt = copay_amt;
	}
	public void setCopay_amt(double copay_amt) {
		this.copay_amt = copay_amt;
	}

	public double getCoinsurance_amt() {
		return coinsurance_amt == null ? 0.0d : coinsurance_amt;
	}
	public Double getCoinsurance_amtD() {
		return coinsurance_amt;
	}
	

	public void setCoinsurance_amt(Double coinsurance_amt) {
		this.coinsurance_amt = coinsurance_amt;
	}
	public void setCoinsurance_amt(double coinsurance_amt) {
		this.coinsurance_amt = coinsurance_amt;
	}

	public double getDeductible_amt() {
		return deductible_amt == null ? 0.0d : deductible_amt;
	}
	public Double getDeductible_amtD() {
		return deductible_amt;
	}

	public void setDeductible_amt(Double deductible_amt) {
		this.deductible_amt = deductible_amt;
	}
	public void setDeductible_amt(double deductible_amt) {
		this.deductible_amt = deductible_amt;
	}

	public String getPrescribing_provider_id() {
		return prescribing_provider_id;
	}

	public void setPrescribing_provider_id(String prescribing_provider_id) {
		this.prescribing_provider_id = prescribing_provider_id;
	}

	public String getPrescribing_provider_NPI() {
		return prescribing_provider_NPI;
	}

	public void setPrescribing_provider_NPI(String prescribing_provider_NPI) {
		this.prescribing_provider_NPI = prescribing_provider_NPI;
	}

	public String getNational_pharmacy_Id() {
		return national_pharmacy_Id;
	}

	public void setNational_pharmacy_Id(String national_pharmacy_Id) {
		this.national_pharmacy_Id = national_pharmacy_Id;
	}

	public String getPharmacy_zip_code() {
		return pharmacy_zip_code;
	}

	public void setPharmacy_zip_code(String pharmacy_zip_code) {
		this.pharmacy_zip_code = pharmacy_zip_code;
	}

	public String getInsurance_product() {
		return insurance_product;
	}

	public void setInsurance_product(String insurance_product) {
		this.insurance_product = insurance_product;
	}

	public String getPrescribing_provider_DEA() {
		return prescribing_provider_DEA;
	}

	public void setPrescribing_provider_DEA(String prescribing_provider_DEA) {
		this.prescribing_provider_DEA = prescribing_provider_DEA;
	}

	public String getPlan_id() {
		return plan_id;
	}

	public void setPlan_id(String plan_id) {
		this.plan_id = plan_id;
	}
	
	
	public String getFinal_version_flag() {
		return final_version_flag;
	}
	public void setFinal_version_flag(String final_version_flag) {
		this.final_version_flag = final_version_flag;
	}
	public String getClaim_encounter_flag() {
		return claim_encounter_flag;
	}
	public void setClaim_encounter_flag(String claim_encounter_flag) {
		this.claim_encounter_flag = claim_encounter_flag;
	}
	
	public void setClaim_line_type_code(String claim_line_type_code) {
		this.claim_line_type_code = claim_line_type_code;
	}
	
	public void setLineCounter(Integer lineCounter) {
		this.lineCounter = lineCounter;
	}
	public void addMed_codes(String code, String nomen, int prin) {
		MedCode mc = new MedCode();
		mc.setU_c_id(this.getId());
		mc.setFunction_code("RX");
		mc.setCode_value(code);
		mc.setNomen(nomen);
		mc.setPrincipal(prin);
		this.getMed_codes().add(mc);
	}

	public boolean isValid(ErrorManager errMgr) {
		this.errMgr = errMgr;
		return isValid();
	}
	
	public boolean isValid() {

		boolean bResult = true;
		
		return bResult;
		
	}
	
	
	/**
	 * finalize cloned fields for generic (reflection) access
	 */
	public void finalout () {
		if (getClaim_id() == null  ||  getClaim_id().isEmpty()) {
			sbMC.setLength(0);
			if (getDrug_code()  != null)
				sbMC.append( getDrug_code());
			if (getRx_fill_date() != null)
				sbMC.append(getRx_fill_date().getTime());
			setClaim_id(sbMC.toString());
		}
		setMaster_claim_id(getMember_id() + "_"+ getClaim_id() + "_1");
		
		tbl_allowed_amt = allowed_amt;
		tbl_rx_fill_date = rx_fill_date;
		tbl_master_claim_id = master_claim_id;
		tbl_member_id = member_id;
		
	}
	
	@Transient
	transient StringBuffer sbMC = new StringBuffer();
	
	
	/**
	 * finalize cloned fields for generic (reflection) access
	 */
	public void finalin () {
		/*
		if (builder_match_code == null  ||  builder_match_code.isEmpty())
			builder_match_code = DrugCodeManager.getCodeFromNDC(drug_code);
		addMed_codes(builder_match_code, "RX", 1);
		*/
		addMed_codes(drug_code, "RX", 1);
	}
	
	
	public String toString () {
		String s = "clRx";
		String sx;
		List<?> lx;
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
				else if (f[i].getType().equals(List.class)) {
					lx = (List<?>) f[i].get(this);
					if (!lx.isEmpty())
					{
						s = s + "," + f[i].getName() + "=" + lx;
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
