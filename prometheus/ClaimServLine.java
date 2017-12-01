



@Entity

/*
@Table(name="claim_line", indexes = {
        @Index(columnList = "member_id", name = "user_memberid_hidx")
		}
)
*/

@Table(name="claims_combined", indexes = {
        @Index(columnList = "member_id", name = "user_memberid_hidx")
		}
)
@SecondaryTable(name="claim_line", indexes = {
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


public class ClaimServLine implements Serializable {
	
	/**
	 * 
	 */
	@Transient
	private static final long serialVersionUID = 6206157595719832494L;
	
	/*
	 * should only have one entry per claim id
	 */
	
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
	
	// test for holding the id from the claim insert...
	transient long u_c_id;
	
	public long getU_c_id() {
		return u_c_id;
	}
	public void setU_c_id(long u_c_id) {
		this.u_c_id = u_c_id;
	}

	@Column(table="claim_line", name="master_claim_id")
	String master_claim_id;
	@Column(table="claim_line", name="member_id")
	String member_id;
	@Column(table="claim_line", precision=15, scale=4)
	Double allowed_amt;
	@Column(table="claim_line", precision=15, scale=4)
	Double real_allowed_amt;
	@Column(table="claim_line", precision=15, scale=4)
	Double proxy_allowed_amt;
	@Column(table="claim_line")
	String claim_line_type_code;
	@Column(table="claim_line")
	Date begin_date;
	@Column(table="claim_line")
	Date end_date;
	
	@Column(table="claim_line")
	String claim_id;
	@Column(table="claim_line")
	String claim_line_id;
	@Column(table="claim_line")
	String sequence_key;
	@Column(table="claim_line")
	String final_version_flag;
	@Column(table="claim_line")
	String claim_encounter_flag;
	@Column(table="claim_line")
	String provider_npi;
	@Column(table="claim_line")
	String provider_id;
	@Column(table="claim_line")
	String physician_id;
	@Column(table="claim_line")
	String facility_id;
	@Column(table="claim_line")
	String facility_type_code;
	@Column(table="claim_line")
	String place_of_svc_code;
	@Column(table="claim_line")
	String plan_id;
	
	@Transient
	transient boolean assigned; // if has been assigned and is final assignment, mark true here so we won't go any further with it.
	
	@Transient
	transient int assignedCount; // the number of times this claim has been assigned (so we know what proportion of the dollars to apportion to each)
	
	@Column(table="claim_line")
	Integer quantity;
	@Column(table="claim_line", precision=15, scale=4)
	Double standard_payment_amt;
	@Column(table="claim_line", precision=15, scale=4)
	Double charge_amt;
	@Column(table="claim_line", precision=15, scale=4)
	Double paid_amt;
	@Column(table="claim_line", precision=15, scale=4)
	Double prepaid_amt;
	@Column(table="claim_line", precision=15, scale=4)
	Double copay_amt;
	@Column(table="claim_line", precision=15, scale=4)
	Double coinsurance_amt;
	@Column(table="claim_line", precision=15, scale=4)
	Double deductible_amt;
	@Column(table="claim_line")
	String insurance_product;
	@Column(table="claim_line")
	Date admission_date;
	@Column(table="claim_line")
	String admission_src_code;
	@Column(table="claim_line")
	Integer admit_type_code;
	@Column(table="claim_line")
	String discharge_status_code;
	@Column(table="claim_line")
	Date discharge_date;
	@Column(table="claim_line")
	String type_of_bill;
	@Column(table="claim_line")
	Integer rev_count;
	@Column(table="claim_line")
	String drg_version;
	@Column(table="claim_line")
	String ms_drg_code;
	@Column(table="claim_line")
	String apr_drg_code;
	
	@Transient
	String orig_adj_rev;
	@Transient
	transient String claim_status;
	
	// new claim storage list for table output... EC does NOT yet use this, need both for now...
	@Transient
	List<MedCode> med_codes;


	@Transient
	transient String diag_claim_nomen = "DX";		// DX = ICD-9 CM; DXX = ICD-10 CM
	//@Transient
	//String principal_diag_code;
	//@Transient
	//List<String> secondary_diag_code;
	@Transient
	transient String proc_claim_nomen = "PX";		// PX = ICD-9 PCS; PXX = ICD-10 PCS
	@Transient
	transient String proc_line_level_nomen = "HCPC";	
	//@Transient
	//List<String> principal_proc_code;
	@Transient
	String principal_proc_mod_code;
	//@Transient
	//List<String> secondary_proc_code;
	@Transient
	List<String> other_proc_mod_code;
	//@Transient
	//List<String> rev_code;
	@Transient
	List<String> ecodes;
	@Transient
	boolean capitatedFlag;
	@Transient
	transient boolean edService;
	
	@Transient
	transient ErrorManager errMgr;
	

	// DUPLICATE VARS FOR SECOND TABLE INSERT...
	@Column(table="claims_combined", name="master_claim_id")
	String tbl_master_claim_id;
	@Column(table="claims_combined", name="member_id")
	String tbl_member_id;
	@Column(table="claims_combined", name="allowed_amt")
	Double tbl_allowed_amt;
	@Column(table="claims_combined", name="begin_date")
	Date tbl_begin_date;
	@Column(table="claims_combined", name="end_date")
	Date tbl_end_date;
	@Column(table="claims_combined", name="claim_line_type_code")
	String tbl_claim_line_type_code;
	
	public String getTbl_master_claim_id() {
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
	public double getTbl_allowed_amt() {
		return tbl_allowed_amt == null ? 0.0d : tbl_allowed_amt;
	}
	public void setTbl_allowed_amt(double tbl_allowed_amt) {
		this.tbl_allowed_amt = tbl_allowed_amt;
	}
	public Date getTbl_begin_date() {
		return tbl_begin_date;
	}
	public void setTbl_begin_date(Date tbl_begin_date) {
		this.tbl_begin_date = tbl_begin_date;
	}
	public Date getTbl_end_date() {
		return tbl_end_date;
	}
	public void setTbl_end_date(Date tbl_end_date) {
		this.tbl_end_date = tbl_end_date;
	}
	public String getTbl_claim_line_type_code() {
		return tbl_claim_line_type_code;
	}
	public void setTbl_claim_line_type_code(String tbl_claim_line_type_code) {
		this.tbl_claim_line_type_code = tbl_claim_line_type_code;
	}
	// End of table duplicates...
	
	
	
	
	public ClaimServLine () {
		
		med_codes = new ArrayList<MedCode>();
		
		//principal_proc_code = new ArrayList<String>();
		//secondary_diag_code = new ArrayList<String>();
		//secondary_proc_code = new ArrayList<String>();
		other_proc_mod_code = new ArrayList<String>();
		//rev_code = new ArrayList<String>();
		ecodes = new ArrayList<String>();
		assigned = false;
		assignedCount=0;
		
	}
	
	public long getId() {
		return id;
	}
	public void setId(long id) {
		this.id = id;
		this.setU_c_id(id);
	}
	
	public String getMember_id() {
		return member_id;
	}
	public void setMember_id(String member_id) {
		this.member_id = member_id;
		this.setTbl_member_id(member_id);
		// create master claim id
		this.setMaster_claim_id(this.getMember_id() + "_"+ this.getClaim_id() + "_" + this.getClaim_line_id());
	}
	public String getClaim_id() {
		return claim_id;
	}
	public void setClaim_id(String claim_id) {
		this.claim_id = claim_id;
		// create master claim id
		this.setMaster_claim_id(this.getMember_id() + "_"+ this.getClaim_id() + "_" + this.getClaim_line_id());
	}
	public String getClaim_line_id() {
		return claim_line_id;
	}
	public void setClaim_line_id(String claim_line_id) {
		this.claim_line_id = claim_line_id;
		//create master claim id
		this.setMaster_claim_id(this.getMember_id() + "_"+ this.getClaim_id() + "_" + this.getClaim_line_id());
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

	public String getMaster_claim_id() {
		return this.getMember_id() + "_"+ this.getClaim_id() + "_" + this.getClaim_line_id();
	}
	public void setMaster_claim_id(String master_claim_id) {
		this.master_claim_id = master_claim_id;
		this.setTbl_master_claim_id(master_claim_id);
	}
	public String getProvider_id() {
		return provider_id;
	}
	public void setProvider_id(String provider_id) {
		this.provider_id = provider_id;
	}
	public String getProvider_NPI() {
		return provider_npi;
	}

	public void setProvider_NPI(String provider_NPI) {
		this.provider_npi = provider_NPI;
	}

	public String getPhysician_id() {
		return physician_id;
	}

	public void setPhysician_id(String physician_id) {
		this.physician_id = physician_id;
	}

	public String getFacility_id() {
		return facility_id;
	}

	public void setFacility_id(String facility_id) {
		this.facility_id = facility_id;
	}

	public double getAllowed_amt() {
		return allowed_amt == null ? 0.0d : allowed_amt;
	}
	public Double getAllowed_amtD() {
		return allowed_amt;
	}
	public void setAllowed_amt(Double allowed_amt) {
		this.allowed_amt = allowed_amt;
		this.tbl_allowed_amt = allowed_amt;
	}
	public void setAllowed_amt(double allowed_amt) {
		this.allowed_amt = allowed_amt;
		this.tbl_allowed_amt = allowed_amt;
	}
	
	public Double getReal_allowed_amt() {
		return real_allowed_amt == null ? 0.0d : real_allowed_amt;
	}
	public void setReal_allowed_amt(Double real_allowed_amt) {
		this.real_allowed_amt = real_allowed_amt;
	}
	public Double getProxy_allowed_amt() {
		return proxy_allowed_amt == null ? 0.0d : proxy_allowed_amt;
	}
	public void setProxy_allowed_amt(Double proxy_allowed_amt) {
		this.proxy_allowed_amt = proxy_allowed_amt;
	}
	
	public int getQuantity() {
		return quantity  == null ? 0 : quantity;
	}

	public void setQuantity(Integer quantity) {
		this.quantity = quantity;
	}
	public void setQuantity(Double quantity) {
		this.quantity = new Integer(quantity.intValue());
	}
	public void setQuantity(int quantity) {
		this.quantity = quantity;
	}
	public void setQuantity(double quantity) {
		this.quantity = new Double(quantity).intValue();
	}

	public double getCharge_amt() {
		return charge_amt == null ? 0.0d : charge_amt;
	}
	public Double getCharge_amtD() {
		return charge_amt;
	}

	public void setCharge_amt(Double charge_amt) {
		this.charge_amt = charge_amt;
	}
	public void setCharge_amt(Integer charge_amt) {
		this.charge_amt = new Double(charge_amt);
	}
	public void setCharge_amt(double charge_amt) {
		this.charge_amt = charge_amt;
	}
	public void setCharge_amt(int charge_amt) {
		this.charge_amt = new Double(charge_amt);
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

	public double getStandard_payment_amt() {
		return standard_payment_amt == null ? 0.0d : standard_payment_amt;
	}
	public void setStandard_payment_amt(Double standard_payment_amt) {
		this.standard_payment_amt = standard_payment_amt;
	}
	public void setStandard_payment_amt(double standard_payment_amt) {
		this.standard_payment_amt = standard_payment_amt;
	}
	
	public String getFacility_type_code() {
		return facility_type_code;
	}
	public void setFacility_type_code(String facility_type_code) {
		this.facility_type_code = facility_type_code;
	}
	public Date getBegin_date() {
		return begin_date;
	}
	public void setBegin_date(Date begin_date) {
		this.begin_date = begin_date;
		this.setTbl_begin_date(getBegin_date());
	}
	public Date getEnd_date() {
		return end_date == null ? begin_date : end_date;
	}
	public void setEnd_date(Date end_date) {
		this.end_date = end_date;
		this.setTbl_end_date(getEnd_date());
	}
	public Date getAdmission_date() {
		return admission_date;
	}

	public void setAdmission_date(Date admission_date) {
		this.admission_date = admission_date;
	}

	public String getAdmission_src_code() {
		return admission_src_code;
	}
	public void setAdmission_src_code(String admission_src_code) {
		this.admission_src_code = admission_src_code;
	}
	public int getAdmit_type_code() {
		return admit_type_code == null ? 0 : admit_type_code;
	}
	public void setAdmit_type_code(Integer admit_type_code) {
		this.admit_type_code = admit_type_code;
	}
	public void setAdmit_type_code(int admit_type_code) {
		this.admit_type_code = admit_type_code;
	}
	
	public String getDischarge_status_code() {
		return discharge_status_code;
	}
	public void setDischarge_status_code(String discharge_status_code) {
		this.discharge_status_code = discharge_status_code;
	}
	public Date getDischarge_date() {
		return discharge_date;
	}

	public void setDischarge_date(Date discharge_date) {
		this.discharge_date = discharge_date;
	}

	public String getPlace_of_svc_code() {
		return place_of_svc_code;
	}
	public void setPlace_of_svc_code(String place_of_svc_code) {
		this.place_of_svc_code = place_of_svc_code;
	}
	public String getClaim_line_type_code() {
		return claim_line_type_code;
	}
	public void setClaim_line_type_code(String claim_line_type_code) {
		this.claim_line_type_code = claim_line_type_code;
		this.setTbl_claim_line_type_code(getClaim_line_type_code());
	}
	public String getClaim_status() {
		return claim_status;
	}

	public void setClaim_status(String claim_status) {
		this.claim_status = claim_status;
	}

	public String getDiag_claim_nomen() {
		return diag_claim_nomen;
	}
	public void setDiag_claim_nomen(String diag_claim_nomen) {
		this.diag_claim_nomen = diag_claim_nomen;
	}
	
	
	public List<MedCode> getMed_codes() {
		return med_codes;
	}
	public void setMed_codes(List<MedCode> med_codes) {
		this.med_codes = med_codes;
	}
	
	
	
	public void addMed_codes(String function, String code, String nomen, int prin) {
		MedCode mc = new MedCode(function, code, nomen);
		mc.setU_c_id(this.getId());
		mc.setPrincipal(prin);
		med_codes.add(mc);
	}
	
	public String getPrincipal_diag_code() {
		String s = null;
		for (MedCode _mc : getMed_codes()) {
			//log.info("_mc: " + _mc.getCode_value() + "|" + _mc.getNomen() + "|" + _mc.getPrincipal());
			if (_mc.getFunction_code().equals("DX")  &&  _mc.getPrincipal() == 1) {
				s = _mc.getCode_value();
				break;
			}
		}
		return s;
	}
	public MedCode getPrincipal_diag_code_object() {
		MedCode _o = null;
		for (MedCode _mc : getMed_codes()) {
			if (_mc.getFunction_code().equals("DX")  &&  _mc.getPrincipal() == 1) {
				_o = _mc;
				break;
			}
		}
		return _o;
	}
	public void setPrincipal_diag_code(String principal_diag_code) {
		MedCode _o = null;
		String sCd = principal_diag_code == null  ?  null : principal_diag_code.replace(".", "");
		if ( (_o = getPrincipal_diag_code_object()) == null) {
			this.addMed_codes("DX", sCd, this.getDiag_claim_nomen(), 1);
		}
		else {
			_o.setCode_value(sCd);
		}
	}
	
	public void setDiag_code_nomen(String nomen) {
		this.setDiag_claim_nomen(nomen);
		for (MedCode _mc : getMed_codes()) {
			if (_mc.getFunction_code().equals("DX")) {
				_mc.setNomen(nomen);
			}
		}
	}
	
	public List<String> getSecondary_diag_code() {
		//return secondary_diag_code;
		List<String> retList = new ArrayList<String>();
		for (MedCode _mc : getMed_codes()) {
			if (_mc.getFunction_code().equals("DX")  &&  _mc.getPrincipal() != 1) {
				retList.add(_mc.getCode_value());
			}
		}
		return retList;
	}
	public List<MedCode> getSecondary_diag_code_objects() {
		//return secondary_diag_code;
		List<MedCode> retList = new ArrayList<MedCode>();
		for (MedCode _mc : getMed_codes()) {
			if (_mc.getFunction_code().equals("DX")  &&  _mc.getPrincipal() != 1) {
				retList.add(_mc);
			}
		}
		return retList;
	}
	
	public String getProc_claim_nomen() {
		return proc_claim_nomen;
	}
	public void setProc_claim_nomen(String proc_claim_nomen) {
		this.proc_claim_nomen = proc_claim_nomen;
		for (MedCode _mc : getMed_codes()) {
			if (_mc.getFunction_code().equals("PX")  &&  _mc.getPrincipal() == 1) {
				if (_mc.getNomen().equals("HCPC")  || _mc.getNomen().equals("CPT") ) {}
				else
					_mc.setNomen(proc_claim_nomen);
			}
		}
	}
	
	public String getProc_line_level_nomen() {
		return proc_line_level_nomen;
	}
	public void setProc_line_level_nomen(String proc_level_line_nomen) {
		this.proc_line_level_nomen = proc_level_line_nomen;
		MedCode _mc = getLineLevelPrincipal_proc_code_object();
		if (_mc != null) {
			_mc.setNomen(proc_level_line_nomen);
		}
	}
	
	public List<String> getPrincipal_proc_code() {
		//return principal_proc_code;
		//log.info("get principal proc code:");
		List<String> retList = new ArrayList<String>();
		for (MedCode _mc : getMed_codes()) {
			if (_mc.getFunction_code().equals("PX")  &&  _mc.getPrincipal() == 1) {
				retList.add(_mc.getCode_value());
				//log.info(">>>>got principal proc code: " + _mc.getFunction_code() + "|" + _mc.getCode_value() + "|" + _mc.getNomen());
			}
		}
		return retList;
	}
	

	public List<MedCode> getPrincipal_proc_code_objects() {
		//return principal_proc_code;
		List<MedCode> retList = new ArrayList<MedCode>();
		for (MedCode _mc : getMed_codes()) {
			if (_mc.getFunction_code().equals("PX")  &&  _mc.getPrincipal() == 1) {
				retList.add(_mc);
			}
		}
		return retList;
	}
	
	
	public MedCode getLineLevelPrincipal_proc_code_object() {
		MedCode _o = null;
		for (MedCode _mc : getMed_codes()) {
			if (_mc.getFunction_code().equals("PX")  &&  _mc.getPrincipal() == 1) {
				if (_mc.getNomen().equals("HCPC")  || _mc.getNomen().equals("CPT") ) {
					_o = _mc;
					break;
				}
			}
		}
		return _o;
	}
	
	/*
	public void setPrincipal_proc_code(List<String> principal_proc_code) {
		this.principal_proc_code = principal_proc_code;
	}
	*/
	
	public String getPrincipal_proc_mod_code() {
		return principal_proc_mod_code;
	}
	public void setPrincipal_proc_mod_code(String principal_proc_mod_code) {
		this.principal_proc_mod_code = principal_proc_mod_code;
	}
	public List<String> getOther_proc_mod_code() {
		return other_proc_mod_code;
	}

	public void setOther_proc_mod_code(List<String> other_proc_mod_code) {
		this.other_proc_mod_code = other_proc_mod_code;
	}

	public List<String> getSecondary_proc_code() {
		//return secondary_proc_code;
		//log.info("get other proc code: ");
		List<String> retList = new ArrayList<String>();
		for (MedCode _mc : getMed_codes()) {
			if (_mc.getFunction_code().equals("PX")  &&  _mc.getPrincipal() != 1) {
				retList.add(_mc.getCode_value());
				//log.info(">>>>got other proc code: " + _mc.getFunction_code() + "|" + _mc.getCode_value() + "|" + _mc.getNomen() );
			}
		}
		return retList;
	}
	/*
	public void setSecondary_proc_code(List<String> secondary_proc_code) {
		this.secondary_proc_code = secondary_proc_code;
	}
	*/
	
	public List<MedCode> getSecondary_proc_code_objects() {
		//return principal_proc_code;
		List<MedCode> retList = new ArrayList<MedCode>();
		for (MedCode _mc : getMed_codes()) {
			if (_mc.getFunction_code().equals("PX")  &&  _mc.getPrincipal() != 1) {
				retList.add(_mc);
			}
		}
		return retList;
	}
	
	public int getRev_count() {
		return rev_count;
	}
	public void setRev_count(int rev_count) {
		this.rev_count = rev_count;
	}
	public List<String> getRev_code() {
		//return rev_code;
		List<String> retList = new ArrayList<String>();
		for (MedCode _mc : getMed_codes()) {
			if (_mc.getNomen().equals("REV")) {
				retList.add(_mc.getCode_value());
			}
		}
		return retList;
	}
	/*
	public void setRev_code(List<String> rev_code) {
		this.rev_code = rev_code;
	}
	*/
	public String getMs_drg_code() {
		return ms_drg_code;
	}
	public void setMs_drg_code(String ms_drg_code) {
		this.ms_drg_code = ms_drg_code;
	}
	public String getInsurance_product() {
		return insurance_product;
	}

	public void setInsurance_product(String insurance_product) {
		this.insurance_product = insurance_product;
	}

	public String getDrg_version() {
		return drg_version;
	}

	public void setDrg_version(String drg_version) {
		this.drg_version = drg_version;
	}

	public List<String> getEcodes() {
		return ecodes;
	}

	public void setEcodes(List<String> ecodes) {
		this.ecodes = ecodes;
	}

	public boolean isCapitatedFlag() {
		return capitatedFlag;
	}

	public void setCapitatedFlag(boolean capitatedFlag) {
		this.capitatedFlag = capitatedFlag;
	}

	public String getApr_drg_code() {
		return apr_drg_code;
	}
	public void setApr_drg_code(String apr_drg_code) {
		this.apr_drg_code = apr_drg_code;
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
	public String getPlan_id() {
		return plan_id;
	}
	public void setPlan_id(String plan_id) {
		this.plan_id = plan_id;
	}
	
	
	public void addDiagCode (String s) {
		if (s == null  || s.trim().isEmpty())
		{}
		else
		{
			addMed_codes( "DX", (s == null ? null : s.replace(".", "")), this.getDiag_claim_nomen(), 0);
		}
	}

	
	public void addProcCode (String s) {
		if (s == null  || s.trim().isEmpty())
		{}
		else
		{
			//secondary_proc_code.add( (s == null ? null : s.replace(".", "")) );
			addMed_codes( "PX", (s == null ? null : s.replace(".", "")), this.getProc_claim_nomen(), 0);
		}
	}
	public void addRevCode (String s) {
		if (s == null  || s.trim().isEmpty())
		{}
		else if (s.contains(";")) {
			//log.info("Found rev codes that need to be split");
			for (String sX : s.split(";", 0)) {
				//log.info("Adding rev code: " + sX);
				addMed_codes( "REV", (sX == null ? null : sX.replace(".", "")), "REV", 0);
			}
		}
		else
		{
			//rev_code.add(s);
			addMed_codes( "REV", (s == null ? null : s.replace(".", "")), "REV", 0);
		}
	}
	
	public void addPrinProcCode (String s) {
		if (s == null  || s.trim().isEmpty())
		{}
		else
		{
			//principal_proc_code.add( (s == null ? null : s.replace(".", "")) );
			addMed_codes( "PX", (s == null ? null : s.replace(".", "")), this.getProc_line_level_nomen(), 1);
		}
	}
	public void addPrinProcCodeAndNomen (String s, String n) {
		if (s == null  || s.trim().isEmpty())
		{}
		else
		{
			//principal_proc_code.add( (s == null ? null : s.replace(".", "")) );
			addMed_codes( "PX", (s == null ? null : s.replace(".", "")), n, 1);
		}
	}
	public void addPrinICDProcCode (String s) {
		if (s == null  || s.trim().isEmpty())
		{}
		else
		{
			//principal_proc_code.add( (s == null ? null : s.replace(".", "")) );
			addMed_codes( "PX", (s == null ? null : s.replace(".", "")), this.getProc_claim_nomen(), 1);
		}
	}
	
	public void addECode (String s) {
		if (s == null  || s.trim().isEmpty())
		{}
		else
		{
			ecodes.add(s);
		}
	}
	public void addOther_proc_mod_code (String s) {
		if (s == null  || s.trim().isEmpty())
		{}
		else
		{
			other_proc_mod_code.add(s);
		}
	}
	
	public boolean isEdService() {
		return edService;
	}

	public void setEdService(boolean edService) {
		this.edService = edService;
	}

	public boolean isAssigned() {
		return assigned;
	}
	public void setAssigned(boolean assigned) {
		this.assigned = assigned;
	}
	
	public boolean isValid(ErrorManager errMgr) {
		this.errMgr = errMgr;
		return isValid();
	}
	public int getAssignedCount() {
		return assignedCount;
	}
	public void setAssignedCount(int assignedCount) {
		this.assignedCount = assignedCount;
	}
	
	public String getType_of_bill() {
		return type_of_bill;
	}

	public void setType_of_bill(String type_of_bill) {
		this.type_of_bill = type_of_bill;
	}
	
	
	public String getProvider_npi() {
		return provider_npi;
	}
	public void setProvider_npi(String provider_npi) {
		this.provider_npi = provider_npi;
	}
	/**
	 * don't modify this without also modifying getPrimaryKeyMethod
	 * @return
	 */
	public String getPrimaryKey() {
		return "master_claim_id";
	}
	public Method getPrimaryKeyMethod() {
		Method m = null;
		try {
			m =  this.getClass().getMethod("getMaster_claim_id");
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		} catch (NoSuchMethodError e) {
			e.printStackTrace();
		} catch (SecurityException e) {
			e.printStackTrace();
		}
		return m;
	}
	

	public boolean isValid() {
	
		boolean bResult = true;
		
		// valid source admissions code
		if (this.admission_src_code == null  || this.admission_src_code.isEmpty())
		{}
		else
		if (!LookUpTables.isSrcAdmsValid(this.admission_src_code)) {
			doErrorReporting("svc109", this.admission_src_code, this);
			bResult = false;
		}
		
		// valid facility type
		if (this.facility_type_code == null  || this.facility_type_code.isEmpty())
			this.facility_type_code = "000";
		else
		if (!LookUpTables.isFacilityTypeValid(this.facility_type_code)) {
			doErrorReporting("svc110", this.facility_type_code, this);
			bResult = false;
		}
		
		// valid discharge status
		//if (this.claim_line_type_code.equalsIgnoreCase("OP")  || this.claim_line_type_code.equalsIgnoreCase("PB"))
		if (this.discharge_status_code == null  || this.discharge_status_code.isEmpty())
		{}
		else
		if (!LookUpTables.isDischargeStatusValid(this.discharge_status_code)) {
			doErrorReporting("svc111", this.discharge_status_code, this);
			bResult = false;
		}
		
		// valid place of service code
		if (this.place_of_svc_code == null  || this.place_of_svc_code.isEmpty())
			this.place_of_svc_code = "00";
		else
		if (!LookUpTables.isPlaceOfSvcCodeValid(this.place_of_svc_code)) {
			doErrorReporting("svc112", this.place_of_svc_code, this);
			bResult = false;
		}
		
		// valid line number...
		if(this.claim_line_id==null || this.claim_line_id.isEmpty()) {
			this.claim_line_id="1";
		}
		
		return bResult;
	
	}
	
	
	/**
	 * finalize cloned fields for generic (reflection) access
	 */
	public void finalout () {
		//this.setMaster_claim_id(this.getMember_id() + "_"+ this.getClaim_id() + 
		//		this.getClaim_line_id() == null ? "_000" : "_" + this.getClaim_line_id());
		sbMC.setLength(0);
		sbMC.append(member_id).append('_').append(claim_id).append('_');
		if(claim_line_id == null)
			sbMC.append("000");
		else
			sbMC.append(claim_line_id);
		this.master_claim_id = sbMC.toString();
		
		this.tbl_allowed_amt = allowed_amt;
		this.tbl_begin_date = begin_date;
		this.tbl_claim_line_type_code = claim_line_type_code;
		this.tbl_end_date = end_date;
		this.tbl_master_claim_id = master_claim_id;
		this.tbl_member_id = member_id;
		
	}
	@Transient
	transient private StringBuffer sbMC = new StringBuffer();
	
	
	/**
	 * finalize cloned fields for generic (reflection) access
	 */
	public void finalin () {
				
		// diag codes
		/*
		this.addMed_codes(getPrincipal_diag_code(), "DX", 1);
		for (String s : secondary_diag_code) {
			addMed_codes( (s == null ? null : s.replace(".", "")), "DX", 0);
		}
		// proc codes
		for (String s : principal_proc_code) {
			addMed_codes( (s == null ? null : s.replace(".", "")), "PX", 1);
		}
		for (String s : secondary_proc_code) {
			addMed_codes( (s == null ? null : s.replace(".", "")), "PX", 0);
		}
		// rev codes
		for (String s : rev_code) {
			addMed_codes( (s == null ? null : s.replace(".", "")), "REV", 0);
		}
		*/
		
		tbl_allowed_amt = allowed_amt;
		tbl_begin_date = begin_date;
		tbl_claim_line_type_code = claim_line_type_code;
		tbl_end_date = end_date;
		tbl_master_claim_id = master_claim_id;
		tbl_member_id = member_id;
		
	}
	

	private void doErrorReporting (String id, String errValue, ClaimServLine clm) {
		if(errMgr != null)
			errMgr.issueError(id, errValue, clm);
	}
	
	
	public int hashCode() {
	    StringBuilder builder = new StringBuilder();
	    builder.append(member_id);
	    builder.append(claim_id);
	    return builder.toString().hashCode();
	}
	
	public boolean equals(Object obj) {
	    //null instanceof Object will always return false
	    if (!(obj instanceof ClaimServLine))
	      return false;
	    if (obj == this)
	      return true;
	    if (this.member_id == null) {
	    	if (((ClaimServLine) obj).member_id == null)
	    		return true;
	    	else
	    		return false;
	    }
	    if (this.claim_id == null) {
	    	if (((ClaimServLine) obj).claim_id == null)
	    		return true;
	    	else
	    		return false;
	    }
	    if (this.claim_line_id == null) {
	    	if (((ClaimServLine) obj).claim_line_id == null)
	    		return true;
	    	else
	    		return false;
	    }
	    return  this.member_id.equals( ((ClaimServLine) obj).member_id ) &&
	            this.claim_id.equals( ((ClaimServLine) obj).claim_id ) && 
	            this.claim_line_id.equals( ((ClaimServLine) obj).claim_line_id );
	  }

	
	
	public String toString () {
		String s = "clml";
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
	
	//private static org.apache.log4j.Logger log = Logger.getLogger(ClaimServLine.class);

}
