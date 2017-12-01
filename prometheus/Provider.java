




@Entity
@Table(name = "provider")

@org.hibernate.annotations.GenericGenerator( name="hibernate-native", strategy="native")
/*
@org.hibernate.annotations.GenericGenerator( name="sequence-style-generator",
strategy="org.hibernate.id.enhanced.SequenceStyleGenerator",
	parameters = {
		@Parameter(name = "optimizer", value = "pooled"),
		@Parameter(name = "increment_size", value = "1"),
		@Parameter(name = "initial_value", value = "1000")}
) 
*/

//,
// @Parameter(name = "prefer_sequence_per_entity", value = "true")

public class Provider {
	
	/*
	 * only one per provider, only provider id and specialty 1 are required to be submitted
	 */

	
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
	@Column(name="id", table="provider")
	int id;
	*/
	
	String provider_id;
	@Column(name="npi")
	String NPI;
	@Column(name="dea_no")
	String DEA_no;
	String group_name;
	String medicare_id;
	String provider_name;
	String system_name;
	String tax_id;
	@ElementCollection
	@Transient
	List<String> specialty;
	@ElementCollection
	@Transient
	List<String> provider_taxonomy_codes;
	String zipcode;
	String pilot_name;
	String aco_name;
	String provider_type;
	String provider_attribution_code;
	String facility_id;
	
	//@Transient
	transient ErrorManager errMgr;
	
	
	public Provider () {
		specialty = new ArrayList<String>();
		provider_taxonomy_codes = new ArrayList<String>();
	}
	
	
	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}
	public String getProvider_id() {
		return provider_id;
	}
	public void setProvider_id(String provider_id) {
		this.provider_id = provider_id;
	}
	public String getNPI() {
		return NPI;
	}

	public void setNPI(String NPI) {
		this.NPI = NPI;
	}

	public String getDEA_no() {
		return DEA_no;
	}

	public void setDEA_no(String DEA_no) {
		this.DEA_no = DEA_no;
	}

	public String getMedicare_id() {
		return medicare_id;
	}

	public void setMedicare_id(String medicare_id) {
		this.medicare_id = medicare_id;
	}

	public String getFacility_id() {
		return facility_id;
	}

	public void setFacility_id(String facility_id) {
		this.facility_id = facility_id;
	}

	public String getGroup_name() {
		return group_name;
	}
	public void setGroup_name(String group_name) {
		this.group_name = group_name;
	}
	public String getProvider_name() {
		return provider_name;
	}

	public void setProvider_name(String provider_name) {
		this.provider_name = provider_name;
	}

	public String getSystem_name() {
		return system_name;
	}
	public void setSystem_name(String system_name) {
		this.system_name = system_name;
	}
	public String getTax_id() {
		return tax_id;
	}
	public void setTax_id(String tax_id) {
		this.tax_id = tax_id;
	}
	public List<String> getSpecialty() {
		return specialty;
	}
	public void setSpecialty(List<String> specialty) {
		this.specialty = specialty;
	}
	public List<String> getProvider_taxonomy_codes() {
		return provider_taxonomy_codes;
	}

	public void setProvider_taxonomy_codes(List<String> provider_taxonomy_codes) {
		this.provider_taxonomy_codes = provider_taxonomy_codes;
	}

	public String getZipcode() {
		return zipcode;
	}
	public void setZipcode(String zipcode) {
		this.zipcode = zipcode;
	}
	public String getPilot_name() {
		return pilot_name;
	}
	public void setPilot_name(String pilot_name) {
		this.pilot_name = pilot_name;
	}
	public String getAco_name() {
		return aco_name;
	}
	public void setAco_name(String aco_name) {
		this.aco_name = aco_name;
	}
	
	public String getProvider_type() {
		return provider_type;
	}

	public void setProvider_type(String provider_type) {
		this.provider_type = provider_type;
	}

	public String getProvider_attribution_code() {
		return provider_attribution_code;
	}

	public void setProvider_attribution_code(String provider_attribution_code) {
		this.provider_attribution_code = provider_attribution_code;
	}

	public void addSpecialty (String s) {
		specialty.add(s);
	}
	

	public void addTaxonomyCode (String s) {
		provider_taxonomy_codes.add(s);
	}
	
	
	public boolean isValid(ErrorManager errMgr) {
		this.errMgr = errMgr;
		return isValid();
	}
	
	
	
	public boolean isValid() {

		boolean bResult = true;
		
		// valid specialty codes
		for (String s : this.specialty)
			if (!LookUpTables.isProviderSpecialtyValid(s)){
				doErrorReporting("prv101", s, this);
				bResult = false;
			}
		
		return bResult;
	
	}
	

	private void doErrorReporting (String id, String errValue, Provider prv) {
		if(errMgr != null)
			errMgr.issueError(id, errValue, prv);
	}
	
	
	
	public String toString () {
		String s = "prov";
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
