




@Entity
@Table(name="enrollment", indexes = {
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

public class Enrollment {
	
	/*
	 * Can have MORE than one record per member:
	 * a) if more than one enrollment with non-contiguous time periods
	 * b) if more than one product
	 */
	
	@Id
	@GeneratedValue(generator="hibernate-native")
	//@GeneratedValue(generator = "sequence-style-generator")
	@Basic(optional = false)
	@Column(name="id")
	private long id;

	String member_id;
	Date begin_date;
	Date end_date;
	Integer age_at_enrollment;
	String insurance_product;
	String coverage_type;
	
	boolean isGap;
	
	@Transient
	ErrorManager errMgr;
	
	public Enrollment() {
		isGap = false;
	}
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
	public Date getBegin_date() {
		return begin_date;
	}
	public void setBegin_date(Date begin_date) {
		this.begin_date = begin_date;
	}
	
	public void setBeginDateFromYYYYMM (String YYYYMM) {
		try {
			begin_date = sdf_yyyyMMdd.parse(YYYYMM + "01");
		} catch (ParseException e) {
			log.error("Failure parsing YYYYMM begin date from: " + YYYYMM);
		    e.printStackTrace();
		    throw new IllegalStateException("Failure parsing YYYYMM begin date from: " + YYYYMM);
		}
	}
	@Transient
	private transient static final SimpleDateFormat sdf_yyyyMMdd = new SimpleDateFormat("yyyyMMdd");
	
	public Date getEnd_date() {
		return end_date;
	}
	public void setEnd_date(Date end_date) {
		this.end_date = end_date;
	}
	
	public void setEndDateFromYYYY (String YYYY) {
		try {
			end_date = sdf_yyyyMMdd.parse(YYYY + "1231");
		} catch (ParseException e) {
			log.error("Failure parsing YYYY end date from: " + YYYY);
		    e.printStackTrace();
		    throw new IllegalStateException("Failure parsing YYYY begin date from: " + YYYY);
		}
	}
	
	public void setEndDateFromYYYYMM (String YYYYMM) {
		try {
			Date _end_date = sdf_yyyyMMdd.parse(YYYYMM + "01");
			_cWrk.setTime(_end_date);
			_cWrk.add(Calendar.MONTH, 1);
			_cWrk.add(Calendar.DATE, -1);
			end_date = _cWrk.getTime();
		} catch (ParseException e) {
			log.error("Failure parsing YYYY end date from: " + YYYYMM);
		    e.printStackTrace();
		    throw new IllegalStateException("Failure parsing YYYY end date from: " + YYYYMM);
		}
	}
	
	@Transient
	private transient Calendar _cWrk = Calendar.getInstance();


	
	public Integer getAge_at_enrollment() {
		return age_at_enrollment;
	}
	public void setAge_at_enrollment(Integer age_at_enrollment) {
		this.age_at_enrollment = age_at_enrollment;
	}
	public String getInsurance_product() {
		return insurance_product;
	}
	public void setInsurance_product(String insurance_product) {
		this.insurance_product = insurance_product;
	}
	public String getCoverage_type() {
		return coverage_type;
	}
	public void setCoverage_type(String coverage_type) {
		this.coverage_type = coverage_type;
	}
	public boolean isGap() {
		return isGap;
	}
	public void setGap(boolean isGap) {
		this.isGap = isGap;
	}
	public boolean isValid(ErrorManager errMgr) {
		this.errMgr = errMgr;
		return isValid();
	}
	
	public boolean isValid() {

		boolean bResult = true;
		
		return bResult;
	
	}
	
	
	
	public String toString () {
		String s = "enrl";
		String sx;
		Date dx;
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
				else if (f[i].getType().equals(Date.class)) {
					dx = (Date) f[i].get(this);
					s = s + "," + f[i].getName() + "=" + dx;
				}	
			} 
			catch (IllegalArgumentException | IllegalAccessException e) {
				// do nothing
			}
		}
		return s;
	}
	

	private static org.apache.log4j.Logger log = Logger.getLogger(Enrollment.class);
	
}
