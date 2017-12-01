



public abstract class AllDataAbstract {
	
	
	List<ClaimServLine> svcLines;
	Map<String, List<Enrollment>> enrollments;
	Map<String, PlanMember> members;
	Map<String, Provider> providers;
	List<ClaimRx> rxClaims;

	void initialize () {
		
		if( (parameters.get("validate")  != null)  &&  parameters.get("validate").equalsIgnoreCase("yes")  )  {
			if (errMgr == null)
				errMgr = new ErrorManager((AllDataInterface) this);
		}
		
		boolean bOK = true;
		
		if ( parameters.get("provider_file") == null) {
			log.error("No provider_file parameter specified.  Can not continue");
			bOK = false;
		}
		
		if ( checkClaimsParametersOK() ) {
			log.error("No claim files parameters were specified.  Can not continue");
			bOK = false;
		}
		
		if ( checkEnrollParametersOK() ) {
			log.error("No enroll_file parameter specified.  Can not continue");
			bOK = false;
		}
		
		if ( parameters.get("member_file") == null) {
			log.error("No member_file parameter specified.  Can not continue");
			bOK = false;
		}
		
		if (!bOK)
			throw new IllegalStateException("some required parameters missing");
		
		
		log.info("Obtaining Provider Data");			// claims may have dependencies on providers
		providers = getAllProviders();
		log.info("Provider Data Obtained");
		
		
		log.info("Obtaining Claim Data");
		svcLines = getAllServiceClaims();
		log.info("Claim Data Obtained");
		
		log.info("Obtaining Enrollment Data");
		enrollments = getAllEnrollments();
		log.info("Enrollment Data Obtained");
		
		log.info("Obtaining Plan Member Data");
		members = getAllPlanMembers();
		log.info("Plan Member Data Obtained");
		
		log.info("Obtaining Rx Claims Data");
		if ( parameters.get("claim_rx_file") == null) {
			log.warn("No claim_rx_file parameter specified.  Rx claims will not be considered in this run");
			rxClaims = new ArrayList<ClaimRx>();
		}
		else {
			rxClaims = getAllRxClaims();
			log.info(" Rx Claims Data Obtained");
		}
		
	}
	
	// this default assumes a separate enrollment file is not needed (only legacy csv needs it as of this writing)
	boolean checkEnrollParametersOK() {
		return false;
	}

	boolean checkClaimsParametersOK() {
		throw new IllegalStateException("Must always override");
	}

	List<ClaimRx> getAllRxClaims() {
		throw new IllegalStateException("Must always override");
	}

	Map<String, Provider> getAllProviders() {
		throw new IllegalStateException("Must always override");
	}

	Map<String, PlanMember> getAllPlanMembers() {
		throw new IllegalStateException("Must always override");
	}

	Map<String, List<Enrollment>> getAllEnrollments() {
		throw new IllegalStateException("Must always override");
	}

	List<ClaimServLine> getAllServiceClaims() {
		throw new IllegalStateException("Must always override");
	}
	

	
	public ErrorManager getErrorManager () {
		return this.errMgr;
	}
	
	public void setErrorManager(ErrorManager errMgr) {
		this.errMgr = errMgr;
	}
	ErrorManager errMgr;
	
	
	HashMap<String, String> parameters = new HashMap<String, String>();
	

	private static org.apache.log4j.Logger log = Logger.getLogger(AllDataAbstract.class);

}
