



public class AllDataCSV extends AllDataAbstract implements AllDataInterface {

	public AllDataCSV(HashMap<String, String> parameters) {
		this.parameters = parameters;
		super.initialize();
	}
	


	// service claim references
	private ClaimDataInterface cdIf = null;	
	@Override
	public List<ClaimServLine> getAllServiceClaims() {
		if(super.svcLines != null)
			return super.svcLines;
		try {
			if (cdIf == null) {
				if (errMgr == null)
					cdIf = new ClaimDataFromCSV();
				else
					cdIf = new ClaimDataFromCSV(errMgr);
				cdIf.addSourceFile(parameters.get("prof_file"));
				cdIf.addSourceFile(parameters.get("stay_file"));
			}
			return cdIf.getAllServiceLines ();
		} catch (Throwable e) {
			e.printStackTrace();
			log.error("An error occurred trying to obtain claim data: " + e);
			throw new IllegalStateException("An error occurred trying to obtain claim data: " + e);
		}
	}
	@Override
	public ClaimInputCounts getAllServiceClaimCounts() {
		return cdIf.getCounts();
	}
	@Override
	public List<String> getAllServiceClaimErrors() {
		return cdIf.getErrorMsgs();
	}
	@Override
	public InputStatistics getServiceClaimStatistics() {
		return cdIf.getInputStatistics();
	}
	@Override
	boolean checkClaimsParametersOK() {
		return (parameters.get("prof_file") == null)  &&  (parameters.get("stay_file") == null);
	}
	
	
	private EnrollmentDataInterface enIf = null;
	@Override
	public Map<String, List<Enrollment>> getAllEnrollments() {
		if(super.enrollments != null)
			return super.enrollments;
		try {
			if (enIf == null) {
				if (errMgr == null)
					enIf = new EnrollmentDataFromCSV();
				else
					enIf = new EnrollmentDataFromCSV(errMgr);
				enIf.addSourceFile(parameters.get("enroll_file"));
			}
			return enIf.getAllEnrollments();
		} catch (Throwable e) {
			e.printStackTrace();
			log.error("An error occurred trying to obtain enrollment data: " + e);
			throw new IllegalStateException("An error occurred trying to obtain enrollment data: " + e);
		}
	}
	@Override
	public EnrollmentInputCounts getAllEnrollmentCounts() {
		return enIf.getCounts();
	}
	@Override
	public List<String> getAllEnrollmentErrors() {
		return enIf.getErrorMsgs();
	}
	@Override
	public InputStatistics getEnrollmentStatistics() {
		return enIf.getInputStatistics();
	}
	@Override
	boolean checkEnrollParametersOK() {
		return (parameters.get("enroll_file") == null);
	}
	
	
	private PlanMemberDataInterface pmIf = null;
	@Override
	public Map<String, PlanMember> getAllPlanMembers() {
		if (super.members != null)
			return super.members;
		try {
			if (pmIf == null) {
				if (errMgr == null)
					pmIf = new PlanMemberDataFromCSV();
				else
					pmIf = new PlanMemberDataFromCSV(errMgr);
				pmIf.addSourceFile(parameters.get("member_file"));
			}
			return pmIf.getAllMembers();
		} catch (Throwable e) {
			e.printStackTrace();
			log.error("An error occurred trying to obtain plan member data: " + e);
			throw new IllegalStateException("An error occurred trying to obtain plan member data: " + e);
		}
	}
	@Override
	public MemberInputCounts getAllPlanMemberCounts() {
		return pmIf.getCounts();
	}
	@Override
	public List<String> getAllPlanMemberErrors() {
		return pmIf.getErrorMsgs();
	}
	@Override
	public InputStatistics getMemberStatistics() {
		return pmIf.getInputStatistics();
	}
	
	
	private ClaimRxDataInterface rxIf = null;
	@Override
	public List<ClaimRx> getAllRxClaims() {
		if(super.rxClaims != null)
			return super.rxClaims;
		try {
			if(rxIf == null) {
				if (errMgr == null)
					rxIf = new ClaimRxDataFromCSV();
				else
					rxIf = new ClaimRxDataFromCSV(errMgr);
				rxIf.addSourceFile(parameters.get("claim_rx_file"));
			}
			return rxIf.getAllRxs();
		} catch (Throwable e) {
			e.printStackTrace();
			log.error("An error occurred trying to obtain Rx claim data: " + e);
			throw new IllegalStateException("An error occurred trying to obtain Rx claim data: " + e);
		}
	}
	@Override
	public ClaimRxInputCounts getAllRxClaimCounts() {
		return rxIf.getCounts();
	}
	@Override
	public List<String> getAllRxClaimErrors() {
		return rxIf.getErrorMsgs();
	}
	@Override
	public InputStatistics getRxClaimStatistics() {
		return rxIf.getInputStatistics();
	}
	
	
	private ProviderDataInterface pvIf = null;
	@Override
	public Map<String, Provider> getAllProviders() {
		if(super.providers != null)
			return super.providers;
		try {
			if (pvIf == null) {
				if (errMgr == null)
					pvIf = new ProviderDataFromCSV();
				else
					pvIf = new ProviderDataFromCSV(errMgr);
				pvIf.addSourceFile(parameters.get("provider_file"));
			}
			return pvIf.getAllProviders();
		} catch (Throwable e) {
			e.printStackTrace();
			log.error("An error occurred trying to obtain provider data: " + e);
			throw new IllegalStateException("An error occurred trying to obtain provider data: " + e);
		}
	}
	@Override
	public ProviderInputCounts getAllProviderCounts() {
		return pvIf.getCounts();
	}
	@Override
	public List<String> getAllProviderErrors() {
		return pvIf.getErrorMsgs();
	}
	@Override
	public InputStatistics getProviderStatistics() {
		return pvIf.getInputStatistics();
	}
	
	
	private static org.apache.log4j.Logger log = Logger.getLogger(AllDataCSV.class);
	
	
}