



public interface AllDataInterface {
	
	
	public List<ClaimServLine> getAllServiceClaims();
	public ClaimInputCounts getAllServiceClaimCounts();
	public List<String> getAllServiceClaimErrors();
	public InputStatistics getServiceClaimStatistics();
	
	public List<ClaimRx> getAllRxClaims();
	public ClaimRxInputCounts getAllRxClaimCounts();
	public List<String> getAllRxClaimErrors();
	public InputStatistics getRxClaimStatistics();
	
	public Map<String, List<Enrollment>> getAllEnrollments();
	public EnrollmentInputCounts getAllEnrollmentCounts();
	public List<String> getAllEnrollmentErrors();
	public InputStatistics getEnrollmentStatistics();
	
	public Map<String, PlanMember> getAllPlanMembers();
	public MemberInputCounts getAllPlanMemberCounts();
	public List<String> getAllPlanMemberErrors();
	public InputStatistics getMemberStatistics();
	
	public Map<String, Provider> getAllProviders();
	public ProviderInputCounts getAllProviderCounts();
	public List<String> getAllProviderErrors();
	public InputStatistics getProviderStatistics();
	
	public void setErrorManager(ErrorManager errMgr);

}
