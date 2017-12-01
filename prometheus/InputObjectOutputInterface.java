


public interface InputObjectOutputInterface {
	
	
	public boolean writeMedicalClaims (Collection<ClaimServLine> itmc);
	
	public boolean writeRxClaims (Collection<ClaimRx> itrx);
	
	public boolean writeMembers (Collection<PlanMember> itmb);
	
	public boolean writeEnrollments (Collection<List<Enrollment>> itme);
	
	public boolean writeProviders (Collection<Provider> itpv);
	

}
