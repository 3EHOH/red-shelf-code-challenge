



public class InputObjectOutputCSV implements InputObjectOutputInterface  {

	
	private GenericOutputInterface oi;
	
	
	@Override
	public boolean writeMedicalClaims(Collection<ClaimServLine> itmc) {
		
		log.info("Medical Claim Data Output Starting");
		
		oi = new GenericOutputCSV();

		for (ClaimServLine svcLine : itmc) { 
			//log.info("Write ready");
			oi.write(svcLine);
			//log.info("Write done");
		}
		oi.close();
		log.info("Medical Claim Data Output Completed");
		return true;
		
	}

	@Override
	public boolean writeRxClaims(Collection<ClaimRx> itrx) {
		
		if (itrx.isEmpty()) {
			log.info("No Rx data to process");
			return true;
		}
		
		log.info("Rx Claim Data Output Starting");
		
		oi = new GenericOutputCSV();
		
		for (ClaimRx rxLine : itrx) { 
			oi.write(rxLine);
		}
		oi.close();
		log.info("Rx Claim Data Output Completed");
		
		return true;
	}

	@Override
	public boolean writeMembers(Collection<PlanMember> itmb) {

		log.info("Member Data Output Starting");

		oi = new GenericOutputCSV();

		for (PlanMember member : itmb) { 
			oi.write(member);
		}
		oi.close();
		log.info("Member Data Output Completed");
		
		return true;
	}

	@Override
	public boolean writeEnrollments(Collection<List<Enrollment>> itme) {

		log.info("Enrollment Data Output Starting");
		
		oi = new GenericOutputCSV();
		
		for (List<Enrollment> enrollments : itme) { 
			for (Enrollment enrollment : enrollments) {
				oi.write(enrollment);
			}
		}
		oi.close();
		log.info("Enrollment Data Output Completed");
		
		return true;
	}

	@Override
	public boolean writeProviders(Collection<Provider> itpv) {

		log.info("Provider Data Output Starting");
		oi = new GenericOutputCSV();

		for (Provider provider : itpv) { 
			oi.write(provider);
		}
		oi.close();
		log.info("Provider Data Output Completed");

		return true;
	}
	

	private static org.apache.log4j.Logger log = Logger.getLogger(InputObjectOutputCSV.class);


}
