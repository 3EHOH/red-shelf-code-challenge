




public class InputObjectOutputSQL implements InputObjectOutputInterface {
	
	
	private GenericOutputInterface oi;
	
	

	HashMap<String, String> parameters;
    
	public InputObjectOutputSQL () {
		this (RunParameters.parameters);
	}
	
	public InputObjectOutputSQL (HashMap<String, String> parameters) {
		this.parameters = parameters;
	}
	
	
	
	@Override
	public boolean writeMedicalClaims(Collection<ClaimServLine> itmc) {
		
		log.info("Medical Claim Data Output Starting");
		log.info(itmc.size() + " items to process");
		
		// look for a getPrimaryKey method in the object
		ClaimServLine c = itmc.iterator().next();
		mKeyGetter = c.getPrimaryKeyMethod();
		String lastKey = getMedicalClaimKey(c);
		
		
		List<ClaimServLine> list = new ArrayList<ClaimServLine>(itmc);
		Collections.sort(list,new keyCompare());
		
		oi = new GenericOutputSQL(parameters);
		
		boolean bS = false;
		
		int iAppend = 0;
		for (ClaimServLine svcLine : list) { 
			if ( ! getMedicalClaimKey(svcLine).equals(lastKey) ) {
				iAppend = 0;
				lastKey = getMedicalClaimKey(svcLine);
			}
			iAppend++;
			svcLine.setMaster_claim_id(svcLine.getMaster_claim_id() + "_" + String.format("%04d", iAppend));
			if (!bS)  {
				log.info("Write attempt");
			}
			oi.write(svcLine);
			if (!bS)  {
				log.info("Write complete");
				bS = true;
			}
		}
		oi.close();
		log.info("Medical Claim Data Output Completed");
		return true;
		
	}
	
	Method mKeyGetter;
	
	private String getMedicalClaimKey (ClaimServLine c) {
		String s = null;
		try {
			s = (String)mKeyGetter.invoke(c);
		} catch (IllegalAccessException | IllegalArgumentException
				| InvocationTargetException e) {
			e.printStackTrace();
		}
		return s;
	}
	
	

	@Override
	public boolean writeRxClaims(Collection<ClaimRx> itrx) {
		
		log.info("Rx Claim Data Output Starting");
		
		oi = new GenericOutputSQL(parameters);
		
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

		oi = new GenericOutputSQL(parameters);

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
		
		oi = new GenericOutputSQL(parameters);
		
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
		oi = new GenericOutputSQL(parameters);

		for (Provider provider : itpv) { 
			oi.write(provider);
		}
		oi.close();
		log.info("Provider Data Output Completed");

		return true;
	}
	

	private static org.apache.log4j.Logger log = Logger.getLogger(InputObjectOutputSQL.class);
	
	
	
	class keyCompare implements Comparator<Object> {

	    @Override
	    public int compare(Object o1, Object o2) {
	    	
	    	if (mKeyGetter == null) {
	    		throw new IllegalStateException("trying to compare with no compare method");
	    	}
	    	//mKey.invoke(obj)
	    	int c = 0;
	    	try {
	    		if (mKeyGetter.invoke(o1) == null)
	    			c = -1;
	    		else if (mKeyGetter.invoke(o2) == null)
	    			c = 1;
	    		else {
					c = ((String)mKeyGetter.invoke(o1)).compareTo((String) mKeyGetter.invoke(o2));
	    		}
	    	} catch (IllegalAccessException e) {
				e.printStackTrace();
			} catch (IllegalArgumentException e) {
				e.printStackTrace();
			} catch (InvocationTargetException e) {
				e.printStackTrace();
			}
	    	
	        return c;
	    }
	    
	    
	    
	}


}
