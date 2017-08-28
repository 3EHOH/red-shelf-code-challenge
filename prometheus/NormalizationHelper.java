




public class NormalizationHelper {
	
	
	public static ArrayList<ClaimServLine> xg_highSequenceRollUpMd (ArrayList<ClaimServLine> lines)  {
		
		ArrayList<ClaimServLine> oLines = new ArrayList<ClaimServLine>();
		HashMap<Integer, ClaimServLine> chkMap = new HashMap<Integer, ClaimServLine>();
		ClaimServLine cTarget;
		Integer idx;
		StringBuilder builder;
		
		for (ClaimServLine c : lines) {
			
			builder = new StringBuilder();
		    builder.append(c.getMember_id());
		    builder.append(c.getClaim_id());
		    builder.append(c.getClaim_line_id());
		    builder.append(c.getOrig_adj_rev());
		    idx = builder.toString().hashCode();
		    
		    cTarget = chkMap.get(idx);
			if ( cTarget == null ) {
				chkMap.put(idx, c);
			}
			else {
				try {
					if (c.getSequence_key().compareTo(cTarget.getSequence_key()) > 0 )
						cTarget = c;
					}
				catch (IndexOutOfBoundsException e) {
					log.error("Hash error: m-" + c.getMember_id() + " c-" + c.getClaim_id() + 
							" l-" + c.getClaim_line_id() + " s-" + c.getOrig_adj_rev());
				}
			}
			
		}
		
		for (ClaimServLine c : chkMap.values()) {
			oLines.add(c);
		}
		
		return oLines;
		
	}
	
	
	
	
	public static ArrayList<ClaimServLine> MD_APCD_ClaimLineNumberGenerator_md (ArrayList<ClaimServLine> lines)  {
		
		ArrayList<ClaimServLine> oLines = new ArrayList<ClaimServLine>();
		HashMap<String, Integer> chkMap = new HashMap<String, Integer>();
		Integer idx;
		
		for (ClaimServLine c : lines) {
			
			//log.info("MD roll up:" + c);
			
			// if a claim id line number already exists, leave as is
			if(c.getClaim_line_id() == null  ||  c.getClaim_line_id().trim().length() == 0) {
				
				if (c.getClaim_id() == null) {
					log.error("No Claim Id - must halt: " + c);
					throw new IllegalStateException("Trying to normalize MD_APCD claim with no claim id");
				}
				
				idx = 1;
				
				if (chkMap.containsKey(c.getClaim_id())) {
					idx = chkMap.get(c.getClaim_id());
					idx++;
				}
					
				chkMap.put(c.getClaim_id(), new Integer(idx));
				
				c.setClaim_line_id(MathUtil.lPadZero(idx, 0));	
				
			}
			
			oLines.add(c);
		}
		
		
		
		return oLines;
		
	}
	
	
	
	public static ArrayList<ClaimServLine> claimLineSequenceNumberGenerator (ArrayList<ClaimServLine> lines)  {
		
		ArrayList<ClaimServLine> oLines = new ArrayList<ClaimServLine>();
		HashMap<String, Integer> chkMap = new HashMap<String, Integer>();
		Integer idx;
		
		//log.info("* " + lines.size() + "incoming to claimLineSequenceNumberGenerator");
		
		for (ClaimServLine c : lines) {
			
			
			
			// if a claim id line number already exists, leave as is
			if(c.getClaim_line_id() == null  ||  c.getClaim_line_id().trim().length() == 0) {
					log.error("No Claim Line Id - must halt: " + c);
					throw new IllegalStateException("Trying to perform directive 'addSequenceNumber' with no claim line number - must halt");
			}
				
			else {	
				idx = 1;
				
				if (chkMap.containsKey(c.getClaim_id() + c.getClaim_line_id())) {
					//log.info("* found " + c.getClaim_id() + c.getClaim_line_id() );
					idx = chkMap.get(c.getClaim_id() + c.getClaim_line_id());
					idx++;
				}
					
				chkMap.put(c.getClaim_id() + c.getClaim_line_id(), new Integer(idx));
				
				//log.info("* idx is now " + idx);
				//log.info("* setting sequence of " + MathUtil.lPadZero(idx, 0));
				
				c.setSequence_key(MathUtil.lPadZero(idx, 0));	
				
			}
			
			oLines.add(c);
		}
		
		
		
		return oLines;
		
	}
	
	
	
	private static org.apache.log4j.Logger log = Logger.getLogger(NormalizationHelper.class);
	

}
