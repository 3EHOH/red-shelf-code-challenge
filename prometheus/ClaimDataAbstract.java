




public abstract class ClaimDataAbstract extends CommonInputDataAbstract {
	
	
	protected List<ClaimServLine> svcLines = new ArrayList<ClaimServLine>();
	protected ClaimServLine svcLine;
	protected ClaimInputCounts counts = new ClaimInputCounts();
	
	

	public List<ClaimServLine> getAllServiceLines() {
		if (svcLines.isEmpty())
			prepareAllServiceLines();
		return svcLines;
	}
	
	

	
	protected void prepareAllServiceLines () {
		
		if (sourceFiles.isEmpty())
			throw new IllegalStateException("No source files - must invoke addSourceFile prior to invoking this method");
		
		
		
		// get each file in the source file list
		for (String sFile : sourceFiles) {
			
			InputManager inputFile = new InputManager(sFile);
			try {
				log.info("Opening " + sFile);
				inputFile.setPreserveQuotes(bPreserveQuotes);
				inputFile.openFile();
			} catch (IOException | ZipException e) {
				log.error("An error occurred while opening " + sFile + " - " + e);
			}
			
			String line;//, col_value;
			int col_ix = 0;
			boolean bStart = false;
			boolean bAcceptThisClaim = false;
			col_index = new HashMap<Integer, ColumnFinder>();
			
			// read the lines from each file
			try {
				//while ((line = br.readLine()) != null)  {
				while ((line = inputFile.readFile()) != null)  {
					// check for a header in the first line  
					if (!bStart) {
						findDelimiter(line);
						bStart = checkForHeader(line);
					}
					// map all data lines
					if (bStart) {
						counts.claimRecordsRead++;
						//in = line.split(sDelimiter);
						in = splitter(line);
						//log.info("Found line with " + in.length + " tokens");
						col_ix = 0;
						getServiceLine();
						// map each column to the service line object
						bAcceptThisClaim = true;
						// override to take records completely out of the process
						if (filterPass() ) {
							for (String col_value : in)  
							{  
								if (col_value != null)
									col_value = col_value.trim();
								bAcceptThisClaim = bAcceptThisClaim && doMap(col_ix, col_value);
								col_ix++;
							}
							if (errMgr != null)
								bAcceptThisClaim = bAcceptThisClaim && svcLine.isValid(errMgr);
							}
						else
							bAcceptThisClaim = false;
						if (bAcceptThisClaim)
						{
							doPostProcess();
							if (svcLine.getClaim_line_type_code() != null )
								doRollUp();		// override when totals need to be calculated
							else
								claimStore();
								//svcLines.add(svcLine);
							counts.claimRecordsAccepted++;
						}
						else
							counts.claimRecordsRejected++;
					}
					else
						bStart = true;
					
					if(counts.claimRecordsRead % 10000 == 0) { 
						log.info("executing " + counts.claimRecordsRead + " ==> " + new Date());
					}
				
				}
				inputFile.closeFile();
				log.info("Found " + counts.claimRecordsRead + " records in " + sFile);
				//br.close();
			} catch (IOException e) {
				throw new IllegalStateException("An error occurred while reading " + sFile);
			}  
			
		}
		
		// all source has been read in and loaded into svcLines List
		// let's sort
		log.info("Starting sort of all service lines");
		
		Collections.sort(svcLines,new slCompare());
		
		log.info("Completed sort of all service lines");
		
		log.info("Starting Medical Claim Post-processing");
		
		
		
	}
	
	protected boolean bPreserveQuotes = true;
	
	
	protected void claimStore () {
		svcLines.add(svcLine);
	}
	

	

	protected boolean filterPass() {
		return true;
	}


	protected void doPostProcess() {
		
		if (svcLine.getClaim_line_type_code().equals("IP")) {
			if (svcLine.getAdmission_date() == null  &&
					svcLine.getBegin_date() != null)
				svcLine.setAdmission_date(svcLine.getBegin_date());
			if (svcLine.getDischarge_date() == null  &&
					svcLine.getEnd_date() != null)
				svcLine.setDischarge_date(svcLine.getEnd_date());
		}
		
		setAttributionProvider();
		
	}	
	

	/**
	 * default provider attribution
	 * @param prov_id
	 */
	protected void setAttributionProvider() {
		if(svcLine.getClaim_line_type_code().equals("PB") )
			svcLine.setPhysician_id(svcLine.getProvider_npi());
		else
			svcLine.setFacility_id(svcLine.getProvider_npi());
	}

	
	/**
	 * default roll-up
	 * For IP, combine all lines into one service line.
	 * For OP and PB, combine all line adjustments into one service line: 
	 * get earliest start date and latest end date
	 * add all amounts
	 * concatenate all lists
	 */
	protected void doRollUp() {
		
		if (svcLine.getClaim_line_type_code().equals("IP")) {

			idx = ipSvcLines.get(svcLine.hashCode());
			if ( idx != null ) {
			
				ClaimServLine cTarget = svcLines.get(idx);
				cTarget.setClaim_line_id("00");
				combineLines(cTarget);
			
			}
			else
			{
				ipSvcLines.put(svcLine.hashCode(), svcLines.size());
				svcLines.add(svcLine);
			}
			
		}
		else {
			
			idx = nonIpSvcLines.get(hashSL());
			if ( idx != null ) {
				ClaimServLine cTarget = svcLines.get(idx);
				combineLines(cTarget);
			}
			else
			{
				nonIpSvcLines.put(hashSL(), svcLines.size());
				svcLines.add(svcLine);
			}
			
		}
		
	}
	
	Integer idx;
	private HashMap <Integer, Integer> ipSvcLines = new HashMap<Integer, Integer>();
	private HashMap <Integer, Integer> nonIpSvcLines = new HashMap<Integer, Integer>();
	
	
	private int hashSL() {
	    StringBuilder builder = new StringBuilder();
	    builder.append(svcLine.getMember_id());
	    builder.append(svcLine.getClaim_id());
	    builder.append(svcLine.getClaim_line_id());
	    return builder.toString().hashCode();
	}
	
	
	
	private void combineLines (ClaimServLine cTarget) {
		
		cTarget.setAllowed_amt(cTarget.getAllowed_amt() + svcLine.getAllowed_amt());
		cTarget.setProxy_allowed_amt(cTarget.getProxy_allowed_amt() + svcLine.getProxy_allowed_amt());
		cTarget.setReal_allowed_amt(cTarget.getReal_allowed_amt() + svcLine.getReal_allowed_amt());
		cTarget.setCharge_amt(cTarget.getCharge_amt() + svcLine.getCharge_amt());
		cTarget.setCoinsurance_amt(cTarget.getCoinsurance_amt() + svcLine.getCoinsurance_amt());
		cTarget.setCopay_amt(cTarget.getCopay_amt() + svcLine.getCopay_amt());
		cTarget.setDeductible_amt(cTarget.getDeductible_amt() + svcLine.getDeductible_amt());
		cTarget.setPaid_amt(cTarget.getPaid_amt() + svcLine.getPaid_amt());
		cTarget.setPrepaid_amt(cTarget.getPrepaid_amt() + svcLine.getPrepaid_amt());
		cTarget.setQuantity(cTarget.getQuantity() + svcLine.getQuantity());
		cTarget.setStandard_payment_amt(cTarget.getStandard_payment_amt() + svcLine.getStandard_payment_amt());
		
		if (cTarget.getBegin_date() == null) {
			cTarget.setBegin_date(svcLine.getBegin_date());
		}
		else {
			if (svcLine.getBegin_date() != null  &&
				svcLine.getBegin_date().before(cTarget.getBegin_date()))
					cTarget.setBegin_date(svcLine.getBegin_date());
		}
		
		if (cTarget.getEnd_date() == null) {
			cTarget.setEnd_date(svcLine.getEnd_date());
		}
		else {
			if (svcLine.getEnd_date() != null  &&
				svcLine.getEnd_date().after(cTarget.getEnd_date()))
					cTarget.setEnd_date(svcLine.getEnd_date());
		}	
		
		if (cTarget.getAdmission_date() == null) {
			cTarget.setAdmission_date(svcLine.getAdmission_date());
		}
		else {
			if (svcLine.getAdmission_date() != null  &&
				svcLine.getAdmission_date().before(cTarget.getAdmission_date()))
					cTarget.setAdmission_date(svcLine.getAdmission_date());
		}	
		
		if (cTarget.getDischarge_date() == null) {
			cTarget.setDischarge_date(svcLine.getDischarge_date());
		}
		else {
			if (svcLine.getDischarge_date() != null  &&
				svcLine.getDischarge_date().after(cTarget.getDischarge_date()))
					cTarget.setDischarge_date(svcLine.getDischarge_date());
		}	
		for(MedCode _mc : svcLine.getPrincipal_proc_code_objects()) {
			String s = _mc.getCode_value().replace(".", "");
			if(!cTarget.getPrincipal_proc_code().contains(s))
				cTarget.addPrinProcCodeAndNomen(_mc.getCode_value(), _mc.getNomen());
				
		}
		for(String s : svcLine.getRev_code()) {
			if(!cTarget.getRev_code().contains(s))
				cTarget.addRevCode(s);
		}
		for(String s : svcLine.getSecondary_diag_code()) {
			s = s.replace(".", "");
			if(!cTarget.getSecondary_diag_code().contains(s))
				cTarget.addDiagCode(s);
		}
		for(String s : svcLine.getSecondary_proc_code()) {
			s = s.replace(".", "");
			if(!cTarget.getSecondary_proc_code().contains(s))
				cTarget.addProcCode(s);
				
		}
		
	}
	
	
	
	/**
	 * new service line method allows override to do a look up of previously existing
	 */
	protected void getServiceLine () {
		svcLine = new ClaimServLine();
	}


	protected boolean doMap(int col_ix, String col_value) {
		throw new IllegalStateException("always override this method");
	}


	protected boolean checkForHeader(String line) {
		throw new IllegalStateException("always override this method");
	}

	
	
	public static final String PROF_FILE = "C:\\input\\UAHN_PROF_UCA.txt";
	public static final String STAY_FILE = "C:\\input\\UAHN_STAY_UCA.txt";
	

	
	private static org.apache.log4j.Logger log = Logger.getLogger(ClaimDataAbstract.class);
	
	
	class slCompare implements Comparator<ClaimServLine> {

	    @Override
	    public int compare(ClaimServLine o1, ClaimServLine o2) {
	    	int c;
	    	
	    	/*if (o1.getMember_id() == null)
	    		c = -1;
	    	else if (o2.getMember_id() == null)
	    		c = 1;
	    	else {*/
	    	
	    	
	    	if (o1.getMember_id() == null)
	    		o1.setMember_id("");
	    	if (o2.getMember_id() == null)
	    		o2.setMember_id("");
	    	
	    	if (o1.getBegin_date() == null)
	    		o1.setBegin_date(new Date(0));
	    	if (o2.getBegin_date() == null)
	    		o2.setBegin_date(new Date(0));
	    		
	    	
	    		c = o1.getMember_id().compareTo(o2.getMember_id());
	    		if (c == 0) {
	    			
	    			/*
	    			if (o1.getBegin_date() == null)
	    				c = -1;
	    			else if (o2.getBegin_date() == null)
	    				c = 1;
	    			else
	    			*/
	    			
	    				c = o1.getBegin_date().compareTo(o2.getBegin_date());
	    		}
	    		
	    	//}
	        return c;
	    }
	}

	
	
	protected void doErrorReporting (String id, String errValue, ClaimServLine clm) {
		if(errMgr != null)
			errMgr.issueError(id, errValue, clm);
	}

}
