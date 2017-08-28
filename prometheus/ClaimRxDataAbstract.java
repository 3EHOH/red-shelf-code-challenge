





public abstract class ClaimRxDataAbstract extends CommonInputDataAbstract {
	
	protected List<ClaimRx> Rxs = new ArrayList<ClaimRx>();
	protected ClaimRx Rx;
	protected ClaimRxInputCounts counts = new ClaimRxInputCounts();
	
	
	

	public List<ClaimRx> getAllRxs() {
		if (Rxs.isEmpty())
			prepareAllRxs();
		return Rxs;
	}
	
	
	protected void prepareAllRxs () {
		
		if (sourceFiles.isEmpty())
			throw new IllegalStateException("No source files - must invoke addSourceFile prior to invoking this method");
		
		// get each file in the source file list
		for (String sFile : sourceFiles) {
			
			InputManager inputFile = new InputManager(sFile);
			try {
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
						getRx();
						
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
								bAcceptThisClaim = bAcceptThisClaim && Rx.isValid(errMgr);
							}
						else
							bAcceptThisClaim = false;
						if (bAcceptThisClaim)
						{
							doRollUp();		// override when totals need to be calculated
							counts.claimRecordsAccepted++;
						}
						else
							counts.claimRecordsRejected++;
						
						/*
						for (String col_value : in)  
						{  
						    bAcceptThisClaim = bAcceptThisClaim && doMap(col_ix, col_value);
						    col_ix++;
						} 
						if (errMgr != null)
							bAcceptThisClaim = bAcceptThisClaim && Rx.isValid(errMgr);
						if (bAcceptThisClaim)
						{
							Rxs.add(Rx);
							counts.claimRecordsAccepted++;
						}
						else
							counts.claimRecordsRejected++;
							*/
					}
					else
						bStart = true;
				}
				inputFile.closeFile();
			} catch (IOException e) {
				throw new IllegalStateException("An error occurred while reading " + sFile);
			}  
			
		}
		
		// all source has been read in and loaded into svcLines List
		// let's sort
		log.info("Starting sort of all Rx Claims");
		
		Collections.sort(Rxs,new rxCompare());
		
		log.info("Completed sort of all Rx Claims");
		
	}
	
	protected boolean bPreserveQuotes = true;
	
	/**
	 * default Rx roll-up - just add the Rx claim to the pot
	 */
	protected void doRollUp() {
		Rxs.add(Rx);
	}
	
	
	/**
	 * new service line method allows override to do a look up of previously existing
	 */
	protected void getRx () {
		Rx = new ClaimRx();
	}



	protected boolean filterPass() {
		return true;
	}
	


	protected void combineLines (ClaimRx cTarget) {
		
		cTarget.setAllowed_amt(cTarget.getAllowed_amt() + Rx.getAllowed_amt());
		cTarget.setProxy_allowed_amt(cTarget.getProxy_allowed_amt() + Rx.getProxy_allowed_amt());
		cTarget.setReal_allowed_amt(cTarget.getReal_allowed_amt() + Rx.getReal_allowed_amt());
		cTarget.setCharge_amt(cTarget.getCharge_amt() + Rx.getCharge_amt());
		cTarget.setCoinsurance_amt(cTarget.getCoinsurance_amt() + Rx.getCoinsurance_amt());
		cTarget.setCopay_amt(cTarget.getCopay_amt() + Rx.getCopay_amt());
		cTarget.setDeductible_amt(cTarget.getDeductible_amt() + Rx.getDeductible_amt());
		cTarget.setPaid_amt(cTarget.getPaid_amt() + Rx.getPaid_amt());
		cTarget.setPrepaid_amt(cTarget.getPrepaid_amt() + Rx.getPrepaid_amt());
		
	}
	


	
	protected boolean doMap(int col_ix, String col_value) {
		throw new IllegalStateException("always override this method");
	}


	protected boolean checkForHeader(String line) {
		throw new IllegalStateException("always override this method");
	}

	public void addSourceFile (String s) {
		sourceFiles.add(s);
		stats.fileName.concat(s).concat(";");
	}
	
	List<String> sourceFiles = new ArrayList<String>();

	
	
	//public static final String RX_FILE = "C:\\input\\ECR_RX_MEDICARE.txt";
	public static final String RX_FILE = "C:\\input\\Pharma.txt.txt";

	
	SimpleDateFormat sdf_1; // = new SimpleDateFormat("yyyy-MM-dd");
	
	private static org.apache.log4j.Logger log = Logger.getLogger(ClaimRxDataAbstract.class);
	
	class rxCompare implements Comparator<ClaimRx> {

	    @Override
	    public int compare(ClaimRx o1, ClaimRx o2) {
	    	int c;
	    	if (o1.getMember_id() == null)
	    		o1.setMember_id("");
	    	if (o2.getMember_id() == null)
	    		o2.setMember_id("");
	        c = o1.getMember_id().compareTo(o2.getMember_id());
	        if (c == 0)
	        	if(o1.getRx_fill_date() == null)
	        		c = -1;
	        	else if(o2.getRx_fill_date() == null)
		        	c = 1;
	        	else
	        		c = o1.getRx_fill_date().compareTo(o2.getRx_fill_date());
	        //if (c == 0)
	        //	c = o1.getMember_id().compareTo(o2.getMember_id());
	        return c;
	    }
	}

	

	protected void doErrorReporting (String id, String errValue, ClaimRx clm) {
		if(errMgr != null)
			errMgr.issueError(id, errValue, clm);
	}
	

}
