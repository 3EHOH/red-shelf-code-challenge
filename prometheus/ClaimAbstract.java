






public abstract class ClaimAbstract extends CommonInputDataAbstract implements ClaimInterface  {
	
	
	protected ClaimServLine svcLine;
	
	GenericOutputInterface oi = new GenericOutputMongo();
	GenericInputInterface ii = new GenericInputMongo(new ClaimServLine());
	
	
	public void prepareAllClaims () {
		
		if (sourceFiles.isEmpty())
			throw new IllegalStateException("No source files - must invoke addSourceFile prior to invoking this method");
		
		
		// get each file in the source file list
		for (String sFile : sourceFiles) {

			InputManager inputFile = new InputManager(sFile);
			try {
				inputFile.openFile();
			} catch (IOException | ZipException e) {
				log.error("An error occurred while opening " + sFile + " - " + e);
			}
			
			String line;//, col_value;
			int col_ix = 0;
			boolean bStart = false;
			boolean bAccept = false;
			col_index = new HashMap<Integer, ColumnFinder>();
			
			// read the lines from each file
			try {
				while ((line = inputFile.readFile()) != null)  {
					// check for a header in the first line  
					if (!bStart)
						bStart = checkForHeader(line);
					// map all data lines
					if (bStart) {
						//counts.providerRecordsRead++;
						in = line.split(sDelimiter);
						col_ix = 0;
						svcLine = initializeClaim(in);
						// map each column to the provider object
						bAccept = true;
						for (String col_value : in)  
						{  
							if (col_value != null)
								col_value = col_value.trim();
						    bAccept = bAccept && doMap(col_ix, col_value);
						    col_ix++;
						} 
						if (errMgr != null)
							bAccept = bAccept && svcLine.isValid(errMgr);
						if (bAccept)
						{
							//providers.put(provider.getProvider_id(), provider);
							doPostProcess();
							storeClaim();
							//counts.providerRecordsAccepted++;
						}
						//else
							//counts.providerRecordsRejected++;
					}
					else
						bStart = true;
				}
				inputFile.closeFile();
			} catch (IOException e) {
				throw new IllegalStateException("An error occurred while reading " + sFile);
			}  
			
		}
		
		log.info("Completed load of all medical claims");
		
	}
	

	protected ClaimServLine initializeClaim(String[] in) {
		return new ClaimServLine();
	}
	
	protected void storeClaim () {
		/*String id =*/ oi.write(svcLine);
		//log.info("Wrote: " + id);
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
	}	


	protected boolean doMap(int col_ix, String col_value) {
		throw new IllegalStateException("always override this method");
	}


	protected boolean checkForHeader(String line) {
		throw new IllegalStateException("always override this method");
	}

	
	
	ClaimHelper h;
	
	public ArrayList<String> getMemberList() {
		
		if (h == null)
			h = new ClaimHelper();
		
		return h.getMemberList();
		
	}

	
	private static org.apache.log4j.Logger log = Logger.getLogger(ClaimAbstract.class);
	
	
	

}
