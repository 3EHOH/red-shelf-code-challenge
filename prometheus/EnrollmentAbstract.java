





public class EnrollmentAbstract extends CommonInputDataAbstract implements EnrollmentInterface  {
	
	
	protected Enrollment enrollment;
	
	GenericOutputInterface oi = new GenericOutputMongo();
	
	
	public void prepareAllEnrollments () {
		
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
						enrollment = initializeEnrollment(in);
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
							bAccept = bAccept && enrollment.isValid(errMgr);
						if (bAccept)
						{
							//providers.put(provider.getProvider_id(), provider);
							storeEnrollment();
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
		
		log.info("Completed load of all enrollments");
		
	}
	

	protected Enrollment initializeEnrollment(String[] in) {
		return new Enrollment();
	}
	
	protected void storeEnrollment () {
		/*String id =*/ oi.write(enrollment);
		//log.info("Wrote: " + id);
	}


	protected boolean doMap(int col_ix, String col_value) {
		throw new IllegalStateException("always override this method");
	}


	protected boolean checkForHeader(String line) {
		throw new IllegalStateException("always override this method");
	}


	
	private static org.apache.log4j.Logger log = Logger.getLogger(EnrollmentAbstract.class);
	
	
	


}
