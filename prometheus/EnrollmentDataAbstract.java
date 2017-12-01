






public abstract class EnrollmentDataAbstract extends CommonInputDataAbstract {
	
	protected Map<String, List<Enrollment>> enrollments = new HashMap<String, List<Enrollment>>();
	protected Enrollment enrollment;
	protected List<Enrollment> enList;
	protected EnrollmentInputCounts counts = new EnrollmentInputCounts();
	
	

	public Map<String, List<Enrollment>> getAllEnrollments() {
		if (enrollments.isEmpty())
			prepareAllEnrollments();
		return enrollments;
	}
	
	
	protected void prepareAllEnrollments () {
		
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
			boolean bAcceptThisEnrollment = false;
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
						counts.enrollmentRecordsRead++;
						//in = line.split(sDelimiter);
						in = splitter(line);
						//log.info("Found line with " + in.length + " tokens");
						col_ix = 0;
						getEnrollment();
						// map each column to the service line object
						bAcceptThisEnrollment = true;
						// override to take records completely out of the process
						if (filterPass() ) {
							for (String col_value : in)  
							{  
								if (col_value != null)
									col_value = col_value.trim();
								bAcceptThisEnrollment = bAcceptThisEnrollment && doMap(col_ix, col_value);
								col_ix++;
							}
							if (errMgr != null)
								bAcceptThisEnrollment = bAcceptThisEnrollment && enrollment.isValid(errMgr);
							}
						else
							bAcceptThisEnrollment = false;
						
						if (bAcceptThisEnrollment)
						{
							enList = enrollments.get(enrollment.getMember_id());
							if (enList == null) {
								enList = new ArrayList<Enrollment>();
								enrollments.put(enrollment.getMember_id(), enList);
								enList.add(enrollment);
							}
							else {
								doRollUp(enList);
							}
							//enList.add(enrollment);
							//enrollments.put(enrollment.getMember_id(), enrollment);
							counts.enrollmentRecordsAccepted++;
						}
						else
							counts.enrollmentRecordsRejected++;
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
	
	protected boolean bPreserveQuotes = true;
	
	/**
	 * stock enrollment roll-up
	 * if product and coverage types are the same
	 * if this record's begin date is one day greater than a prior record's enrollment, 
	 * just extend the end date of the existing record
	 * @param enlist
	 */
	protected void doRollUp(List<Enrollment> enlist) {
		
		boolean bConcat = false;
		
		for (Enrollment e : enlist) {
			if ( (!(enrollment.getEnd_date() == null))  &&
					(e.getInsurance_product()  == null  ||
					e.getInsurance_product().equals(enrollment.getInsurance_product()))  && 
					(e.getCoverage_type() == null  || 
					e.getCoverage_type().equals(enrollment.getCoverage_type()))) {
				c.setTime(enrollment.getEnd_date());
				c.add(Calendar.DAY_OF_MONTH, 1);
				d.setTime(c.getTimeInMillis());
				if (fmt.format(d).equals(fmt.format(e.getBegin_date()))) {
					e.setEnd_date(enrollment.getEnd_date());
					bConcat = true;
					break;
				}
			}
		}
		
		if (!bConcat)
			enlist.add(enrollment);
		
	}
	
	SimpleDateFormat fmt = new SimpleDateFormat("yyyyMMdd");
	Calendar c = Calendar.getInstance();
	Date d = new Date(0);
	

	protected boolean doMap(int col_ix, String col_value) {
		throw new IllegalStateException("always override this method");
	}


	protected boolean checkForHeader(String line) {
		throw new IllegalStateException("always override this method");
	}
	
	protected void getEnrollment() {
		enrollment = new Enrollment();
	}


	protected boolean filterPass() {
		return true;
	}

	
	
	
	private static org.apache.log4j.Logger log = Logger.getLogger(EnrollmentDataAbstract.class);


}
