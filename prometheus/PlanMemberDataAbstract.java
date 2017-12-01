





public abstract class PlanMemberDataAbstract extends CommonInputDataAbstract {
	
	protected Map<String, PlanMember> members = new HashMap<String, PlanMember>();
	protected PlanMember member;
	protected MemberInputCounts counts = new MemberInputCounts();

	


	public Map<String, PlanMember> getAllMembers() {
		if (members.isEmpty())
			prepareAllMembers();
		return members;
	}
	
	

	
	protected void prepareAllMembers () {
		
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
						counts.memberRecordsRead++;
						//in = line.split(sDelimiter);
						in = splitter(line);
						//log.info("Found line with " + in.length + " tokens");
						col_ix = 0;
						member = new PlanMember();
						// map each column to the service line object
						bAcceptThisEnrollment = true;
						for (String col_value : in)  
						{  
						    bAcceptThisEnrollment = bAcceptThisEnrollment && doMap(col_ix, col_value);
						    col_ix++;
						} 
						if (errMgr != null)
							bAcceptThisEnrollment = bAcceptThisEnrollment && member.isValid(errMgr);
						if (bAcceptThisEnrollment)
						{
							members.put(member.getMember_id(), member);
							counts.memberRecordsAccepted++;
						}
						else
							counts.memberRecordsRejected++;
					}
					else
						bStart = true;
				}
				inputFile.closeFile();
			} catch (IOException e) {
				throw new IllegalStateException("An error occurred while reading " + sFile);
			}  
			
		}
		
		log.info("Completed load of all plan members");
		
	}
	
	protected boolean bPreserveQuotes = true;
	

	protected boolean doMap(int col_ix, String col_value) {
		throw new IllegalStateException("always override this method");
	}


	protected boolean checkForHeader(String line) {
		throw new IllegalStateException("always override this method");
	}

	
	
	
	private static org.apache.log4j.Logger log = Logger.getLogger(PlanMemberDataAbstract.class);

}
