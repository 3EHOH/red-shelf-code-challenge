



public class QuickMap {
	
	public String sDelimiter = "\\*|\\||;|,";
	
	private int limit = 1000;
	private static final String carrierId = "291";
	
	
	private void process() {

		// get a medical claims file
		InputManager medicalClaimFile = new InputManager(parameters.get("claiminput"));
		try {
			medicalClaimFile.openFile();
		} catch (IOException | ZipException e) {
			log.error("An error occurred while opening " + parameters.get("claiminput"));
		}
		
		// get a member eligibility file
		InputManager memberEligibilityFile = new InputManager(parameters.get("memberinput"));
		try {
			memberEligibilityFile.openFile();
		} catch (IOException | ZipException e) {
			log.error("An error occurred while opening " + parameters.get("memberinput"));
		}
		
		// get a Rx claim file
		InputManager RxClaimFile = new InputManager(parameters.get("rxinput"));
		try {
			RxClaimFile.openFile();
		} catch (IOException | ZipException e) {
			log.error("An error occurred while opening " + parameters.get("rxinput"));
		}
		
		// get a Provider file
		InputManager ProviderFile = new InputManager(parameters.get("providerinput"));
		try {
			ProviderFile.openFile();
		} catch (IOException | ZipException e) {
			log.error("An error occurred while opening " + parameters.get("providerinput"));
		}
				
		
		// define an extract file for the medical claims
		FileWriter outputMedicalClaimFile = null;
		try {
			outputMedicalClaimFile = new FileWriter(parameters.get("claimoutput"));
		} catch (IOException e1) {
			log.error("An error occurred while opening " + parameters.get("claimoutput"));
		}
		BufferedWriter bwMCF = new BufferedWriter(outputMedicalClaimFile);
		
		
		// define an extract file for the member eligibility records
		FileWriter outputMemberEligibilityFile = null;
		try {
			outputMemberEligibilityFile = new FileWriter(parameters.get("memberoutput"));
		} catch (IOException e1) {
			log.error("An error occurred while opening " + parameters.get("memberoutput"));
		}
		BufferedWriter bwMEF = new BufferedWriter(outputMemberEligibilityFile);
		
		// define an extract file for the Rx claim records
		FileWriter outputRxClaimFile = null;
		try {
			outputRxClaimFile = new FileWriter(parameters.get("rxoutput"));
		} catch (IOException e1) {
			log.error("An error occurred while opening " + parameters.get("rxoutput"));
		}
		BufferedWriter bwRXF = new BufferedWriter(outputRxClaimFile);
		
		// define an extract file for the Rx claim records
		FileWriter outputProviderFile = null;
		try {
			outputProviderFile = new FileWriter(parameters.get("provideroutput"));
		} catch (IOException e1) {
			log.error("An error occurred while opening " + parameters.get("provideroutput"));
		}
		BufferedWriter bwPRF = new BufferedWriter(outputProviderFile);
		
		limit = Integer.parseInt(parameters.get("count"));
		
		String line;
		int countMedicalClaimRecordsInput = 0;
		int countMedicalClaimRecordsOutput = 0;
		int countMemberEligibilityRecordsInput = 0;
		int countMemberEligibilityRecordsOutput = 0;
		int countRxClaimRecordsInput = 0;
		int countRxClaimRecordsOutput = 0;
		int countProviderRecordsInput = 0;
		int countProviderRecordsOutput = 0;
		
		// read the medical claims
		try {
			// header
			line = medicalClaimFile.readFile();
			countMedicalClaimRecordsInput++;
			bwMCF.write(line);
			bwMCF.newLine();
			countMedicalClaimRecordsOutput++;
			while ((line = medicalClaimFile.readFile()) != null)  {
				if (selectClaim(line)) {
					bwMCF.write(line);
					bwMCF.newLine();
					countMedicalClaimRecordsOutput++;
				}
				countMedicalClaimRecordsInput++;
			}
		} catch (IOException e) {
			log.error("An error occurred while reading " + parameters.get("claiminput"));
		}  
		
		try {
			medicalClaimFile.closeFile();
		} catch (IOException e) {
			log.error("An error occurred while closing file: " + parameters.get("claiminput"));
		} 
		
		try {
			bwMCF.close();
		} catch (IOException e) {
			log.error("An error occurred while closing file: " + parameters.get("claimoutput"));
		}
		
		// read the member eligibility records
		try {
			// header
			line = memberEligibilityFile.readFile();
			countMemberEligibilityRecordsInput++;
			bwMEF.write(line);
			bwMEF.newLine();
			countMemberEligibilityRecordsOutput++;
			while ((line = memberEligibilityFile.readFile()) != null)  {
				if (selectMember(line)) {
					bwMEF.write(line);
					bwMEF.newLine();
					countMemberEligibilityRecordsOutput++;
				}
				countMemberEligibilityRecordsInput++;
			}
		} catch (IOException e) {
			log.error("An error occurred while reading " + parameters.get("memberinput"));
		}  
		
		try {
			memberEligibilityFile.closeFile();
		} catch (IOException e) {
			log.error("An error occurred while closing file: " + parameters.get("memberinput"));
		}  
		
		try {
			bwMEF.close();
		} catch (IOException e) {
			log.error("An error occurred while closing file: " + parameters.get("memberoutput"));
		}
		
		// read the Rx claim records
		try {
			// header
			line = RxClaimFile.readFile();
			countRxClaimRecordsInput++;
			bwRXF.write(line);
			bwRXF.newLine();
			countRxClaimRecordsOutput++;
			while ((line = RxClaimFile.readFile()) != null)  {
				if (selectRx(line)) {
					bwRXF.write(line);
					bwRXF.newLine();
					countRxClaimRecordsOutput++;
				}
				countRxClaimRecordsInput++;
			}
		} catch (IOException e) {
			log.error("An error occurred while reading " + parameters.get("rxinput"));
		}  
		
		try {
			RxClaimFile.closeFile();
		} catch (IOException e) {
			log.error("An error occurred while closing file: " + parameters.get("rxinput"));
		}  
		
		try {
			bwRXF.close();
		} catch (IOException e) {
			log.error("An error occurred while closing file: " + parameters.get("rxoutput"));
		}
		
		// read the Provider claim records
		try {
			// header
			line = ProviderFile.readFile();
			countProviderRecordsInput++;
			bwPRF.write(line);
			bwPRF.newLine();
			countProviderRecordsOutput++;
			while ((line = ProviderFile.readFile()) != null)  {
				if (selectProvider(line)) {
					bwPRF.write(line);
					bwPRF.newLine();
					countProviderRecordsOutput++;
				}
				countProviderRecordsInput++;
			}
		} catch (IOException e) {
			log.error("An error occurred while reading " + parameters.get("providerinput"));
		}  
		
		try {
			ProviderFile.closeFile();
		} catch (IOException e) {
			log.error("An error occurred while closing file: " + parameters.get("providerinput"));
		}  
		
		try {
			bwPRF.close();
		} catch (IOException e) {
			log.error("An error occurred while closing file: " + parameters.get("provideroutput"));
		}
				
		
		log.info("Read " + countMedicalClaimRecordsInput + " Medical Claim Records");
		log.info("Wrote " + countMedicalClaimRecordsOutput + " Medical Claim Records");
		log.info("Read " + countMemberEligibilityRecordsInput + " Member Eligibility Records");
		log.info("Wrote " + countMemberEligibilityRecordsOutput + " Member Eligibility Records");
		log.info("Read " + countRxClaimRecordsInput + " Rx Claim Records");
		log.info("Wrote " + countRxClaimRecordsOutput + " Rx Claim Records");
		log.info("Read " + countProviderRecordsInput + " Provider Records");
		log.info("Wrote " + countProviderRecordsOutput + " Provider Records");
		
	}
	
	HashMap<String, String> memberEID = new HashMap<String, String>();
	
	
	private boolean selectClaim (String s) {
		
		boolean bResult = false;
		
		String[] in = s.split(sDelimiter);
		String sOrgId = in[11];
		// select only for specified carrier
		if (sOrgId.equals(carrierId))
		{
			String sMemberEID = in[9];
		
			// column 9 is the MemberLinkEID
			if (sMemberEID.isEmpty())
				bResult = false;
			else if (memberEID.containsKey(sMemberEID))
				bResult = true;
			else if (memberEID.size() < limit) {
				bResult = true;
				memberEID.put(sMemberEID, sMemberEID);
			}
		}
		
		return bResult;
		
	}
	
	private boolean selectMember (String s) {
		
		boolean bResult = false;
		
		String[] in = s.split(sDelimiter);
		
		String sOrgId = in[8];
		// select only for specified carrier
		if (sOrgId.equals(carrierId))
		{
		
			String sMemberEID = in[6];
		
			// column 6 is the MemberLinkEID
			if (memberEID.containsKey(sMemberEID))
				bResult = true;
		}
		
		return bResult;
		
	}
	
	private boolean selectRx (String s) {
		
		boolean bResult = false;
		
		String[] in = s.split(sDelimiter);
		
		String sOrgId = in[8];
		// select only for specified carrier
		if (sOrgId.equals(carrierId))
		{
		
			String sMemberEID = in[6];
		
			// column 6 is the MemberLinkEID
			if (memberEID.containsKey(sMemberEID))
				bResult = true;
		}
		
		return bResult;
		
	}
	
	private boolean selectProvider (String s) {
		
		boolean bResult = false;
		
		String[] in = s.split(sDelimiter);
		
		String sOrgId = in[2];
		// select only for specified carrier
		if (sOrgId.equals(carrierId))
		{
				bResult = true;
		}
		
		return bResult;
		
	}
	

	public static void main(String[] args) throws IOException {
		
		QuickMap instance = new QuickMap();
		
		// get parameters (if any)
		instance.loadParameters(args);
		
		if (instance.parameters.get("runname") == null)
			instance.parameters.put("runname", "sample");
		
		// process
		instance.process();

	}
	
	HashMap<String, String> parameters = new HashMap<String, String>();
	String [][] parameterDefaults = {
			{"count", "1000"}
	};
	
	/**
	 * load default parameters and 
	 * put any run arguments in the hash map as well
	 * arguments should take the form keyword=value (e.e., studybegin=20140101)
	 * @param args
	 */
	private void loadParameters (String[] args) {
		// load any default parameters from the default parameter array
		for (int i = 0; i < parameterDefaults.length; i++) {
			parameters.put(parameterDefaults[i][0], parameterDefaults[i][1]);
		}
		// overlay or add any incoming parameters
		for (int i = 0; i < args.length; i++) {
			parameters.put(args[i].substring(0, args[i].indexOf('=')), args[i].substring(args[i].indexOf('=')+1)) ;
		}		
	}
	
	
	DateFormat df1 = new SimpleDateFormat("yyyyMMdd");
	DateFormat df2 = new SimpleDateFormat("yyyy-MM-dd");
	
	
	
	private static org.apache.log4j.Logger log = Logger.getLogger(QuickMap.class);



}
