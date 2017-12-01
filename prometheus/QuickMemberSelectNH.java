



public class QuickMemberSelectNH {
	
	
	private int captureLimit = 100;
	
	public String sDelimiter = "\\*|\\||;|,";
	
	
	private BufferedWriter bwMEF;
	
	private int countMedicalClaimRecordsInput = 0;
	private int countMedicalClaimRecordsOutput = 0;
	private int countMemberEligibilityRecordsInput = 0;
	private int countMemberEligibilityRecordsOutput = 0;
	private int countRxClaimRecordsInput = 0;
	private int countRxClaimRecordsOutput = 0;
	private int countProviderRecordsInput = 0;
	private int countProviderRecordsOutput = 0;
	
	
	private void process() {
		
		if (parameters.get("capturelimit") != null) {
			captureLimit = Integer.parseInt(parameters.get("capturelimit"));
		}

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
		bwMEF = new BufferedWriter(outputMemberEligibilityFile);
		
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
		
		
		String line;
				
		
		// read the member eligibility records
		try {
			// header
			line = memberEligibilityFile.readFile();
			countMemberEligibilityRecordsInput++;
			bwMEF.write(line);
			bwMEF.newLine();
			countMemberEligibilityRecordsOutput++;
			//
			while ((line = memberEligibilityFile.readFile()) != null)  {
				if (accumulateMember(line) ) {
					bwMEF.write(line);
					bwMEF.newLine();
					countMemberEligibilityRecordsInput++;
				}
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
		
		log.info("Selected " + memberAccumulator.size() + " Unique Members");
		
		// read the medical claims
		try {
			// header
			line = medicalClaimFile.readFile();
			countMedicalClaimRecordsInput++;
			bwMCF.write(line);
			bwMCF.newLine();
			countMedicalClaimRecordsOutput++;
			//
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
		
		// read the Rx claim records
		try {
			// header
			line = RxClaimFile.readFile();
			countRxClaimRecordsInput++;
			bwRXF.write(line);
			bwRXF.newLine();
			countRxClaimRecordsOutput++;
			//
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
			//
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
		
		
		
				
		log.info("Read " + countMemberEligibilityRecordsInput + " Member Eligibility Records");
		log.info("Wrote " + countMemberEligibilityRecordsOutput + " Member Eligibility Records");
		log.info("Selected " + memberAccumulator.size() + " Unique Members");
		log.info("Read " + countMedicalClaimRecordsInput + " Medical Claim Records");
		log.info("Wrote " + countMedicalClaimRecordsOutput + " Medical Claim Records");
		log.info("Read " + countRxClaimRecordsInput + " Rx Claim Records");
		log.info("Wrote " + countRxClaimRecordsOutput + " Rx Claim Records");
		log.info("Read " + countProviderRecordsInput + " Provider Records");
		log.info("Wrote " + countProviderRecordsOutput + " Provider Records");
		
	}
	
	

	private HashSet<String> memberAccumulator = new HashSet<String>();
	private HashSet<String> providerAccumulator = new HashSet<String>();

	

	private boolean accumulateMember (String s) {
		
		boolean bResult = false;
		
		lCountM++;
		
		String[] in = s.split(sDelimiter);
		
		// MEMBER_KEY_ME904
		String sMemberEID = in[15];
		// COVERAGE_MEDICAL_ME018
		//String sMedicalCoverage = in[7];
			
		// column 6 is the MemberLinkEID
		// when member eligibility file is the driver:
		if (sMemberEID.isEmpty()) {
			bResult = false;
		}
		//else if (!sMedicalCoverage.equals("Y")) {
		//	bResult = false;
		//}
		else {
			if (isInList(sMemberEID)) {
				bResult = true;
			}
			else {
				if (! (memberAccumulator.size() > captureLimit) ) {
					memberAccumulator.add(sMemberEID);
					log.info("Adding member " + sMemberEID);
					bResult = true;
				}
			}
			
		}
			
		
		if (lCountM < 10)
			log.info(sMemberEID);

		return bResult;
		
	}
	
	private int lCountM = 0;
	
	
	private boolean selectClaim (String s) {
		
		boolean bResult = false;
		
		lCountC++;
		
		String[] in = s.split(sDelimiter);
			
		// column 62 is the MEMBER_KEY_MC903
		String sMemberEID = in[62];
		bResult = isInList(sMemberEID);
		
		if (lCountC < 10)
			log.info(sMemberEID);
			
		// column 12 is provider id
		if(bResult)
			providerAccumulator.add(in[12]);
		
		
		return bResult;
		
	}
	
	private int lCountC = 0;
	

	
	private boolean isInList (String s) {
		return memberAccumulator.contains(s);
	}
	
	
	private boolean selectRx (String s) {
		
		boolean bResult = false;
		
		lCountR++;
		
		String[] in = s.split(sDelimiter);
		

		// MEMBER_KEY_PC903
		String sMemberEID = in[16];
		
		if (lCountR < 10)
			log.info(sMemberEID);
		
		bResult = isInList(sMemberEID);

		
		return bResult;
		
	}
	
	private int lCountR = 0;
	
	private boolean selectProvider (String s) {
		
		boolean bResult = false;
		
		String[] in = s.split(sDelimiter);
		
		if (providerAccumulator.contains(in[0]))
			bResult = true;
		
		return bResult;
		
	}
	

	public static void main(String[] args) throws IOException {
		
		QuickMemberSelectNH instance = new QuickMemberSelectNH();
		
		// get parameters (if any)
		instance.loadParameters(args);
		
		if (instance.parameters.get("runname") == null)
			instance.parameters.put("runname", "sample");
		
		// process
		instance.process();

	}
	
	HashMap<String, String> parameters = new HashMap<String, String>();
	String [][] parameterDefaults = {
			{"capturelimit", "1000"}
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
	
	
	
	private static org.apache.log4j.Logger log = Logger.getLogger(QuickMemberSelectNH.class);

	
	

}

