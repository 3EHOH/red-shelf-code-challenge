




public class QuickMemberSelectMA_APCD {
	
	
	public String sDelimiter = "\\*|\\||;|,";
	
	//private static final String carrierId = "291";
	private static final String [] carrierId = {"291", "300", "3505", "4962", "7041", "8026", "8647", "11701"};    
	
	private BufferedWriter bwMEF;
	
	private int countfinalMedicalClaimRecordsInput = 0;
	private int countMedicalClaimRecordsInput = 0;
	private int countMedicalClaimRecordsOutput = 0;
	private int countMemberEligibilityRecordsInput = 0;
	private int countMemberEligibilityRecordsOutput = 0;
	private int countRxClaimRecordsInput = 0;
	private int countRxClaimRecordsOutput = 0;
	private int countProviderRecordsInput = 0;
	private int countProviderRecordsOutput = 0;
	
	
	private void process() {
		
		for (String sC : carrierId) {
			memberByCarrierList.put(sC, new HashSet<String>());
		}

		// get a medical claims file
		InputManager medicalClaimFile = new InputManager(parameters.get("claiminput"));
		try {
			medicalClaimFile.openFile();
		} catch (IOException | ZipException e) {
			log.error("An error occurred while opening " + parameters.get("claiminput"));
		}
		
		// get a medical claims file
		InputManager finalClaimFile = new InputManager(parameters.get("finalclaiminput"));
		try {
			finalClaimFile.openFile();
		} catch (IOException | ZipException e) {
			log.error("An error occurred while opening " + parameters.get("finalclaiminput"));
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
		
		//limit = Integer.parseInt(parameters.get("count"));
		
		String line;
		
		
		// read the final claim indicator records
		try {
			// header
			line = finalClaimFile.readFile();
			countfinalMedicalClaimRecordsInput++;
			//
			while ((line = finalClaimFile.readFile()) != null)  {
				countfinalMedicalClaimRecordsInput++;
				accumulateFinalVersionKeys(line);
				if(countfinalMedicalClaimRecordsInput % 1000000 == 0) { 
					log.info("Completed reading  " + countfinalMedicalClaimRecordsInput + " final claim indicator records. " + new Date());
					log.info("--There are now  " + fvKey.size() + " final claim document Ids in memory. " + new Date());
				}
			}
		} catch (IOException e) {
			log.error("An error occurred while reading " + parameters.get("finalclaiminput"));
		}  
		
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
				countMemberEligibilityRecordsInput++;
				if (selectMember(line)) {
					bwMEF.write(line);
					bwMEF.newLine();
					countMemberEligibilityRecordsOutput++;
				}
				if(countMemberEligibilityRecordsInput % 1000000 == 0) { 
					log.info("Completed reading  " + countMemberEligibilityRecordsInput + " member eligibility records. " + new Date());
					for (String sC : carrierId) {
						log.info("--There are now " + memberByCarrierList.get(sC).size() + " Unique Members for carrier " + sC);
					}
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
				if(countMedicalClaimRecordsInput % 1000000 == 0) { 
					log.info("Completed reading  " + countMedicalClaimRecordsInput + " medical claim records. " + new Date());
					log.info("--So far  " + countMedicalClaimRecordsOutput + " medical claim records have been written. " + new Date());
				}
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
				if(countRxClaimRecordsInput % 1000000 == 0) { 
					log.info("Completed reading  " + countRxClaimRecordsInput + " pharmacy claim records. " + new Date());
					log.info("--So far  " + countRxClaimRecordsOutput + " pharmacy claim records have been written. " + new Date());
				}
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
				if(countProviderRecordsInput % 1000000 == 0) { 
					log.info("Completed reading  " + countProviderRecordsInput + " provider records. " + new Date());
					log.info("--So far  " + countProviderRecordsOutput + " provider records have been written. " + new Date());
				}
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
		
		
		
		log.info("Read " + countfinalMedicalClaimRecordsInput + " Final Version Records");		
		log.info("Read " + countMemberEligibilityRecordsInput + " Member Eligibility Records");
		log.info("Wrote " + countMemberEligibilityRecordsOutput + " Member Eligibility Records");
		for (String sC : carrierId) {
			memberByCarrierList.put(sC, new HashSet<String>());
			log.info("Selected " + memberByCarrierList.get(sC).size() + " Unique Members for carrier " + sC);
		}
		log.info("Read " + countMedicalClaimRecordsInput + " Medical Claim Records");
		log.info("Wrote " + countMedicalClaimRecordsOutput + " Medical Claim Records");
		log.info("Read " + countRxClaimRecordsInput + " Rx Claim Records");
		log.info("Wrote " + countRxClaimRecordsOutput + " Rx Claim Records");
		log.info("Read " + countProviderRecordsInput + " Provider Records");
		log.info("Wrote " + countProviderRecordsOutput + " Provider Records");
		
	}
	


	
	private void accumulateFinalVersionKeys (String s) {
		
		String[] in = s.split(sDelimiter);
		
		// ReleaseID is 1
		// VersionIndicator is 3
		
		if ( in[3].equals("1")  ||   in[3].equals("9")) {
			fvKey.add(in[1]);
			if (fvKey.size() < 10) {
				log.info("---Adding " + in[1] + "to final version key list");
			}
		}
		
	}
	
	private HashSet<String> fvKey = new HashSet<String>();
	

	
	private boolean isFinalVersion (String s) {
		return fvKey.contains(s);
	}
	
	
	
	
	
	private boolean selectClaim (String s) {
		
		boolean bResult = false;
		
		String[] in = s.split(sDelimiter);
		String sOrgId = in[11];
		String sFv = in[0];
		String sSvcStart = in[52];
		
		if (countMedicalClaimRecordsInput < 11) {
			log.info("Checking release Id: " + sFv + " OrgId: " + sOrgId + " SvcStart: " + sSvcStart);
		}
		
		
		// select only for specified carrier
		if (isFinalVersion (sFv)  &&  isWantedCarrier(sOrgId))
		{
			
			// column 9 is the MemberLinkEID
			String sMemberEID = in[9];
			bResult = isInList(sOrgId, sMemberEID);
			
			// 52 is service date from
			// format 2011-02-07 00:00:00
			// 53 is service date to
			bResult = bResult && claimDateOK(sSvcStart);
			
		}
		
		return bResult;
		
	}
	
	
	private boolean selectMember (String s) {
		
		boolean bResult = false;
		
		String[] in = s.split(sDelimiter);
		
		String sOrgId = in[8];
		
		// select only for specified carrier
		if (isWantedCarrier(sOrgId))
		{
		
			String sMemberEID = in[6];
			String sMedicalCoverage = in[12];
			
			// column 6 is the MemberLinkEID
			// when member eligibility file is the driver:
			if (sMemberEID.isEmpty()) {
				bResult = false;
			}
			else if (!sMedicalCoverage.equals("1")) {
				bResult = false;
			}
			else {
				if (isInList(sOrgId, sMemberEID)) {
					bResult = true;
				}
				else {
					memberByCarrierList.get(sOrgId).add(sMemberEID);
					bResult = true;
				}
			}
			
		}
		
		return bResult;
		
	}
	
	private boolean isWantedCarrier(String sOrgId) {
		boolean bR = false;
		for (String sC : carrierId) {
			if (sC.equals(sOrgId)) {
				bR = true;
				break;
			}
		}
		return bR;
	}


	Date eStartDate;
	Date eEndDate;
	SimpleDateFormat sdf; 
	

	private boolean claimDateOK(String s) {
		boolean bR = false;
		try {
			Date d = df2.parse(s);
			bR = d.after(dRangeStart) && d.before(dRangeEnd);
		} catch (ParseException e) {
			log.error("Something went wrong parsing incoming date" + e + " - " + s);
			e.printStackTrace();
		}
		return bR;
	}

	
	
	
	private boolean isInList (String sOrgId, String sMemberEID) {
		return memberByCarrierList.get(sOrgId).contains(sMemberEID);
	}
	
	
	private boolean selectRx (String s) {
		
		boolean bResult = false;
		
		String[] in = s.split(sDelimiter);
		
		String sFv = in[3];
		String sOrgId = in[8];
		String sFillDt = in[20];
		
		if (countRxClaimRecordsInput < 11) {
			log.info("Checking release Id: " + sFv + " OrgId: " + sOrgId + " FillDt: " + sFillDt);
		}
		
		// select only for specified carrier
		if (isFinalVersion (sFv)  &&  isWantedCarrier(sOrgId))
		{
		
			String sMemberEID = in[6];
		
			// column 6 is the MemberLinkEID
			bResult = isInList(sOrgId, sMemberEID);
			
			// 20 is date filled  [2009-08-18 00:00:00]
			bResult = bResult && claimDateOK(sFillDt);
	
		}
		
		return bResult;
		
	}
	
	private boolean selectProvider (String s) {
		
		boolean bResult = false;
		
		String[] in = s.split(sDelimiter);
		
		String sOrgId = in[2];
		// select only for specified carrier
		if (isWantedCarrier(sOrgId))
		{
				bResult = true;
		}
		
		return bResult;
		
	}
	
	
	private HashMap<String, HashSet<String>> memberByCarrierList = new HashMap<String, HashSet<String>>();
	

	public static void main(String[] args) throws IOException {
		
		QuickMemberSelectMA_APCD instance = new QuickMemberSelectMA_APCD();
		
		// get parameters (if any)
		instance.loadParameters(args);
		
		if (instance.parameters.get("runname") == null)
			instance.parameters.put("runname", "sample");
		
		try {
			instance.dRangeStart = instance.df2.parse("2010-12-31 23:59:59");
			instance.dRangeEnd = instance.df1.parse("20130101");
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
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
	DateFormat df2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	Date dRangeStart, dRangeEnd;
	
	
	private static org.apache.log4j.Logger log = Logger.getLogger(QuickMemberSelectMA_APCD.class);

	

}

