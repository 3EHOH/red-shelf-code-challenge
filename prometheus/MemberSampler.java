





public class MemberSampler {
	
	

	private int selectCount = 1;
	
	
	private BufferedWriter bwMEF;
	
	private int countMedicalClaimRecordsInput = 0;
	private int countMedicalClaimRecordsOutput = 0;
	private int countMedicalClaim2RecordsInput = 0;
	private int countMedicalClaim2RecordsOutput = 0;
	private int countMemberEligibilityRecordsInput = 0;
	private int countMemberEligibilityRecordsOutput = 0;
	//private int countUniqueMembersOutput = 0;
	private int countRxClaimRecordsInput = 0;
	private int countRxClaimRecordsOutput = 0;
	private int countProviderRecordsInput = 0;
	private int countProviderRecordsOutput = 0;
	
	private ArrayList<String> mbCols = new ArrayList<String>();
	private ArrayList<String> mcCols = new ArrayList<String>();
	private ArrayList<String> rxCols = new ArrayList<String>();
	private ArrayList<String> pvCols = new ArrayList<String>();
	
	private ArrayList<String> mbIds = new ArrayList<String>();
	private ArrayList<String> pvIds = new ArrayList<String>();

	/* Maryland
	private static final String mbMemberIdColName = "PIDBDGP_ORIG";
	private static final String mcMemberIdColName = "PIDBDGP_ORIG";
	private static final String rxMemberIdColName = "PIDBDGP_ORIG";
	private static final String mcProviderIdColName = "NP_BP_NPI_P_EDT";
	private static final String rxProviderIdColName = "PROVID_PP";
	private static final String pvProviderIdColName = null;
	*/
	
	// New York
	private static final String mbMemberIdColName = "Member_ID";
	private static final String mcMemberIdColName = "Member_ID";
	private static final String rxMemberIdColName = "Member_ID";
	private static final String mcProviderIdColName = "Provider_NPI";
	private static final String rxProviderIdColName = "Prescribing_Provider_NPI";
	private static final String pvProviderIdColName = "Provider_NPI";
	
	private int mbMbrIdIdx = -1;
	private int mcMbrIdIdx = -1;
	private int rxMbrIdIdx = -1;
	
	private int mcPvrIdIdx = -1;
	private int rxPvrIdIdx = -1;
	private int pvPvrIdIdx = -1;
	
	
	// constructor only needed to pre-load member id's
	public MemberSampler() {
		mbIds.add("YE60610A");
		mbIds.add("ZP67796B");
		mbIds.add("DG81850U");
		mbIds.add("ER72886Q");
		mbIds.add("DY93742Q");
		mbIds.add("ER74981G");
	}
	
	private void process() {
		
		if (parameters.get("count") != null) {
			selectCount = Integer.parseInt(parameters.get("count"));
		}

		// get a medical claims file
		InputManager medicalClaimFile = new InputManager(parameters.get("claiminput"));
		try {
			medicalClaimFile.openFile();
		} catch (IOException | ZipException e) {
			log.error("An error occurred while opening " + parameters.get("claiminput"));
		}
		
		// get a second medical claims file
		InputManager medicalClaim2File = null;
		if (parameters.get("claim2input") != null) {
				medicalClaim2File = new InputManager(parameters.get("claim2input"));
				try {
					medicalClaim2File.openFile();
				} catch (IOException | ZipException e) {
					log.error("An error occurred while opening " + parameters.get("claim2input"));
				}
				
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
		InputManager ProviderFile = null;
		if (parameters.get("providerinput") != null) {
			ProviderFile = new InputManager(parameters.get("providerinput"));
			try {
				ProviderFile.openFile();
			} catch (IOException | ZipException e) {
				log.error("An error occurred while opening " + parameters.get("providerinput"));
			}
		}
				
		
		// define an extract file for the medical claims
		FileWriter outputMedicalClaimFile = null;
		try {
			outputMedicalClaimFile = new FileWriter(parameters.get("claimoutput"));
		} catch (IOException e1) {
			log.error("An error occurred while opening " + parameters.get("claimoutput"));
		}
		BufferedWriter bwMCF = new BufferedWriter(outputMedicalClaimFile);
		
		// define an extract file for a second medical claims file
		FileWriter outputMedicalClaim2File = null;
		BufferedWriter bwMCF2 = null;
		if (parameters.get("claim2output") != null) {
			try {
				outputMedicalClaim2File = new FileWriter(parameters.get("claim2output"));
				} catch (IOException e1) {
					log.error("An error occurred while opening " + parameters.get("claim2output"));
				}
				bwMCF2 = new BufferedWriter(outputMedicalClaim2File);
		}
		
		
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
		
		// define an extract file for the provider records
		FileWriter outputProviderFile = null;
		BufferedWriter bwPRF = null;
		if (parameters.get("provideroutput") != null) {
			try {
				outputProviderFile = new FileWriter(parameters.get("provideroutput"));
			} catch (IOException e1) {
				log.error("An error occurred while opening " + parameters.get("provideroutput"));
			}
			bwPRF = new BufferedWriter(outputProviderFile);
		}
		
		//limit = Integer.parseInt(parameters.get("count"));
		
		String line;
		
		
		
		
		// read the member eligibility records
		try {
			// header
			line = memberEligibilityFile.readFile();
			countMemberEligibilityRecordsInput++;
			bwMEF.write(line);
			bwMEF.newLine();
			countMemberEligibilityRecordsOutput++;
			findDelimiter(line);
			mbCols = getColumnHeadings(line);
			for (String sC : mbCols) {
				mbMbrIdIdx++;
				if (sC.equalsIgnoreCase(mbMemberIdColName)) {
					log.info("Found member Id for member file in column " + mbMbrIdIdx);
					break; 
				}
			}
			//
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
		
		//log.info("Selected " + countUniqueMembersOutput + " Unique Members");
		log.info("Selected " + mbIds.size() + " Unique Members");
		
		for (int i=0; i < 25  && i < mbIds.size(); i++) {
			log.info("Member " + i + " in list " + mbIds.get(i));
		}
		
		// read the medical claims
		try {
			// header
			line = medicalClaimFile.readFile();
			countMedicalClaimRecordsInput++;
			bwMCF.write(line);
			bwMCF.newLine();
			countMedicalClaimRecordsOutput++;
			findDelimiter(line);
			mcCols = getColumnHeadings(line);
			for (String sC : mcCols) {
				mcMbrIdIdx++;
				if (sC.equalsIgnoreCase(mcMemberIdColName)) {
					log.info("Found member Id for medical claim file in column " + mcMbrIdIdx);
					break; 
				}
			}
			for (String sC : mcCols) {
				mcPvrIdIdx++;
				if (sC.equalsIgnoreCase(mcProviderIdColName)) {
					log.info("Found provider Id for medical claim file in column " + mcPvrIdIdx);
					break; 
				}
			}
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
		
		// read the second medical claims file
		if (parameters.get("claim2input") != null) {
			
			try {
				// header
				line = medicalClaim2File.readFile();
				countMedicalClaim2RecordsInput++;
				bwMCF2.write(line);
				bwMCF2.newLine();
				countMedicalClaim2RecordsOutput++;
				findDelimiter(line);
				mcCols = getColumnHeadings(line);
				mcMbrIdIdx = -1;
				for (String sC : mcCols) {
					mcMbrIdIdx++;
					if (sC.equalsIgnoreCase(mcMemberIdColName)) {
						log.info("Found member Id for medical claim file 2 in column " + mcMbrIdIdx);
						break; 
					}
				}
				mcPvrIdIdx = -1;
				for (String sC : mcCols) {
					mcPvrIdIdx++;
					if (sC.equalsIgnoreCase(mcProviderIdColName)) {
						log.info("Found provider Id for medical claim file [2] in column " + mcPvrIdIdx);
						break; 
					}
				}
				//
				while ((line = medicalClaim2File.readFile()) != null)  {
					if (selectClaim(line)) {
						bwMCF2.write(line);
						bwMCF2.newLine();
						countMedicalClaim2RecordsOutput++;
					}
					countMedicalClaim2RecordsInput++;
				}
			} catch (IOException e) {
				log.error("An error occurred while reading " + parameters.get("claim2input"));
			}  
				
			try {
				medicalClaim2File.closeFile();
			} catch (IOException e) {
				log.error("An error occurred while closing file: " + parameters.get("claim2input"));
			} 
				
			try {
				bwMCF2.close();
			} catch (IOException e) {
				log.error("An error occurred while closing file: " + parameters.get("claim2output"));
			}
				
		}
				
		
		// read the Rx claim records
		try {
			// header
			line = RxClaimFile.readFile();
			countRxClaimRecordsInput++;
			bwRXF.write(line);
			bwRXF.newLine();
			countRxClaimRecordsOutput++;
			findDelimiter(line);
			rxCols = getColumnHeadings(line);
			for (String sC : rxCols) {
				rxMbrIdIdx++;
				if (sC.equalsIgnoreCase(rxMemberIdColName)) {
					log.info("Found member Id for medical claim file in column " + rxMbrIdIdx);
					break; 
				}
			}
			for (String sC : rxCols) {
				rxPvrIdIdx++;
				if (sC.equalsIgnoreCase(rxProviderIdColName)) {
					log.info("Found provider Id for Rx claim file in column " + rxPvrIdIdx);
					break; 
				}
			}
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
		if (parameters.get("providerinput") != null) {
		
			try {
				// header
				line = ProviderFile.readFile();
				countProviderRecordsInput++;
				bwPRF.write(line);
				bwPRF.newLine();
				countProviderRecordsOutput++;
				findDelimiter(line);
				pvCols = getColumnHeadings(line);
				for (String sC : pvCols) {
					pvPvrIdIdx++;
					if (sC.equalsIgnoreCase(pvProviderIdColName)) {
						log.info("Found provider Id for provider file in column " + pvPvrIdIdx);
						break; 
					}
				}
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
		
		}
		
		
		
				
		log.info("Read " + countMemberEligibilityRecordsInput + " Member Eligibility Records");
		log.info("Wrote " + countMemberEligibilityRecordsOutput + " Member Eligibility Records");
		//log.info("Selected " + countUniqueMembersOutput + " Unique Members");
		log.info("Read " + countMedicalClaimRecordsInput + " Medical Claim Records");
		log.info("Wrote " + countMedicalClaimRecordsOutput + " Medical Claim Records");
		log.info("Read " + countMedicalClaim2RecordsInput + " Medical Claim [2] Records");
		log.info("Wrote " + countMedicalClaim2RecordsOutput + " Medical Claim [2] Records");
		log.info("Read " + countRxClaimRecordsInput + " Rx Claim Records");
		log.info("Wrote " + countRxClaimRecordsOutput + " Rx Claim Records");
		log.info("Read " + countProviderRecordsInput + " Provider Records");
		log.info("Wrote " + countProviderRecordsOutput + " Provider Records");
		
	}
	

	private boolean selectClaim (String s) {
		
		boolean bResult = false;
		
		String[] in = splitter(s);
		
		String sMemberEID = in[mcMbrIdIdx].trim();
		bResult = isInList(sMemberEID);
		
		if (cCheck < 25) {
			cCheck++;
			log.info("Checking claim member id " + sMemberEID);
		}
		
		if (bResult) {
			if (pvIds.contains(in[mcPvrIdIdx].trim()))
			{}
			else
				pvIds.add(in[mcPvrIdIdx].trim());
		}
		
		return bResult;
		
	}
	
	private int cCheck = 0;
	
	
	
	private boolean selectMember (String s) {
		
		boolean bResult = false;
		
		String[] in = splitter(s);
		
		String sMemberEID = in[mbMbrIdIdx].trim();
		
		if (mbIds.contains(sMemberEID)) {
			bResult = true;
		}
		else if (mbIds.size() > selectCount)
			bResult = false;
		else {
			mbIds.add(sMemberEID);
			bResult = true;
		}

		return bResult;
		
	}

	
	private boolean isInList (String s) {
		return mbIds.contains(s);
	}
	
	
	private boolean selectRx (String s) {
		
		boolean bResult = false;
		
		String[] in = splitter(s);
		
		String sMemberEID = in[rxMbrIdIdx].trim();
		bResult = isInList(sMemberEID);
		
		if (bResult) {
			if (pvIds.contains(in[rxPvrIdIdx]))
			{}
			else
				pvIds.add(in[rxPvrIdIdx]);
		}
		
		return bResult;
		
	}
	
	private boolean selectProvider (String s) {
		
		String[] in = splitter(s);
		
		return pvIds.contains(in[pvPvrIdIdx].trim());
		
	}
	
	
	private ArrayList<String> getColumnHeadings (String s) {
		
		String[] in = splitter(s);
		ArrayList<String> retA = new ArrayList<String>();
		for (String h : in) {
			retA.add(h);
		}
		return retA;
	}
	
	
	private String [] splitter (String line) {
		
		String [] sReturn;
		
		boolean b = false;
		
		if (delimiter.contains(",")  &&  line.contains("\"")) {
			b = true;
			StringBuffer sb = new StringBuffer();
			int linePtr = 0;
			while (linePtr < line.length()  && line.indexOf('"', linePtr) > -1) {
				sb.append(line.substring(linePtr, line.indexOf('"', linePtr)));
				linePtr = line.indexOf('"', linePtr) + 1;
				sb.append(line.substring(linePtr, line.indexOf('"', linePtr)).replace(",", "`"));
				linePtr = line.indexOf('"', linePtr) + 1;
			}
			if (linePtr < line.length())
				sb.append(line.substring(linePtr));
			line = sb.toString();
		}
		
		sReturn = line.split(getDelimiter(), -1);
		
		if (b) {
			for (int i=0; i < sReturn.length; i++) {
				sReturn [i] = sReturn [i].replace("`", ","); 
			}
		}
		
		return sReturn;
		
	}
	
	char [] sTypicalDelimiters = {'|', ',', '*', '\t'};

	private String delimiter = ""; 
	
	/**
	 * find a delimiter from the above list
	 * add to it as needed
	 * @param line
	 * @return
	 */
	boolean findDelimiter (String line) {

		StringBuffer sb = new StringBuffer();
		for ( int i=0; i < line.length(); i++ ) {
			for (char x: sTypicalDelimiters) {
				if (line.charAt(i) == x) {
					sb.append('\\');
					sb.append(x);
					setDelimiter(sb.toString());
					break;
				}
			}
			if (!getDelimiter().isEmpty())
				break;
		}
		
		if (getDelimiter().isEmpty()) {
			sb = new StringBuffer();
			sb.append("Could not find delimiter among: ");
			for (char x: sTypicalDelimiters) {
				if (x == '\t')
					sb.append("\\t");
				else
					sb.append(x);
			}
			log.error(sb);
		}
			
		return !getDelimiter().isEmpty();
		
	}
	
	
	public String getDelimiter() {
		return delimiter;
	}

	public void setDelimiter(String delimiter) {
		this.delimiter = delimiter;
	}
	
	
	
	

	public static void main(String[] args) throws IOException {
		
		MemberSampler instance = new MemberSampler();
		
		// get parameters (if any)
		instance.loadParameters(args);
		
		if (instance.parameters.get("runname") == null)
			instance.parameters.put("runname", "sample");
		
		try {
			instance.dOld = instance.df1.parse("20010101");
			instance.dNew = instance.df1.parse("20130101");
			instance.dRangeStart = instance.df1.parse("20110101");
			instance.dRangeEnd = instance.df1.parse("20121231");
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		// process
		instance.process();

	}
	
	HashMap<String, String> parameters = new HashMap<String, String>();
	String [][] parameterDefaults = {
			{"count", "1"}
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
	Date dOld, dNew;
	Date dRangeStart, dRangeEnd;
	
	
	
	private static org.apache.log4j.Logger log = Logger.getLogger(MemberSampler.class);
	
	
	
	

}
