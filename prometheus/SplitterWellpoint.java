




public class SplitterWellpoint {
	
	
	public String sDelimiter = "\\|";
	
	
	private int countMedicalClaimHeadersInputPass1 = 0;
	private int countMedicalClaimHeadersInput = 0;
	private int countMedicalClaimHeadersOutput = 0;
	private int countMedicalClaimRecordsInput = 0;
	private int countMedicalClaimRecordsOutput = 0;
	private int countMemberEligibilityRecordsInput = 0;
	private int countMemberEligibilityRecordsOutput = 0;
	private int countRxClaimRecordsInput = 0;
	private int countRxClaimRecordsOutput = 0;
	private int countProviderRecordsInput = 0;
	private int countProviderRecordsOutput = 0;
	
	
	InputManager medicalClaimHdrFile;
	InputManager medicalClaimFile;
	InputManager memberEligibilityFile;
	InputManager RxClaimFile;
	InputManager ProviderFile;
	FileWriter outputMedicalHdrClaimFile;
	FileWriter outputMedicalClaimFile;
	FileWriter outputMemberEligibilityFile;
	FileWriter outputRxClaimFile;
	FileWriter outputProviderFile;
	
	boolean bLastPass = false;
	
	
	
	// Production Episode Builder database 
	private static String		schema 	= "ecr";
	private static String 		dbUrl 	= "54.200.108.113:3306";
	private static String 		dbUser 	= "epbuilder";
	private static String 		dbPw 	= "dev420!Xw";
	private static String 		tableName = "ecr.Wellpoint_V2ClaimServLine2015_03_09_0221";
	
	
	private void process() {
		
		
		getBypassCriteria();
		
		// get a medical claims file
		medicalClaimHdrFile = new InputManager(parameters.get("claimhdrinput"));
		try {
			medicalClaimHdrFile.openFile();
		} catch (IOException | ZipException e) {
			log.error("An error occurred while opening " + parameters.get("claimhdrinput"));
		}

		// get a medical claims file
		medicalClaimFile = new InputManager(parameters.get("claiminput"));
		try {
			medicalClaimFile.openFile();
		} catch (IOException | ZipException e) {
			log.error("An error occurred while opening " + parameters.get("claiminput"));
		}
		
		// get a member eligibility file
		memberEligibilityFile = new InputManager(parameters.get("memberinput"));
		try {
			memberEligibilityFile.openFile();
		} catch (IOException | ZipException e) {
			log.error("An error occurred while opening " + parameters.get("memberinput"));
		}
		
		// get a Rx claim file
		RxClaimFile = new InputManager(parameters.get("rxinput"));
		try {
			RxClaimFile.openFile();
		} catch (IOException | ZipException e) {
			log.error("An error occurred while opening " + parameters.get("rxinput"));
		}
		
		// get a Provider file
		ProviderFile = new InputManager(parameters.get("providerinput"));
		try {
			ProviderFile.openFile();
		} catch (IOException | ZipException e) {
			log.error("An error occurred while opening " + parameters.get("providerinput"));
		}
				
		
		
		//limit = Integer.parseInt(parameters.get("count"));
		
		
		String line;
		String[] in;

		
		// read the medical claim header records
		try {
			// header
			line = medicalClaimHdrFile.readFile();
			countMedicalClaimHeadersInputPass1++;
			
			while ((line = medicalClaimHdrFile.readFile()) != null)  {
				countMedicalClaimHeadersInputPass1++;
				in = line.split(sDelimiter);
				FullMemberList.add(in[0]);
				//log.info("R: " + countMedicalClaimHeadersInputPass1 + " M: " + FullMemberList.size());
			}
		} catch (IOException e) {
			log.error("An error occurred while reading " + parameters.get("claimhdrinput"));
		}
		
		try {
			medicalClaimHdrFile.closeFile();
		} catch (IOException e) {
			log.error("An error occurred while closing file: " + parameters.get("claimhdrinput"));
		}  
		
		int chunkNumber = Integer.parseInt(parameters.get("divisor"));
		int chunkSize = FullMemberList.size() / chunkNumber;
		

		Iterator<String> iter = FullMemberList.iterator();
		
		for (int a=0; a < chunkNumber - 1; a++) {
			MemberSelectionList = new HashSet<String>();
			ProviderSelectionList = new HashSet<String>();
			for (int b=0; b < chunkSize; b++) {
				MemberSelectionList.add(iter.next());
			}
			openEachSlice(a + 1);
			createTempDataset();
		}
		
		// last pass
		MemberSelectionList = new HashSet<String>();
		ProviderSelectionList = new HashSet<String>();
		while (iter.hasNext()) {
			MemberSelectionList.add(iter.next());
		}

		bLastPass = true;
		openEachSlice(chunkNumber);
		createTempDataset();
			
			
		
		
		log.info("Read " + countMedicalClaimHeadersInputPass1 + " Medical Header Records on Pass 1");
		log.info("Read " + countMedicalClaimHeadersInput + " Medical Header Records");
		log.info("Wrote " + countMedicalClaimHeadersOutput + " Medical Header Records");		
		log.info("Read " + countMedicalClaimRecordsInput + " Medical Claim Records");
		log.info("Wrote " + countMedicalClaimRecordsOutput + " Medical Claim Records");
		log.info("Read " + countRxClaimRecordsInput + " Rx Claim Records");
		log.info("Wrote " + countRxClaimRecordsOutput + " Rx Claim Records");
		log.info("Read " + countMemberEligibilityRecordsInput + " Member Eligibility Records");
		log.info("Wrote " + countMemberEligibilityRecordsOutput + " Member Eligibility Records");
		log.info("Read " + countProviderRecordsInput + " Provider Records");
		log.info("Wrote " + countProviderRecordsOutput + " Provider Records");
		
	}
	
	
	private boolean checkBypass(String[] in) {
		
		boolean bKeep = true;
		
		String sMemberId = in[0];
		String sClaimNo = in[1];
		
		if (MemberBypassList.contains(sMemberId)) {
			bKeep = false;
			log.info(sMemberId + " is being bypassed");
		}
		else if (ClaimBypassList.containsKey(sMemberId)) {
			ArrayList<String> _l = ClaimBypassList.get(sMemberId);
			for (String s : _l) {
				if (sClaimNo == null || sClaimNo.equals(s)) {
					bKeep = false;
					log.info(sMemberId + " " + sClaimNo + " is being bypassed");
					break;
				}
			}
		}
		
		return bKeep;
	
	}


	private void getBypassCriteria() {

		Connection conn = null; 
	    
		try
		{

			// create a java mysql database connection
			String myDriver = "org.gjt.mm.mysql.Driver";
			String url = "jdbc:mysql://" + dbUrl + "/" + schema;
			Class.forName(myDriver);
			conn = DriverManager.getConnection(url, dbUser, dbPw);

			log.info("Starting bypass queries");
			
			String sSQL1 = "SELECT member_id, totalamt from  ( (SELECT member_id, SUM(allowed_amt) as totalamt " +
							"FROM " + tableName + " " + 
							"GROUP BY member_id WITH ROLLUP) AS T)  WHERE totalamt > '1000000' OR totalamt < '0'";
					
			Statement stmt = conn.createStatement();
			
            ResultSet result = stmt.executeQuery(sSQL1);
            while(result.next()){
            	MemberBypassList.add(result.getString("member_id"));
            	log.info("Removing member " + result.getString("member_id") + " for total amount " + result.getString("totalamt"));
            }
            
            String sSQL2 = "SELECT member_id, claim_id, totalamt from  ( (SELECT member_id, claim_id, SUM(allowed_amt) as totalamt " + 
            				"FROM " + tableName + " " +  
            				"GROUP BY claim_id WITH ROLLUP) AS T)  WHERE totalamt > '1000000' OR totalamt < '0'";	
            
            result = stmt.executeQuery(sSQL2);
            String _member;
            ArrayList<String> _l;
            while(result.next()){
            	_member = result.getString("member_id");
            	_l = ClaimBypassList.get(_member);
            	if (_l == null) {
            		_l = new ArrayList<String>();
            	}
            	_l.add(result.getString("claim_id"));
            	ClaimBypassList.put(_member, _l);
            	log.info("Removing member " + _member + " claim " + result.getString("claim_id") + " for total amount " + result.getString("totalamt"));
            }

		}
		catch (Exception e)
		{
			log.error("Got an exception querying " + tableName);
			log.error(e.getMessage(), e);
		}
		finally
		{
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				log.error("Got an exception closing  " + tableName);
				log.error(e.getMessage(), e);
			}
		}
		log.info("Completed bypass criteria");
		
	}


	private void openEachSlice(int chunkNo) {
		
		log.info("Starting chunk " + chunkNo);
		log.info("Selected " + MemberSelectionList.size() + " Unique Members");
		
		// OUTPUTS
		
		String sAppend = "_" + String.format("%03d", chunkNo);

		// define an extract file for the medical claims
		outputMedicalHdrClaimFile = null;
		try {
			outputMedicalHdrClaimFile = new FileWriter(parameters.get("claimhdroutput") + sAppend);
		} catch (IOException e1) {
			log.error("An error occurred while opening " + parameters.get("claimhdroutput") + sAppend);
		}
		bwMHF = new BufferedWriter(outputMedicalHdrClaimFile);
		
		// define an extract file for the medical claims
		outputMedicalClaimFile = null;
		try {
			outputMedicalClaimFile = new FileWriter(parameters.get("claimoutput") + sAppend);
		} catch (IOException e1) {
			log.error("An error occurred while opening " + parameters.get("claimoutput") + sAppend);
		}
		bwMCF = new BufferedWriter(outputMedicalClaimFile);
		
		
		// define an extract file for the member eligibility records
		outputMemberEligibilityFile = null;
		try {
			outputMemberEligibilityFile = new FileWriter(parameters.get("memberoutput") + sAppend);
		} catch (IOException e1) {
			log.error("An error occurred while opening " + parameters.get("memberoutput") + sAppend);
		}
		bwMEF = new BufferedWriter(outputMemberEligibilityFile);
		
		// define an extract file for the Rx claim records
		outputRxClaimFile = null;
		try {
			outputRxClaimFile = new FileWriter(parameters.get("rxoutput") + sAppend);
		} catch (IOException e1) {
			log.error("An error occurred while opening " + parameters.get("rxoutput") + sAppend);
		}
		bwRXF = new BufferedWriter(outputRxClaimFile);
		
		// define an extract file for the provider records
		outputProviderFile = null;
		try {
			outputProviderFile = new FileWriter(parameters.get("provideroutput") + sAppend);
		} catch (IOException e1) {
			log.error("An error occurred while opening " + parameters.get("provideroutput") + sAppend);
		}
		bwPRF = new BufferedWriter(outputProviderFile);
		
		
		// INPUTS
		
		// get a medical claims file
		medicalClaimHdrFile = new InputManager(parameters.get("claimhdrinput"));
		try {
			medicalClaimHdrFile.openFile();
		} catch (IOException | ZipException e) {
			log.error("An error occurred while opening " + parameters.get("claimhdrinput"));
		}

		// get a medical claims file
		medicalClaimFile = new InputManager(parameters.get("claiminput"));
		try {
			medicalClaimFile.openFile();
		} catch (IOException | ZipException e) {
			log.error("An error occurred while opening " + parameters.get("claiminput"));
		}
				
		// get a member eligibility file
		memberEligibilityFile = new InputManager(parameters.get("memberinput"));
		try {
			memberEligibilityFile.openFile();
		} catch (IOException | ZipException e) {
			log.error("An error occurred while opening " + parameters.get("memberinput"));
		}
				
		// get a Rx claim file
		RxClaimFile = new InputManager(parameters.get("rxinput"));
		try {
			RxClaimFile.openFile();
		} catch (IOException | ZipException e) {
			log.error("An error occurred while opening " + parameters.get("rxinput"));
		}
				
		// get a Provider file
		ProviderFile = new InputManager(parameters.get("providerinput"));
		try {
			ProviderFile.openFile();
		} catch (IOException | ZipException e) {
			log.error("An error occurred while opening " + parameters.get("providerinput"));
		}
						
		
	}
	
	
	BufferedWriter bwMHF;
	BufferedWriter bwMCF;
	BufferedWriter bwMEF;
	BufferedWriter bwRXF;
	BufferedWriter bwPRF;
	
	private void createTempDataset () {
		
		String line;
		int countMedicalClaimHeadersOutputLcl = 0;
		int countMedicalClaimRecordsOutputLcl = 0;
		int countMemberEligibilityRecordsOutputLcl = 0;
		int countRxClaimRecordsOutputLcl = 0;
		int countProviderRecordsOutputLcl = 0;
		
		// read the medical claim header records
		try {
			// header
			line = medicalClaimHdrFile.readFile();
			if (bLastPass)
				countMedicalClaimHeadersInput++;
			bwMHF.write(line);
			bwMHF.newLine();
			countMedicalClaimHeadersOutputLcl++;
			//
			while ((line = medicalClaimHdrFile.readFile()) != null)  {
				if (bLastPass)
					countMedicalClaimHeadersInput++;
				if (selectClaimHdr(line)) {
					bwMHF.write(line);
					bwMHF.newLine();
					countMedicalClaimHeadersOutputLcl++;
				}
			}
		} catch (IOException e) {
			log.error("An error occurred while reading " + parameters.get("claimhdrinput"));
		}
		
		try {
			medicalClaimHdrFile.closeFile();
		} catch (IOException e) {
			log.error("An error occurred while closing file: " + parameters.get("claimhdrinput"));
		}  
		
		try {
			bwMHF.close();
		} catch (IOException e) {
			log.error("An error occurred while closing file: " + parameters.get("claimhdroutput"));
		}
		

		// read the medical claim records
		try {
			// header
			line = medicalClaimFile.readFile();
			if (bLastPass)
				countMedicalClaimRecordsInput++;
			bwMCF.write(line);
			bwMCF.newLine();
			countMedicalClaimRecordsOutputLcl++;
			//
			while ((line = medicalClaimFile.readFile()) != null)  {
				if (bLastPass)
					countMedicalClaimRecordsInput++;
				if (selectClaim(line)) {
					bwMCF.write(line);
					bwMCF.newLine();
					countMedicalClaimRecordsOutputLcl++;
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
		
		
		// read the member eligibility records
		try {
			// header
			line = memberEligibilityFile.readFile();
			if (bLastPass)
				countMemberEligibilityRecordsInput++;
			bwMEF.write(line);
			bwMEF.newLine();
			countMemberEligibilityRecordsOutputLcl++;
			//
			while ((line = memberEligibilityFile.readFile()) != null)  {
				if (bLastPass)
					countMemberEligibilityRecordsInput++;
				if (selectMember(line)) {
					bwMEF.write(line);
					bwMEF.newLine();
					countMemberEligibilityRecordsOutputLcl++;
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
		
		
		
		
		
		// read the Rx claim records
		try {
			// header
			line = RxClaimFile.readFile();
			if (bLastPass)
				countRxClaimRecordsInput++;
			bwRXF.write(line);
			bwRXF.newLine();
			countRxClaimRecordsOutputLcl++;
			//
			while ((line = RxClaimFile.readFile()) != null)  {
				if (bLastPass)
					countRxClaimRecordsInput++;
				if (selectRx(line)) {
					bwRXF.write(line);
					bwRXF.newLine();
					countRxClaimRecordsOutputLcl++;
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
			if (bLastPass)
				countProviderRecordsInput++;
			bwPRF.write(line);
			bwPRF.newLine();
			countProviderRecordsOutputLcl++;
			//
			while ((line = ProviderFile.readFile()) != null)  {
				if (bLastPass)
					countProviderRecordsInput++;
				if (selectProvider(line)) {
					bwPRF.write(line);
					bwPRF.newLine();
					countProviderRecordsOutputLcl++;
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
		
		countMedicalClaimHeadersOutput = countMedicalClaimHeadersOutput + countMedicalClaimHeadersOutputLcl;
		countMedicalClaimRecordsOutput = countMedicalClaimRecordsOutput + countMedicalClaimRecordsOutputLcl;
		countRxClaimRecordsOutput = countRxClaimRecordsOutput + countRxClaimRecordsOutputLcl;
		countMemberEligibilityRecordsOutput = countMemberEligibilityRecordsOutput + countMemberEligibilityRecordsOutputLcl;
		countProviderRecordsOutput = countProviderRecordsOutput + countProviderRecordsOutputLcl;
		
		log.info("Wrote " + countMedicalClaimHeadersOutputLcl + " Medical Header Records");		
		log.info("Wrote " + countMedicalClaimRecordsOutputLcl + " Medical Claim Records");
		log.info("Wrote " + countRxClaimRecordsOutputLcl + " Rx Claim Records");
		log.info("Wrote " + countMemberEligibilityRecordsOutputLcl + " Member Eligibility Records");
		log.info("Wrote " + countProviderRecordsOutputLcl + " Provider Records");
		
		
	}
	
	/**
	 * select a number of members and all of their claim header records
	 * keep track of who is being selected in the MemberSelectionList
	 * @param s
	 * @return
	 */
	private boolean selectClaimHdr (String s) {
		
		boolean bResult = false;
		
		String[] in = s.split(sDelimiter);
		String sMemberId = in[0];
		String sProvider = in[4];
		
		if (!checkBypass(in)) {
			bResult = false;
		}
		
		
		else if (isInMemberList(sMemberId)) {
			if (!isInProviderList(sProvider))
				ProviderSelectionList.add(sProvider);
			bResult = true;
		}
		/*
		else if(MemberSelectionList.size() < Integer.parseInt(parameters.get("count"))) {
			MemberSelectionList.add(sMemberId);
			if (!isInProviderList(sProvider))
				ProviderSelectionList.add(sProvider);
			bResult = true;
		}
		*/
		else
			bResult = false;
			
		
		return bResult;
		
	}
	
	/**
	 * select claim detail records for members already placed in MemberSelectionList
	 * @param s
	 * @return
	 */
	private boolean selectClaim (String s) {
		
		boolean bResult = false;
		
		String[] in = s.split(sDelimiter);
		String sMemberId = in[0];
		
		if (!checkBypass(in)) {
			bResult = false;
		}
		
		
		else if (isInMemberList(sMemberId))
			bResult = true;
		
		return bResult;
		
	}
	
	

	/**
	 * select member records for members already placed in MemberSelectionList
	 * @param s
	 * @return
	 */
	private boolean selectMember (String s) {
		
		boolean bResult = false;
		
		String[] in = s.split(sDelimiter);
		String sMemberId = in[0];
		
		if (isInMemberList(sMemberId))
			bResult = true;
		
		return bResult;
		
	}
	
	
	
	private boolean selectRx (String s) {
		
		boolean bResult = false;
		
		String[] in = s.split(sDelimiter);
		String sMemberId = in[0];
		
		if (isInMemberList(sMemberId))
			bResult = true;
		
		return bResult;

	}
	
	
	private boolean selectProvider (String s) {
		
		boolean bResult = false;
		
		String[] in = s.split(sDelimiter);
		
		String sProviderId = in[0];
		
		// select only providers found in claims
		if (isInProviderList(sProviderId))
			bResult = true;
		
		return bResult;
		
	}
	
	
	
	private boolean isInMemberList (String s) {
		//boolean b = false;
		return MemberSelectionList.contains(s);
		/*
		for (String x : MemberSelectionList) {
			if (x.equals(s)) {
				b = true;
				break;
			}
		}
		return b;
		*/
	}
	
	
	
	/*
	private boolean isInMemberList (String s) {
		boolean b = false;
		for (String x : MemberList) {
			if (x.equals(s)) {
				b = true;
				break;
			}
		}
		return b;
	}
	*/
	

	private boolean isInProviderList (String s) {
		/*
		boolean b = false;
		for (String x : ProviderSelectionList) {
			if (x.equals(s)) {
				b = true;
				break;
			}
		}
		return b;
		*/
		return ProviderSelectionList.contains(s);
	}
	

	
	public static void main(String[] args) throws IOException {
		
		SplitterWellpoint instance = new SplitterWellpoint();
		
		// get parameters (if any)
		instance.loadParameters(args);
		
		if (instance.parameters.get("runname") == null)
			instance.parameters.put("runname", "sample");
		
		// process
		instance.process();

	}
	
	HashMap<String, String> parameters = new HashMap<String, String>();
	String [][] parameterDefaults = {
			{"divisor", "4"}
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
	
	
	private static org.apache.log4j.Logger log = Logger.getLogger(SplitterWellpoint.class);
	
	HashSet<String> FullMemberList = new HashSet<String>();
	HashSet<String> MemberSelectionList = new HashSet<String>();
	HashSet<String> ProviderSelectionList = new HashSet<String>();
	
	HashSet<String> MemberBypassList = new HashSet<String>();
	HashMap<String, ArrayList<String>> ClaimBypassList = new HashMap<String, ArrayList<String>>();
	// ten members with total cost less than zero
	/*
	private String [] MemberList = {
			"251870340",
			"184085073",
			"245365451",
			"172361806",
			"245343071",
			"246525088",
			"252009695",
			"184085076",
			"244588415",
			"173765857"
	};
	*/
	/*
	private String [] MemberList = {
			"100006092"
	};
	*/
	

}
