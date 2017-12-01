



public class QuickMemberSelect {
	
	public String sDelimiter = "\\*|\\||;|,";
	
	private int debugLimit = 10;
	private static final String carrierId = "291";
	
	private BufferedWriter bwMEF;
	
	private int countMedicalClaimRecordsInput = 0;
	private int countMedicalClaimRecordsOutput = 0;
	private int countMemberEligibilityRecordsInput = 0;
	private int countMemberEligibilityRecordsOutput = 0;
	private int countUniqueMembersOutput = 0;
	private int countRxClaimRecordsInput = 0;
	private int countRxClaimRecordsOutput = 0;
	private int countProviderRecordsInput = 0;
	private int countProviderRecordsOutput = 0;
	
	
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
				//if (selectMember(line)) {
				//	bwMEF.write(line);
				//	bwMEF.newLine();
				//	countMemberEligibilityRecordsOutput++;
				//}
				accumulateMember(line);
				countMemberEligibilityRecordsInput++;
			}
			for (Map.Entry<String, ArrayList<String>> entry : memberAccumulator.entrySet()) {
			    //System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
				//selectMemberByDate(entry);
				selectMemberByList(entry);
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
		
		log.info("Selected " + countUniqueMembersOutput + " Unique Members");
		
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
		log.info("Selected " + countUniqueMembersOutput + " Unique Members");
		log.info("Read " + countMedicalClaimRecordsInput + " Medical Claim Records");
		log.info("Wrote " + countMedicalClaimRecordsOutput + " Medical Claim Records");
		log.info("Read " + countRxClaimRecordsInput + " Rx Claim Records");
		log.info("Wrote " + countRxClaimRecordsOutput + " Rx Claim Records");
		log.info("Read " + countProviderRecordsInput + " Provider Records");
		log.info("Wrote " + countProviderRecordsOutput + " Provider Records");
		
	}
	
	HashMap<String, String> memberEID = new HashMap<String, String>();
	HashMap<String, ArrayList<String>> memberAccumulator = new HashMap<String, ArrayList<String>>();
	
	private boolean selectClaim (String s) {
		
		boolean bResult = false;
		
		String[] in = s.split(sDelimiter);
		String sOrgId = in[11];
		// select only for specified carrier
		if (sOrgId.equals(carrierId))
		{
			
			// column 9 is the MemberLinkEID
			String sMemberEID = in[9];
			bResult = isInList(sMemberEID);
		
			// when eligibility is the driver:
			/*
			if (memberEID.containsKey(sMemberEID))
				bResult = true;
				*/
			
			// when claim is the driver:
			/*
			if (sMemberEID.isEmpty())
				bResult = false;
			else if (memberEID.containsKey(sMemberEID))
				bResult = true;
			else if (memberEID.size() < limit) {
				bResult = true;
				memberEID.put(sMemberEID, sMemberEID);
			}
			*/
		}
		
		return bResult;
		
	}
	
	private boolean accumulateMember (String s) {
		
		boolean bResult = false;
		
		String[] in = s.split(sDelimiter);
		
		String sOrgId = in[8];
		
		// select only for specified carrier
		if (sOrgId.equals(carrierId))
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
				if (memberAccumulator.containsKey(sMemberEID)) {
					memberAccumulator.get(sMemberEID).add(s);
				}
				else {
					currentMemberRecords = new ArrayList<String>();
					currentMemberRecords.add(s);
					memberAccumulator.put(sMemberEID, currentMemberRecords);
				}
				bResult = true;
			}
			
			
			// when claims are the driver:
			/*
			if (memberEID.containsKey(sMemberEID))
				bResult = true;
				*/
			
		}
		
		return bResult;
		
	}
	
	private ArrayList<String> currentMemberRecords = new ArrayList<String>();
	Date eStartDate;
	Date eEndDate;
	SimpleDateFormat sdf; 
	

	/*
	private void selectMemberByDate(Entry<String, ArrayList<String>> entry) {
		
		String[] in;
		Date chkDt;
		eStartDate = dNew;
		eEndDate = dOld;
		currentMemberRecords = entry.getValue();
		
		// eligibility start date is in 21, end date in 22
		for(String s:currentMemberRecords) {
			in = s.split(CommonInputDataAbstract.sDelimiter);
			if (sdf == null) {
				sdf = new SimpleDateFormat(DateUtil.determineDateFormat(in[21]));
			}
			try {
				chkDt = sdf.parse(in[21]);
				if (chkDt.before(eStartDate))
					eStartDate = chkDt;
				chkDt = sdf.parse(in[22]);
				if (chkDt.after(eEndDate))
					eEndDate = chkDt;
			} catch (ParseException e) {
				log.error("Something went wrong parsing incoming dates" + e);
				e.printStackTrace();
			}
		}
		
				
		if ( (!dRangeStart.before(eStartDate))  &&  (!dRangeEnd.after(eEndDate)))
		{
			in = currentMemberRecords.get(0).split(CommonInputDataAbstract.sDelimiter);
			memberEID.put(entry.getKey(), entry.getKey());
			if(countUniqueMembersOutput < debugLimit)
				log.info("Selected " + entry.getKey());
			countUniqueMembersOutput++;
			
			int x=0;
			for(String s:currentMemberRecords) {
				try {
					x++;
					if(countUniqueMembersOutput < debugLimit)
						log.info("  record " + x + ":" + s);
					bwMEF.write(s);
					bwMEF.newLine();
				} catch (IOException e) {
					log.error("An error occurred while trying to write the member eligibility file" + e);
					e.printStackTrace();
				}
				countMemberEligibilityRecordsOutput++;
			}
		}
	}
	*/
	
	private void selectMemberByList(Entry<String, ArrayList<String>> entry) {
		
		if(isInList(entry.getKey()) ) {

			currentMemberRecords = entry.getValue();
		
			int x=0;
			for(String s:currentMemberRecords) {
				try {
					x++;
					if(countUniqueMembersOutput < debugLimit)
						log.info("  record " + x + ":" + s);
					bwMEF.write(s);
					bwMEF.newLine();
				} catch (IOException e) {
					log.error("An error occurred while trying to write the member eligibility file" + e);
					e.printStackTrace();
				}
				countMemberEligibilityRecordsOutput++;
			}
		}
	}
	
	private boolean isInList (String s) {
		boolean b = false;
		for (int i=0; i < MemberSelectionList.length; i++) {
			if (MemberSelectionList[i].equals(s)) {
				b = true;
				break;
			}
		}
		return b;
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
			bResult = isInList(sMemberEID);
			//if (memberEID.containsKey(sMemberEID))
			//	bResult = true;
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
		
		QuickMemberSelect instance = new QuickMemberSelect();
		
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
	Date dOld, dNew;
	Date dRangeStart, dRangeEnd;
	
	
	private static org.apache.log4j.Logger log = Logger.getLogger(QuickMemberSelect.class);

	
	private final static String MemberSelectionList [] =  {
		"41151",
		"91594",
		"109411",
		"205598",
		"459839",
		"802283",
		"1085262",
		"2159970",
		"2257644",
		"3661689",
		"3926242",
		"5129581",
		"5859629",
		"7088520",
		"8602808",
		"8716969",
		"1081277",
		"109089",
		"1170096",
		"1182437",
		"1397718",
		"1560465",
		"1583313",
		"1738540",
		"1747063",
		"1796280",
		"1865338",
		"1867709",
		"1878787",
		"1958898",
		"197675",
		"1998738",
		"2094398",
		"2104646",
		"2265576",
		"2293451",
		"2297192",
		"2304406",
		"2327391",
		"2366940",
		"2374317",
		"2395138",
		"2448403",
		"24485011",
		"2466567",
		"2511158",
		"2542357",
		"2584154",
		"2718571",
		"2740060",
		"2880057",
		"2903634",
		"291546",
		"2950442",
		"300171",
		"3053431",
		"3066220",
		"3137560",
		"3153618",
		"3165856",
		"3201213",
		"3201528",
		"3265052",
		"3316778",
		"3338809",
		"3367212",
		"3441580",
		"3525197",
		"353096",
		"3600536",
		"3634828",
		"3643349",
		"3645372",
		"3661750",
		"3678903",
		"3679160",
		"3735059",
		"3757725",
		"3791618",
		"3820673",
		"3833159",
		"3843726",
		"3846250",
		"3863337",
		"3921221",
		"3926061",
		"3960304",
		"4076394",
		"4110434",
		"4116410",
		"416306",
		"4267120",
		"4275287",
		"4356955",
		"4444580",
		"4466906",
		"4553997",
		"4644408",
		"4676227",
		"4793670",
		"482128",
		"4879608",
		"4966969",
		"5075540",
		"5104720",
		"5114606",
		"5126622",
		"5257749",
		"5314918",
		"5514317",
		"5675660",
		"5677951",
		"5679777",
		"5680510",
		"5715158",
		"5736317",
		"5775850",
		"5811380",
		"583363",
		"5892157",
		"5947949",
		"5983385",
		"5983799",
		"6047037",
		"6095536",
		"6143504",
		"6201733",
		"620708",
		"6227029",
		"6316170",
		"6403446",
		"6409481",
		"6413569",
		"65600",
		"6644566",
		"6666946",
		"6749546",
		"6780684",
		"6838554",
		"6873297",
		"6892272",
		"7009433",
		"7022048",
		"7023163",
		"7215216",
		"7226469",
		"7263218",
		"7268291",
		"7338618",
		"7369618",
		"7419250",
		"7472638",
		"751586",
		"7560324",
		"7575146",
		"759678",
		"7722746",
		"773220",
		"7740160",
		"7761658",
		"7773629",
		"7801708",
		"8161900",
		"8237324",
		"8293914",
		"8304641",
		"834568",
		"8364380",
		"8375181",
		"8551116",
		"8593071",
		"8669184",
		"8726230",
		"8749804",
		"8766887",
		"8873431",
		"8922832",
		"9006041",
		"9046489",
		"9051161",
		"9059117",
		"9063626",
		"9069256",
		"9084562",
		"9116773",
		"9182204",
		"9217274",
		"9302924",
		"9320529",
		"9346684",
		"9372631",
		"9409467",
		"948350",
		"9641659",
		"9656620",
		"9723173",
		"9727484",
		"979982",
		"9863095",
		"9889876"

	};


}
