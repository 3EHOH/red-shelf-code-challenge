



public class QuickCarrierSelect {
	
	public String sDelimiter = "\\*|\\||;|,";

	//private int limit = 1000;
		//private int debugLimit = 10;
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
					if (selectMember(line)) {
						bwMEF.write(line);
						bwMEF.newLine();
						countMemberEligibilityRecordsOutput++;
					}
					countMemberEligibilityRecordsInput++;
				}
				//selectMemberByDate();
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
			
			log.info("Selected " + memberEID.size() + " Unique Members");
			
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
		
		
		private boolean selectClaim (String s) {
			
			boolean bResult = false;
			
			String[] in = s.split(sDelimiter);
			String sOrgId = in[11];
			String sSubmission = in[1];
			// select only for specified carrier
			if (sOrgId.equals(carrierId))
			{
				
				// column 9 is the MemberLinkEID
				String sMemberEID = in[9];
			
				// when eligibility is the driver:
				if (sSubmission == null  || sSubmission.startsWith("200") || sSubmission.equals("2010"))
					bResult = false;
				else if (memberEID.containsKey(sMemberEID))
					bResult = true;
				
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
		
		private boolean selectMember (String s) {
			
			boolean bResult = false;
			
			String[] in = s.split(sDelimiter);
			
			String sOrgId = in[8];
			
			// select only for specified carrier
			if (sOrgId.equals(carrierId))
			{
			
				String sMemberEID = in[6];
				String sMedicalCoverage = in[12];
				String sSubmission = in[1];
				
				/*
				if (previousMemberId == null)
					previousMemberId = sMemberEID;
				
				if ( !sMemberEID.equals(previousMemberId) ) {
					//selectMemberByDate();
					//currentMemberRecords.clear();
					previousMemberId = sMemberEID;
				}
				*/
				
				
				// column 6 is the MemberLinkEID
				// when member eligibility file is the driver:
				if (sSubmission == null  || sSubmission.startsWith("200") || sSubmission.equals("2010"))
					bResult = false;
				else if (sMemberEID.isEmpty()) {
					//if(countUniqueMembersOutput < debugLimit) {
					//	log.info("Member " + sMemberEID + " empty id: " + s);
					//}
					bResult = false;
				}
				else if (!sMedicalCoverage.equals("1")) {
					//if(countUniqueMembersOutput < debugLimit) {
					//	log.info("Member " + sMemberEID + " medical coverage not 1: " + s);
					//}
					bResult = false;
				}
				else {
					//currentMemberRecords.add(s);
					bResult = true;
					memberEID.put(in[6], in[6]);
				}
				
				//if(bResult && countUniqueMembersOutput < debugLimit) {
				//	log.info("Member " + sMemberEID + " now has " + currentMemberRecords.size() + " records" );
				//}
			
				// when claims are the driver:
				/*
				if (memberEID.containsKey(sMemberEID))
					bResult = true;
					*/
				
			}
			
			return bResult;
			
		}
		
		//private String previousMemberId = null;
		//private ArrayList<String> currentMemberRecords = new ArrayList<String>();
		Date eStartDate;
		Date eEndDate;
		SimpleDateFormat sdf; 
		

		/**
		private void selectMemberByDate() {
			
			String[] in;
			Date chkDt;
			eStartDate = dNew;
			eEndDate = dOld;
			
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
			
			if(countUniqueMembersOutput < debugLimit)
				log.info("Selection criteria for " + previousMemberId + ": " + eStartDate + ", " + eEndDate);
			
			if ( (!dRangeStart.before(eStartDate))  &&  (!dRangeEnd.after(eEndDate)))
			{
				in = currentMemberRecords.get(0).split(CommonInputDataAbstract.sDelimiter);
				memberEID.put(in[6], in[6]);
				if(countUniqueMembersOutput < debugLimit)
					log.info("Selected " + in[6]);
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
			
			QuickCarrierSelect instance = new QuickCarrierSelect();
			
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
				// TODO Auto-generated catch block
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
		
		
		private static org.apache.log4j.Logger log = Logger.getLogger(QuickCarrierSelect.class);



	
	
}
