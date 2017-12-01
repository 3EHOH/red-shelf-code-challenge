

public class FileRepair {
	
public static void main(String[] args) {
		
		loadParameters(args);
		
				
		FileReader inputFile = null;
		FileWriter outputFile = null;
		try {
			inputFile = new FileReader(INPUT_FILE_NAME);
			outputFile = new FileWriter(OUTPUT_FILE_NAME);
		} catch (FileNotFoundException e) {
			throw new IllegalStateException("Failure occurred trying to read " + inputFile);
		} catch (IOException e) {
			throw new IllegalStateException("Failure occurred trying to write " + outputFile);
		}
		BufferedReader br = new BufferedReader(inputFile); 
		BufferedWriter bw = new BufferedWriter(outputFile);
		String line;
		int count = 0;
		
		// read the lines and print
		try {
			//while ((line = br.readLine()) != null)  {
			while ((line = readLimitedLine(br, lineLengthLimit)) != null)  {
				if (bDone)
					break;
				if (count == 0)
					bw.write(header);
				else
					bw.write(line);
		        bw.newLine();
				count++;
			}
			br.close();
			bw.close();
		} catch (IOException e) {
			throw new IllegalStateException("An error occurred while processing " + inputFile);
		}  
		
		System.out.println("Record count: " + count);
		
	}

	public static String readLimitedLine(Reader reader, int limit) 
        throws IOException {

		StringBuilder sb = new StringBuilder();
		int i = 0;
		for (i = 0; i < limit; i++) {
			int c = reader.read();
			if (c == -1) {
				return ((sb.length() > 0) ? sb.toString() : null);
			}
			if (((char) c == '\n') || ((char) c == '\r')) {
				break;
			}
			sb.append((char) c);
		}
		
		if (! (i < lineLengthLimit)) {
			bDone = true;
			System.out.println("Last Good Record: " + sLastGoodRecord);
		}
		
		return (sLastGoodRecord = sb.toString());
	}



	public static final int lineLengthLimit = 4096;
	
	static String sLastGoodRecord;
	static boolean bDone = false;


	static private void loadParameters (String[] args) {
		// load any default parameters from the default parameter array
		for (int i = 0; i < parameterDefaults.length; i++) {
			parameters.put(parameterDefaults[i][0], parameterDefaults[i][1]);
		}
		// overlay or add any incoming parameters
		for (int i = 0; i < args.length; i++) {
			parameters.put(args[i].substring(0, args[i].indexOf('=')), args[i].substring(args[i].indexOf('=')+1)) ;
		}
		

			

		
	}
	
	final static String header = "ReleaseID*SubmissionMonth*SubmissionYear*Standardized_MemberCounty*Standardized_" +
			"MemberZIPFirst3*SubmissionControlID*IncurredDate*MedicaidIndicator*HighestVersio" +
			"nDenied*MemberLinkEID*MemberLinkMCL*OrgID*PayerClaimControlNumber*LineCounter*Ve" +
			"rsionNumber*MemberGenderCleaned*MemberDateofBirthYearCleaned*Standardized_Member" +
			"ZIPCode*AdmissionDate*AdmissionType*AdmissionSource*DischargeStatus*ServiceProvi" +
			"derNumber_Linkage_ID*NationalServiceProviderIDCleaned*ServiceProviderEntityTypeQ" +
			"ualifier*ServiceProviderSpecialty*Standardized_ServiceProviderCityName*Standardi" +
			"zed_ServiceProviderState*Standardized_ServiceProviderZIPCode*TypeofBillOnFacilit" +
			"yClaims*SiteofServiceOnNSFCMS1500ClaimsCleaned*ClaimStatus*AdmittingDiagnosisCle" +
			"aned*ECodeCleaned*PrincipalDiagnosisCleaned*OtherDiagnosis1Cleaned*OtherDiagnosi" +
			"s2Cleaned*OtherDiagnosis3Cleaned*OtherDiagnosis4Cleaned*OtherDiagnosis5Cleaned*O" +
			"therDiagnosis6Cleaned*OtherDiagnosis7Cleaned*OtherDiagnosis8Cleaned*OtherDiagnos" +
			"is9Cleaned*OtherDiagnosis10Cleaned*OtherDiagnosis11Cleaned*OtherDiagnosis12Clean" +
			"ed*RevenueCodeCleaned*ProcedureCodeCleaned*ProcedureModifier1*ProcedureModifier2" +
			"*ICD9CMProcedureCodeCleaned*DateofServiceFrom*DateofServiceTo*Quantity*Discharge" +
			"Date*DRG*DRGVersion*APC*APCVersion*DrugCode*BillingProviderNumber_Linkage_ID*Nat" +
			"ionalBillingProviderIDCleaned*CapitatedEncounterFlag*OtherICD9CMProcedureCode1Cl" +
			"eaned*OtherICD9CMProcedureCode2Cleaned*OtherICD9CMProcedureCode3Cleaned*OtherICD" +
			"9CMProcedureCode4Cleaned*OtherICD9CMProcedureCode5Cleaned*OtherICD9CMProcedureCo" +
			"de6Cleaned*TypeOfClaimCleaned*AllowedAmountCleaned*ReferringProviderID_Linkage_I" +
			"D*PaymentArrangementTypeCleaned*MedicareIndicator*ReferralIndicator*PCPIndicator" +
			"*DRGLevel*GlobalPaymentFlag*DeniedFlag*ProcedureCodeType*HashCarrierSpecificUniq" +
			"ueMemberIDCleaned*ClaimLineType";
	
	final static String INPUT_FILE_NAME = "V:\\72_HCI3_ECR Data\\72_HCL3_ECR_MedicalClaim_2.txt";
	final static String OUTPUT_FILE_NAME = "V:\\72_HCI3_ECR Data\\72_HCL3_ECR_MedicalClaim_2(Repair).txt";

	
	static HashMap<String, String> parameters = new HashMap<String, String>();
	static String [][] parameterDefaults = {
			{"count", "20"}
	};
	
	//private static org.apache.log4j.Logger log = Logger.getLogger(QuickRead.class);


}
