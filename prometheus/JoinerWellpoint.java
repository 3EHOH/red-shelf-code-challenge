





public class JoinerWellpoint {
	
	
	
	HashMap<String, String> parameters;
	
	InputManager medicalClaimHdrFile;
	InputManager medicalClaimFile;
	
	FileWriter outputMedicalClaimFile;
	BufferedWriter bwMCF;
	String line;
	String [] in;
	StringBuilder sb = new StringBuilder();
	
	/*
	 * pre-201609 feed
	 * // CLMKEYH is in column 29 (relative to zero)
	 * int iHeaderKey = 29;
	 */
	// Claim Nbr is in column 1 (relative to zero)
	//int iHeaderKey = 1;
	String hdrColumnHeaderImage;
	HashMap<String, String> ClaimHeaders = new HashMap<String, String>();

	/*
	 * pre-201609 feed
	 * // CLMKEYD is in column 45 (relative to zero)
	 * int iDetailKey = 45;
	 */
	// Claim Nbr is in column 1 (relative to zero)
	//int iDetailKey = 1;
	
	String [] matchFlds = {"Master Consumer ID", "Claim Nbr", "Claim Adjustment #"};
				
	HashMap<String, Integer> hdrFldNames = new HashMap<String, Integer>();
	HashMap<String, Integer> dtlFldNames = new HashMap<String, Integer>();
	HashMap<Integer, Boolean> fldControl = new HashMap<Integer, Boolean>();
	
			
	JoinerWellpoint (HashMap<String, String> parameters) {
		this.parameters = parameters;
		initialize();
		process();
		decommission();
	}
	
	
	private void process ()  {
		
		
		// read the medical claim header records, and place in a hash map
		try {
			
			// column header
			hdrColumnHeaderImage = medicalClaimHdrFile.readFile();
			setHdrFldNames();
			
			// save header line images
			while ((line = medicalClaimHdrFile.readFile()) != null)  {
				in = line.split("\\|");
				ClaimHeaders.put(bldHdrKey(in), line);
			}
		} catch (IOException e) {
			log.error("An error occurred while reading " + parameters.get("header_in"));
		}
		
		
		// read the details, grab the header from the hash map, concatenate and write
		try {
			
			// column header
			line = medicalClaimFile.readFile();
			setDtlFldNames();
			setDtlFldUsage();
			
			bwMCF.write(hdrColumnHeaderImage + parseDtl());
			bwMCF.newLine();

			// 
			while ((line = medicalClaimFile.readFile()) != null)  {
				in = line.split("\\|");
				
				bwMCF.write(ClaimHeaders.get(bldDtlKey(in)) + parseDtl());
				bwMCF.newLine();

			}
		} catch (IOException e) {
			log.error("An error occurred while reading " + parameters.get("detail_in"));
		}  
		
		
	}
	
	
	private String bldHdrKey (String [] in) {
		
		sb.setLength(0);
		for (int i=0; i < matchFlds.length; i++) {
			sb.append(in[hdrFldNames.get(matchFlds[i])]).append('|');
		}
		return sb.toString();
		
	}
	
	
	private String bldDtlKey (String [] in) {
		
		sb.setLength(0);
		for (int i=0; i < matchFlds.length; i++) {
			sb.append(in[dtlFldNames.get(matchFlds[i])]).append('|');
		}
		return sb.toString();
		
	}
	
	private String parseDtl() {
		
		sb.setLength(0);
		String [] cols = line.split("\\|");
		for (int i=0; i < cols.length; i++) {
			if (fldControl.get(i)) {
				sb.append(cols[i]).append('|');
			}
		}
		
		if (sb.length() > 0)
			sb.setLength(sb.length() -1);
		
		return sb.toString();
		
	}


	/*
	 * save header field names so we can avoid taking fields of the same name from detail
	 */
	private void setHdrFldNames() {

		String [] cols = hdrColumnHeaderImage.split("\\|");
		for (int i=0; i < cols.length; i++) {
			hdrFldNames.put(cols[i], i);
		}
		
	}

	
	private void setDtlFldNames() {

		String [] cols = line.split("\\|");
		for (int i=0; i < cols.length; i++) {
			dtlFldNames.put(cols[i], i);
		}
		
	}

	
	/*
	 * flag detail fields that already exist in the header
	 */
	private void setDtlFldUsage() {

		String [] cols = line.split("\\|");
		for (int i=0; i < cols.length; i++) {
			fldControl.put(new Integer(i), hdrFldNames.containsKey(cols[i]) ? Boolean.FALSE : Boolean.TRUE);
		}
		
	}


	private void initialize ()  {
		
		// get the medical claims header file
		medicalClaimHdrFile = new InputManager(parameters.get("header_in"));
		try {
			medicalClaimHdrFile.openFile();
		} catch (IOException | ZipException e) {
			log.error("An error occurred while opening " + parameters.get("header_in"));
		}

		// get the medical claims detail file
		medicalClaimFile = new InputManager(parameters.get("detail_in"));
		try {
			medicalClaimFile.openFile();
		} catch (IOException | ZipException e) {
			log.error("An error occurred while opening " + parameters.get("detail_in"));
		}
		
		// define an extract file for the medical claims
		outputMedicalClaimFile = null;
		try {
			outputMedicalClaimFile = new FileWriter(parameters.get("joined_out"));
		} catch (IOException e1) {
			log.error("An error occurred while opening " + parameters.get("joined_out"));
		}
		bwMCF = new BufferedWriter(outputMedicalClaimFile);
		
	}
	
	
	private void decommission ()  {
		
		try {
			bwMCF.close();
		} catch (IOException e) {
			log.error("An error occurred while closing file: " + parameters.get("joined_out"));
		}
		
		try {
			medicalClaimHdrFile.closeFile();
		} catch (IOException e) {
			log.error("An error occurred while closing file: " + parameters.get("header_in"));
		}  
		
		try {
			medicalClaimFile.closeFile();
		} catch (IOException e) {
			log.error("An error occurred while closing file: " + parameters.get("detail_in"));
		}  
		
	}
	
	
	public static void main(String[] args) {
		
		HashMap<String, String> parameters = RunParameters.parameters;
		RunParameters rp = new RunParameters();
		parameters = rp.loadParameters(args, parameterDefaults);
		
		new JoinerWellpoint(parameters);
		
	}
	
	
	static String [][] parameterDefaults = {
		{"header_in", "C:\\input\\Wellpoint\\HCI3_CT_ALLIANCE_claim_header_08112016.txt"},
		{"detail_in", "C:\\input\\Wellpoint\\HCI3_CT_ALLIANCE_claim_det_08112016.txt"},
		{"joined_out", "C:\\input\\Wellpoint\\HCI3_CT_ALLIANCE_claim_Joined.txt"}
	};
	
	

	
	private static org.apache.log4j.Logger log = Logger.getLogger(JoinerWellpoint.class);
	
	

}
