




public class InputManager {
	
	private boolean bIsZip = false;
	private String fileName;
	private ZipFile zf;
	private List<?> fileHeaderList;
	private FileHeader fileHeader;
	private BufferedReader in;
	
	
	public InputManager (String fileName) {
		this.fileName = fileName;
		if (fileName.toLowerCase().endsWith("zip")  ||
				fileName.toLowerCase().endsWith("tar")  ||
				fileName.toLowerCase().endsWith("gz"))
			bIsZip = true;
		this.password = RunParameters.parameters.get("password");
	}
	
	public void openFile () throws IOException, ZipException {
		//log.info("pw: " + password);
		if (bIsZip) {
			zf = new ZipFile(fileName);
			if (zf.isEncrypted()  &&  password == null) {
				log.error("Need password for encrypted zip file");
				throw new IllegalStateException("Need password for encrypted zip file");
			}
			if (zf.isEncrypted())
				zf.setPassword(password);
			fileHeaderList = zf.getFileHeaders();
			fileHeader = (FileHeader)fileHeaderList.get(0);
			//ze = (ZipEntry) zf.entries().nextElement();
			in = new BufferedReader(new InputStreamReader(zf.getInputStream(fileHeader)));
		}
		else {
			in  = new BufferedReader(new InputStreamReader(new FileInputStream(fileName)));
		}
	}
	
	String _line;
	private boolean bPreserveQuotes = true;
	
	public String readFile () throws IOException
	{
		_line = in.readLine();
		if (bPreserveQuotes)
			return _line;
		else
			return _line == null ? _line : _line.replace("\"", "");
	}
	
	public void closeFile () throws IOException {
		in.close();
		if (bIsZip) {
			//zf.close();
		}
	}
	
	public void setPassword(String password) {
		this.password = password;
	}
	
	public void setPreserveQuotes (boolean b) {
		bPreserveQuotes = b;
	}
	
	
	public static ArrayList<String> getInputFileNames (HashMap<String, String> parameters) {
		
		ArrayList<String> foundFileParms = new ArrayList<String>();
		
		Iterator<Entry<String, String>> it = parameters.entrySet().iterator();
	    while (it.hasNext()) {
	    	Map.Entry<String, String> pair = (Map.Entry<String, String>)it.next();
	    	String sFile;
	    	for (String [] sPair:inputFileParameterNames) {
	    		sFile = sPair[0];
	    		if (pair.getKey().matches( "(?i)" + sFile ) ) {
	    			foundFileParms.add(pair.getValue());
	    			log.info("Found parameter for: " + sFile);
	    			break;
		    	}
	    	}
	    }
	    
	    return foundFileParms;
	    
	}
	
	

	public static ArrayList<String[]> getInputFileNamesAndObjectNames (HashMap<String, String> parameters) {
		
		ArrayList<String[]> foundFileParms = new ArrayList<String[]>();
		
		Iterator<Entry<String, String>> it = parameters.entrySet().iterator();
	    while (it.hasNext()) {
	    	Map.Entry<String, String> pair = (Map.Entry<String, String>)it.next();
	    	String sFile;
	    	String [] sFound = {"", ""};
	    	for (String [] sPair:inputFileParameterNames) {
	    		sFile = sPair[0];
	    		if (pair.getKey().matches( "(?i)" + sFile ) ) {
	    			sFound [0] = pair.getValue();
	    			sFound [1] = sPair[1];
	    			foundFileParms.add(sFound);
	    			log.info("Found parameter for: " + sFile);
	    			break;
		    	}
	    	}
	    }
	    
	    return foundFileParms;
	    
	}
	
	
	private static final String [] [] inputFileParameterNames = {
		{"claim_hdr_file", null},
		{"claim_file", construction.model.ClaimServLine.class.getName() },
		{"addl_claim_file", construction.model.ClaimServLine.class.getName() },
		{"prof_file", construction.model.ClaimServLine.class.getName() },
		{"stay_file", construction.model.ClaimServLine.class.getName() },
		{"claim_file\\d{1,2}", construction.model.ClaimServLine.class.getName() },
		//"claim_file2",
		//"claim_file3",
		{"enroll_file", construction.model.Enrollment.class.getName() },
		{"enroll_file\\d{1,2}", construction.model.Enrollment.class.getName() },
		//"enroll_file2",
		//"enroll_file3",
		{"member_file", construction.model.PlanMember.class.getName()},
		{"member_file\\d{1,2}", construction.model.PlanMember.class.getName()},
		//"member_file2",
		//"member_file3",
		{"claim_rx_file", construction.model.ClaimRx.class.getName()},
		{"claim_rx_file\\d{1,2}", construction.model.ClaimRx.class.getName()},
		//"claim_rx_file2",
		//"claim_rx_file3",
		{"provider_file", construction.model.Provider.class.getName()},
		{"provider_file\\d{1,2}",  construction.model.Provider.class.getName()}
		//"provider_file2",
		//"provider_file3"
	};
	
	
	
 	
	
	/**
	 * a main strictly for testing
	 * @param args
	 */
	public static void main(String[] args) {
		
		String s;
		int i=0;
		
		InputManager instance = new InputManager(TEST_ZIP_FILE);
		instance.setPassword("shp123");
		try {
			instance.openFile();
			while ((s = instance.readFile()) != null){
				if (i<100)
					log.info(s);
				i++;
			}
			instance.closeFile();
		} catch (Throwable e) {
			{log.info(e);}	// this main is just for testing - I don't care
		}
		
		log.info("Found " + i + " records in zip file ");
		
		i=0;
		instance = new InputManager(TEST_FLAT_FILE);
		try {
			instance.openFile();
			while ((s = instance.readFile()) != null){
				if (i<100)
					log.info(s);
				i++;
			}
			instance.closeFile();
		} catch (Throwable e) {
			{log.info(e);}	// this main is just for testing - I don't care
		}
		
		log.info("Found " + i + " records in flat file ");
		
	}


	private static org.apache.log4j.Logger log = Logger.getLogger(InputManager.class);
	
	private static final String TEST_ZIP_FILE = "C:\\input\\20141003\\ECR_IP_CLAIMS_CHIP.zip"; 
	private String password = null;
	private static final String TEST_FLAT_FILE = "C:\\input\\ECR_RX_MEDICARE.txt"; 
	
	
}
