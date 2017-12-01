



public class ArchiveController {
	
	
	private HashMap<String, String> parameters;
	
	private String archiveBat;
	private static String OS = System.getProperty("os.name").toLowerCase();
	
	private ECRControl _ec;
	
	/**
	 * constructor - requires job parameters
	 * @param parameters
	 */
	public ArchiveController (HashMap<String, String> parameters) {
		
		this.parameters = parameters;
		
		
		try {
			_ec = new ECRControlProperties().get_ecrControl();
			archiveBat = _ec.getArchiveScript();
		} catch (IOException e) {
			log.warn("Could not find ecrcontrol.properties.  Using OS defaults");
			if (OS.indexOf("nix") >= 0 || OS.indexOf("nux") >= 0 || OS.indexOf("aix") > 0) {
				archiveBat = "/home/ubuntu/ecrArchive.sh";
			} else if (OS.indexOf("win") >= 0) {
				archiveBat = "C:/temp/archive.bat";
			}
		}
		
	}
	
	
	
	public void Backupdbtosql() {
		
		try {
			
			// get connect information from database.properties
			ConnectParameters cp = new ConnectParameters("prd");
			cp.setSchema(parameters.get("jobname"));
			
	        /*NOTE: Creating Database Constraints*/
	        String dbName = cp.getSchema();
	        String dbUser = cp.getDbUser();
	        String dbPass = cp.getDbPw();

	        ProcessBuilder b = new ProcessBuilder(archiveBat, dbName, dbUser, dbPass);

	        Process process = b.start(); 
	        int errCode = process.waitFor(); 
	        log.info("Echo command executed, any errors? " + (errCode == 0 ? "No" : "Yes")); 

	        log.info("Echo Output:\n" + output(process.getInputStream()));     


	    } catch (Exception ex) {

	    	log.error("Error at Backupdbtosql" + ex.getMessage());
	    	log.error("Attempting to create backup " + parameters.get("jobname"));
	    	/*
	    	if (executeCmd != null)
	    		log.error("Attempted backup command " + executeCmd);
	    		*/
	    	
	    }
		
	}
	
	
	private static String output(InputStream inputStream) throws IOException {

		StringBuilder sb = new StringBuilder(); 
        BufferedReader br = null; 

        try { 

            br = new BufferedReader(new InputStreamReader(inputStream)); 
            String line = null; 
            while ((line = br.readLine()) != null) { 
                sb.append(line + System.getProperty("line.separator")); 
            } 

        } finally { 
            br.close(); 
        } 

        return sb.toString(); 

	}
	

	public void Restoredbfromsql(String s) {
        
		try {
            /*NOTE: String s is the mysql file name including the .sql in its name*/
            /*NOTE: Getting path to the Jar file being executed*/
            /*NOTE: YourImplementingClass-> replace with the class executing the code*/
            CodeSource codeSource = ArchiveController.class.getProtectionDomain().getCodeSource();
            File jarFile = new File(codeSource.getLocation().toURI().getPath());
            String jarDir = jarFile.getParentFile().getPath();

            /*NOTE: Creating Database Constraints*/
             String dbName = "YourDBName";
             String dbUser = "YourUserName";
             String dbPass = "YourUserPassword";

            /*NOTE: Creating Path Constraints for restoring*/
            String restorePath = jarDir + "\\backup" + "\\" + s;

            /*NOTE: Used to create a cmd command*/
            /*NOTE: Do not create a single large string, this will cause buffer locking, use string array*/
            String[] executeCmd = new String[]{"mysql", dbName, "-u" + dbUser, "-p" + dbPass, "-e", " source " + restorePath};

            /*NOTE: processComplete=0 if correctly executed, will contain other values if not*/
            Process runtimeProcess = Runtime.getRuntime().exec(executeCmd);
            int processComplete = runtimeProcess.waitFor();

            /*NOTE: processComplete=0 if correctly executed, will contain other values if not*/
            if (processComplete == 0) {
                //JOptionPane.showMessageDialog(null, "Successfully restored from SQL : " + s);
            	log.info("Successfully restored from SQL : " + s);
            } else {
                //JOptionPane.showMessageDialog(null, "Error at restoring");
            	log.error("Error at restoring");
            }


        } catch (URISyntaxException | IOException | InterruptedException | HeadlessException ex) {
            //JOptionPane.showMessageDialog(null, "Error at Restoredbfromsql" + ex.getMessage());
        	log.error("Error at Restoredbfromsql" + ex.getMessage());
        }

    }
	
	

	/*
	private String escapeUx(String executeCmd) {
		StringBuilder sR = new StringBuilder();
		for (int i=0; i<executeCmd.length(); i++) {
			for (int j=0; j<uxEscapeChars.length(); j++) {
				if (executeCmd.charAt(i) == uxEscapeChars.charAt(j)) {
					sR.append('\\');
					break;
				}
				
			}
			sR.append(executeCmd.charAt(i));
		}
		return sR.toString();
	}
	*/


	
	
	private static org.apache.log4j.Logger log = Logger.getLogger(ArchiveController.class);
	
	
	/**
	 * main strictly for unit testing
	 * @param args
	 */
	public static void main(String[] args) {
		
		// get parameters and make them available to all
		RunParameters rp = new RunParameters();
		HashMap<String, String> parameters = RunParameters.parameters;
		parameters = rp.loadParameters(args, parameterDefaults);
		
		
		BigKahuna bigKahuna = new BigKahuna();
		String _cf = parameters.get("configfolder");
    	
    	bigKahuna.parameters = parameters;
		
    	bigKahuna.reLoadParameters();
    	
    	// fake stuff for local testing
    	parameters.put("configfolder", _cf);		

		ArchiveController _AC = new ArchiveController(parameters);
		_AC.Backupdbtosql();
		
	}

	
	
	static String [][] parameterDefaults = {
		{"configfolder", "C:\\workspace\\ECR_Analytics\\trunk\\EpisodeConstruction\\src\\"}
	};
	
	

}
