





public class DeDuplicator {
	
	
	// this delimiter is too broad in many cases
	// it allows for asterisks pipes, commas, semi-colons and double-quotes
	// ADJUST as Needed
	String sDelimiter = "\\*|\\||;|,";
		
	
	
	/**
	 * main process for input validation
	 * @throws IOException
	 */
	private void process () throws IOException {

		log.info("Starting DEDup");
		
		if (parameters.get("input") == null  ||  parameters.get("input").isEmpty())
			throw new IllegalStateException("No 'input=' parameter specified");
		if (parameters.get("output") == null  ||  parameters.get("output").isEmpty())
			throw new IllegalStateException("No 'output=' parameter specified");

		int iRecordCount = 1;
		int oRecordCount = 1;
		
		InputManager inputFile = new InputManager(parameters.get("input"));
		try {
			inputFile.openFile();
		} catch (IOException | ZipException e) {
			log.error("An error occurred while opening " + parameters.get("input") + " - " + e);
		}
		
		String line;

		
		String oFile=parameters.get("output");
		FileWriter outputFile = null;
		BufferedWriter bw = null;
		
		try {
			outputFile = new FileWriter(oFile);
			bw = new BufferedWriter(outputFile);
		} catch (IOException e) {
			System.out.println("Failure occurred trying to write " + oFile);
		}
		
		// read the input file
		try {
			
			// header
			line = inputFile.readFile();
			bw.write(line);
			bw.newLine();
			
			// read all data records until the end
			while ((line = inputFile.readFile()) != null)  {
				if (checkForDupe(line)) {
					bw.write(line);
					bw.newLine();
					oRecordCount++;
				}
				iRecordCount++;
				if(iRecordCount % 10000 == 0) { 
					log.info("executing " + iRecordCount + " ==> " + new Date());
				}
			}
		} catch (IOException e) {
			log.error("An error occurred while reading " + parameters.get("input"));
		}  
		
		try {
			inputFile.closeFile();
		} catch (IOException e) {
			log.error("An error occurred while closing file: " + parameters.get("input"));
		} 
		bw.close();
		
		log.info("Completed DEDup");
		log.info("Read " + iRecordCount + " records; wrote " + oRecordCount + " records");
		
	}

	
	HashSet<String> existingKeys = new HashSet<String>();
	String [] in;
	String sKey;
	
	
	/**
	 * split the line into an array of elements
	 * check if the key exists in existingKeys list
	 * if it does, return false
	 * if it does not, add the key to the list of existing keys and return true
	 * @param line
	 * @return
	 */
	private boolean checkForDupe (String line)  {
		
		boolean bR = true;
		
		in = line.split(sDelimiter);
		
		// Java counts columns relative to zero
		// MemberId/ClaimId/ClaimLineId/BeginDt/EndDt/AllowedAmt/ProviderId
		
		// SHP OP/PB claims have member Id in first column, claim id in second, and line id in third
		// sKey = in[0] + in[1] + in[2];
		
		// XG_GHP IP claims have member Id in first column, claim id in second, begin date in 5th, end date in 6th, allowed amount in fourth, provider id in 3rd (no line number)
		// XG_GHP OP/PB claims have member Id in first column, claim id in second, line number in 3rd,  begin date in 6th, end date in 7th, allowed amount in 4th, provider id in 5th
		sKey = in[0] + "|" + in[1] + "|" + in[2] + "|" + in[5] + "|" + in[6] + "|" + in[3] + "|" + in[4];
		
		if (existingKeys.contains(sKey)) {
			bR = false;
			log.info("Removing duplicate: " + sKey);
		}
		else {
			existingKeys.add(sKey);
			bR = true;
		}
		
		return bR;
		
	}
	
	


	public static void main(String[] args) throws IOException {
		
		DeDuplicator instance = new DeDuplicator();
		
		// get parameters (if any)
		instance.loadParameters(args);


		// process
		instance.process();

	}
	
	HashMap<String, String> parameters = RunParameters.parameters;

	String [][] parameterDefaults = {
			{"placeholder", "whateveryoulike"},
			{"anotherplaceholder", "anothervalue"}
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
	


	
	private static org.apache.log4j.Logger log = Logger.getLogger(DeDuplicator.class);
	
	
}
