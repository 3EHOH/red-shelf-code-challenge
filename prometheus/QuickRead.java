




public class QuickRead {

	
	
	

	
	QuickRead (HashMap<String, String> parameters) {
		this.parameters = parameters;
		process();
	}
	
	
	
	public void process () {
		
		
		if((sFile = parameters.get("input")) == null)
			throw new IllegalStateException("Need input= parameter");
		
		int limit = Integer.parseInt(parameters.get("count"));
		
		FileReader inputFile = null;
		FileWriter outputFile = null;
		BufferedWriter bw = null;
		try {
			inputFile = new FileReader(sFile);
		} catch (FileNotFoundException e) {
			log.error("Failure occurred trying to read " + sFile);
		}
		try {
			if((oFile = parameters.get("output")) != null) {
				outputFile = new FileWriter(oFile);
				bw = new BufferedWriter(outputFile);
			}
		} catch (IOException e) {
			log.error("Failure occurred trying to write " + oFile);
		}
		BufferedReader br = new BufferedReader(inputFile); 
		String line;
		int count = 0;
		
		// read the lines and print
		try {
			while ((line = br.readLine()) != null  && (count < limit))  {
				//System.out.println(line);
				if (!doCriteria(line))
					continue;
				if (bw != null) {
					bw.write(line);
					bw.newLine();
				}
				count++;
			}
			br.close();
			if (bw != null)
				bw.close();
		} catch (IOException e) {
			log.error("An error occurred while reading " + sFile);
		}  
		
	}

	private boolean doCriteria(String line) {
		// column[n]=value (repeat for or)
		boolean bResult = true; 
		for(Entry<String, String> entry : parameters.entrySet()) {
			if(entry.getKey().toLowerCase().startsWith("column")) {
				int col_ix = Integer.parseInt(entry.getKey().substring(entry.getKey().indexOf('[')+1, entry.getKey().indexOf(']')));
				String[] in = line.split(sDelimiter);
				//String s1 = in[col_ix];
				//String s2 = entry.getValue();
				bResult = in[col_ix].equals(entry.getValue());
				break;
			}
		}
		return bResult;
	}

	static String sFile, oFile;
	
	
	
	HashMap<String, String> parameters = new HashMap<String, String>();
	static String [][] parameterDefaults = {
			{"count", "99999"}
	};
	
	public static final String sDelimiter = "\\*|\\||;|,";
	
	
	
	
	public static void main(String[] args) {
		
		HashMap<String, String> parameters = RunParameters.parameters;
		parameters.put("input", "C:\\input\\XGGHC\\HDIV3_GHC_GROUPHEALTH_MED_0001_TEST_20141215224115.dat");
		parameters.put("output", "C:\\input\\XGGHC\\HDIV3_GHC_GROUPHEALTH_MED_0001_TEST_GROUPHEALTH01315409468.dat");
		parameters.put("count", "9999");
		parameters.put("column[7]", "GROUPHEALTH01315409468");
		
		QuickRead instance =   new QuickRead(parameters);
		instance.process();
		
		
	}
	
	private static org.apache.log4j.Logger log = Logger.getLogger(QuickRead.class);

}
