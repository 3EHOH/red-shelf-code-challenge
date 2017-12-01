




public class InputSampler {
	
	
	private HashMap<String, String> parameters;
	//private List<InputMonitor> monitors = new ArrayList<InputMonitor>();
	private InputMonitor mon;
	
	private int limit = 100;
	
	
	
	InputSampler (HashMap<String, String> parameters) {
		
		this.parameters = parameters;
		
		for(Entry<String, String> entry : parameters.entrySet()){
		    log.info("Parameter Key and Value: " +  entry.getKey() + " " + entry.getValue());
		}
		
		try {
			process();  
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	
	/**
	 * main file processor
	 // TODO headerless files?
	 * @throws IOException
	 */
	private void process () throws IOException {
		
		String fName;
		
		for (String sFile:inputFileParameterNames) {
			
			log.info("Starting analysis of: " + sFile);
			
			fName = parameters.get(sFile);
			if (fName == null)
				continue;
			
			String oFile = FileNamer.getFileName(fName) + ".csv"; 
			
			log.info("Starting analysis of file: " + fName);
			
			InputManager inputFile = new InputManager(fName);
			try {
				inputFile.openFile();
			} catch (IOException | ZipException e) {
				log.error("An error occurred while opening " + sFile + " - " + e);
				throw new IllegalStateException ("Requested File not found" + sFile);
			}
			
			mon = new InputMonitor(fName);
			
			FileWriter outputFile = null;
			BufferedWriter bw = null;
			try {
				outputFile = new FileWriter(oFile);
				bw = new BufferedWriter(outputFile);
			} catch (IOException e) {
				System.out.println("Failure occurred trying to write " + oFile);
			}
			
			// get the first line
			String line = inputFile.readFile();
			bw.write(line);
			bw.newLine();
			
			if (!findDelimiter(line)) {
				throw new IllegalStateException("Failed to find file delimiter\n\rReview file and add delimiter to this class: " + 
													this.getClass().getSimpleName());
			}
			
			findColumns(line);
			
			int count = 0;
			
			// read the lines from each file
			
			try {
				
				while ((line = inputFile.readFile()) != null  && (count < limit))  {

					analyzeColumns(line);
					
					if (!doCriteria(line))
						continue;
					if (bw != null) {
						bw.write(line);
						bw.newLine();
					}
					count++;
				}
				
				bw.close();
				inputFile.closeFile();

				
			} catch (IOException e) {
				log.info("Error found at record " + count);
				log.info("An error occurred while reading " + sFile + ": " + e);
				//throw new IllegalStateException("An error occurred while reading " + sFile);
			} 
			
			
			log.info("Completed sampling of file: " + sFile + " writing " + count + " records");
			
		}
		
		
		
	}
	

	private boolean doCriteria(String line) {
		// column[n]=value (repeat for or)
		boolean bResult = true; 
		for(Entry<String, String> entry : parameters.entrySet()) {
			if(entry.getKey().toLowerCase().startsWith("column")) {
				int col_ix = Integer.parseInt(entry.getKey().substring(entry.getKey().indexOf('[')+1, entry.getKey().indexOf(']')));
				String [] in = splitter(line);
				//String s1 = in[col_ix];
				//String s2 = entry.getValue();
				bResult = in[col_ix].equals(entry.getValue());
				break;
			}
		}
		return bResult;
	}
	
	
	
	private void analyzeColumns (String line) {
		
		ColumnFinder cf;
		ColumnController cc;
		
		int col_ix = 0;
		String [] cols = splitter(line);
		for (String s : cols) {
			//log.info(s);
			cf = mon.getCol_index().get(col_ix);
			cc = mon.getCol_cntrl().get(cf.col_name);
			if (s == null  || s.trim().isEmpty()) {}
			else
				cc.setOccurrences(cc.getOccurrences() + 1);
			col_ix++;
		}
		
		
	}
	
	private String [] splitter (String line) {
		
		String [] sReturn;
		
		boolean b = false;
		
		if (line.contains("\"")) {
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
		
		sReturn = line.split(mon.getDelimiter(), -1);
		
		if (b) {
			for (int i=0; i < sReturn.length; i++) {
				sReturn [i] = sReturn [i].replace("`", ","); 
			}
		}
		
		return sReturn;
		
	}
	
	
	
	/**
	 * find the labels associated with each column
	 * instantiate an index and control object for each
	 * @param line
	 */
	private void findColumns (String line) {

		int col_ix = 0;
		mon.setCol_index(new HashMap<Integer, ColumnFinder>());
		mon.setCol_cntrl(new HashMap<String, ColumnController>());
		ColumnFinder cf;
		ColumnController cc;
		
		String [] cols = line.split(mon.getDelimiter(), -1);
		for (String s : cols) {
			//log.info(s);
			cf = new ColumnFinder(col_ix, s);
			cc = new ColumnController();
			cc.setCol_name(s);
			mon.getCol_index().put(col_ix, cf);
			mon.getCol_cntrl().put(s, cc);
			col_ix++;
		}
		
		// feed the Hibernate storable fields
		mon.getColFinder();
		mon.getColController();
		
	}
	
	
	char [] sTypicalDelimiters = {'|', ',', '*', '\t'}; 
	
	/**
	 * find a delimiter from the above list
	 * add to it as needed
	 * @param line
	 * @return
	 */
	private boolean findDelimiter (String line) {

		StringBuffer sb = new StringBuffer();
		for ( int i=0; i < line.length(); i++ ) {
			for (char x: sTypicalDelimiters) {
				if (line.charAt(i) == x) {
					sb.append('\\');
					sb.append(x);
					mon.setDelimiter(sb.toString());
					break;
				}
			}
			if (!mon.getDelimiter().isEmpty())
				break;
		}
		
		if (mon.getDelimiter().isEmpty()) {
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
			
		return !mon.getDelimiter().isEmpty();
		
	}
	
	String [] inputFileParameterNames = {
		"claim_hdr_file",
		"claim_file",
		"prof_file",
		"stay_file",
		"enroll_file",
		"member_file",
		"claim_rx_file",
		"provider_file"
	};
	
	
	

	public static void main(String[] args) {
		
		log.info("Starting Input Sampling");
		RunParameters rp = new RunParameters();
		new InputSampler(rp.loadParameters(args));
		log.info("Completed Input Sampling");
		
	}
	
	private static org.apache.log4j.Logger log = Logger.getLogger(InputSampler.class);
	
	
	

}
