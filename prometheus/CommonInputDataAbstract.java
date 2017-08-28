






public abstract class CommonInputDataAbstract {
	
	
	protected String[] in;
	
	/**
	 * will get a value for a specific column labeled as header
	 * only use where two columns have a dependency as it is inefficient 
	 * relative to normal process-as-you-scan
	 * @param sHeader
	 * @return
	 */
	protected String getColumnValue (String sHeader) {
		for (Map.Entry<Integer, ColumnFinder> entry : col_index.entrySet()) {
			if ( ((ColumnFinder) entry.getValue()).col_name.equalsIgnoreCase(sHeader))
				return in[entry.getKey()];
		}
		return "";
	}
	
	public String sDelimiter = "\\*|\\||;|,";
	
	protected List<String> errors = new ArrayList<String>();
	protected InputStatistics stats = new InputStatistics();
	protected ErrorManager errMgr;
	
	
	private String password;

	protected ArrayList<InputMonitor> sourceMonitors = new ArrayList<InputMonitor>();
	

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public void addSourceFile (String s) {
		sourceFiles.add(s);
		stats.fileName.concat(s).concat(";");
	}
	

	public void addSourceMonitor (InputMonitor m) {
		sourceMonitors.add(m);
	}
	
	
	protected List<String> sourceFiles = new ArrayList<String>();
	
	protected HashMap<Integer, ColumnFinder> col_index;
	protected HashMap<String, Integer> col_map;
	
	
	//protected String [] splitter (String line) {
	//	return line.split(sDelimiter);
	//}
	

	public void doErrorReporting (String id, String errValue, Object obj) {
		if(errMgr != null)
			errMgr.issueError(id, errValue, obj);
	}


	public List<String> getErrorMsgs() {
		return errors;
	}


	public InputStatistics getInputStatistics() {
		return stats;
	}
	
	protected String [] splitter (String line) {
		
		String [] sReturn;
		
		boolean b = false;
		
		if (delimiter.contains(",")  &&  line.contains("\"")) {
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
		
		sReturn = line.split(getDelimiter(), -1);
		
		if (b) {
			for (int i=0; i < sReturn.length; i++) {
				sReturn [i] = sReturn [i].replace("`", ","); 
			}
		}
		
		return sReturn;
		
	}
	
	char [] sTypicalDelimiters = {'|', ',', '*', '\t'};

	private String delimiter = ""; 
	
	/**
	 * find a delimiter from the above list
	 * add to it as needed
	 * @param line
	 * @return
	 */
	boolean findDelimiter (String line) {

		StringBuffer sb = new StringBuffer();
		for ( int i=0; i < line.length(); i++ ) {
			for (char x: sTypicalDelimiters) {
				if (line.charAt(i) == x) {
					sb.append('\\');
					sb.append(x);
					setDelimiter(sb.toString());
					break;
				}
			}
			if (!getDelimiter().isEmpty())
				break;
		}
		
		if (getDelimiter().isEmpty()) {
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
			
		return !getDelimiter().isEmpty();
		
	}
	
	
	public String getDelimiter() {
		return delimiter;
	}

	public void setDelimiter(String delimiter) {
		this.delimiter = delimiter;
	}

	private static org.apache.log4j.Logger log = Logger.getLogger(CommonInputDataAbstract.class);
	

}
