





public class FlatFileToMongo {
	
	
	String dbUrl;
	String schema;
	String dbUser;
	String dbPw;
	
	Mongo mongoClient;
	DB db;
	DBCollection coll;
	//BasicDBObject doc;
	
	private HashMap<String, String> parameters;
	private List<InputMonitor> monitors = new ArrayList<InputMonitor>();
	private InputMonitor mon;
    
	public FlatFileToMongo () {
		this (RunParameters.parameters);
	}
	
	public FlatFileToMongo (HashMap<String, String> parameters) {

		this.parameters = parameters;
		
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
			
			log.info("Starting read of: " + sFile);
			
			fName = parameters.get(sFile);
			if (fName == null)
				continue;
			
			log.info("Starting build of file: " + fName);
			
			InputManager inputFile = new InputManager(fName);
			try {
				inputFile.openFile();
			} catch (IOException | ZipException e) {
				log.error("An error occurred while opening " + sFile + " - " + e);
				throw new IllegalStateException ("Requested File not found" + sFile);
			}
			
			mon = new InputMonitor(fName);
			
			initialize();
			
			// get the first line
			String line = inputFile.readFile();
			
			if (!findDelimiter(line)) {
				throw new IllegalStateException("Failed to find file delimiter\n\rReview file and add delimiter to this class: " + 
													this.getClass().getSimpleName());
			}
			
			findColumns(line);
			
			
			// read the lines from each file
			
			try {
				while ((line = inputFile.readFile()) != null)  {
					
					if(mon.getRcdCount() < 10) { 
						log.info("processing record " + mon.getRcdCount() + " ==> " + new Date());
					}
					
					writeColumns(line);
					mon.setRcdCount(mon.getRcdCount() + 1);
					
					if(mon.getRcdCount() % 1000000 == 0) { 
					//if(mon.getRcdCount() % 100000 == 0) { 
						log.info("processing record " + mon.getRcdCount() + " ==> " + new Date());

						/*  a pause if you want to watch
						try {
						    Thread.sleep(10000);                 //1000 milliseconds is one second.
						} catch(InterruptedException ex) {
						    Thread.currentThread().interrupt();
						}
						*/
						
					}
					
				}
				
				inputFile.closeFile();
				
			} catch (IOException e) {
				throw new IllegalStateException("An error occurred while reading " + sFile);
			} 
			
			monitors.add(mon);
			
			log.info("Completed conversion of file: " + sFile + " containing " + mon.getRcdCount() + " records");
			
		}
		
		
		
	}
	
 	

	public String writeColumns(String line) {
		
		BasicDBObject doc = doMap(line);
		
		
		doBatchInsert(doc);
		
		
		return doc.get( "_id" ).toString();
		
	}
	
	
	DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	
	
	
	private BasicDBObject doMap(String line) {

		BasicDBObject doc = new BasicDBObject();
		
		ColumnFinder cf;
		ColumnController cc;
		
		int col_ix = 0;
		String [] cols = splitter(line);
		
		for (String s : cols) {
			//log.info(s);
			cf = mon.getCol_index().get(col_ix);
			cc = mon.getCol_cntrl().get(cf.col_name);
			doc.put(cc.getCol_name().replace('.', '_'), s);
			col_ix++;
		}
		
		return doc;
		
	}
	
	

	private void doBatchInsert (BasicDBObject doc) {
		
		//getConnection();
		
		coll.insert(doc);
		
		
		//log.info(sb);
		
		
	}
	
	
	
	
	private void initialize() {
		
		// define the output table
		if (parameters.get("rundate") == null) {
			Date date = Calendar.getInstance().getTime();
			DateFormat formatter = new SimpleDateFormat("yyyy_MM_dd_hhmm");
			sRunDate = formatter.format(date);
			parameters.put("rundate", sRunDate);
		}
		else
			sRunDate = parameters.get("rundate");
		
		String sFN = mon.getFilename().contains("/") ? mon.getFilename().substring(mon.getFilename().lastIndexOf("/") + 1) : mon.getFilename();
		
		 String sRunName = parameters.get("runname") == null ?  sDefaultRunName :  parameters.get("runname").replace(' ', '_');
		
		String sClient = parameters.get("clientID") == null ? "Test" : parameters.get("clientID");
		
		sCollectionName = sClient + "_" + sRunName + sRunDate + "_" + sFN;
		
		
				 
		String env = parameters.get("mongo") == null ?  "md1" :  parameters.get("mongo");

		ConnectParameters connParms = new ConnectParameters(env);
		dbUrl = connParms.getDbUrl();
		schema = connParms.getSchema();
		dbUser = connParms.getDbUser();
		dbPw = connParms.getDbPw();
		
		log.info("Initializing " + sCollectionName + " ==> " + new Date());
		log.info(">>>  Connection information - U: " + dbUrl + "; S: " + schema + "; I: " + dbUser + " P:" + dbPw); 
	    

		try {
			mongoClient = new MongoClient(dbUrl);
		} catch (UnknownHostException e) {
			e.printStackTrace();
			throw new IllegalStateException(e);
		}
		db = mongoClient.getDB(schema);
		coll = db.getCollection(sCollectionName);

		
	}
	
	
	Method mKey;
	
	
	
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
	
	
	String [] inputFileParameterNames = {
			"claim_hdr_file",
			"claim_file",
			"claim_file1",
			"claim_file2",
			"claim_file3",
			"prof_file",
			"stay_file",
			"enroll_file",
			"enroll_file1",
			"enroll_file2",
			"enroll_file3",
			"member_file",
			"member_file1",
			"member_file2",
			"member_file3",
			"claim_rx_file",
			"claim_rx_file1",
			"claim_rx_file2",
			"claim_rx_file3",
			"provider_file",
			"provider_file1",
			"provider_file2",
			"provider_file3"
		};



	String sDefaultRunName = "Test";
	String sDelimiter = ",";
	String sRunDate;
	String sCollectionName;
	int iBatchIx = 0;


	public static void main(String[] args) {
		
		log.info("Starting Input Store");
		HashMap<String, String> parameters = RunParameters.parameters;
		parameters.put("clientID", "XG");
		parameters.put("runname", "XG_GHP_SAMPLE");
		parameters.put("rundate", "20150820");
		parameters.put("jobuid", "1066");
		parameters.put("testlimit", "200");
		parameters.put("configfolder", "C:\\workspace\\ECR_Analytics\\trunk\\EpisodeConstruction\\src\\");
		parameters.put("env", "prd");
		parameters.put("studybegin", "20130101");
		parameters.put("studyend", "20141231");
		parameters.put("jobname", "XG_XG_GHP_SAMPLE20150810");
		parameters.put("stepname", ProcessJobStep.STEP_INPUTSTORE);
		parameters.put("mapname", "xg_ghp");
		parameters.put("claim_file2", "C:/input/XGGHP/opaclaimsx.csv");
		parameters.put("claim_file1", "C:/input/XGGHP/ipclaimsx.csv");
		parameters.put("claim_rx_file1", "C:/input/XGGHP/pharmclaimsx.csv");
		parameters.put("enroll_file1", "C:/input/XGGHP/memberenrollx.csv");
		parameters.put("member_file1", "C:/input/XGGHP/memberx.csv");
		parameters.put("provider_file1", "C:/input/XGGHP/providersx.csv");

		
		/*FlatFileToMongo instance = */new FlatFileToMongo(parameters);
		//log.info(instance.getStatsAsHTML());
		log.info("Completed Input Store");
		
	}
	
	
	
	private static org.apache.log4j.Logger log = Logger.getLogger(FlatFileToMongo.class);


}
