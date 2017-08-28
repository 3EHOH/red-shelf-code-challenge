





public class InputAnalyzer {
	
	
	private HashMap<String, String> parameters;
	private List<InputMonitor> monitors = new ArrayList<InputMonitor>();
	private InputMonitor mon;
	
	ArrayList<String> foundFileParms = new ArrayList<String>();
	
	
	
	InputAnalyzer (HashMap<String, String> parameters) {
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
		
	    
		// get all the input file names for this job
	    foundFileParms = InputManager.getInputFileNames(parameters);

		// start working on them all
		for (String sFile:foundFileParms) {
			

			log.info("Starting analysis of file: " + sFile);
			
			InputManager inputFile = new InputManager(sFile);
			try {
				inputFile.openFile();
			} catch (IOException | ZipException e) {
				log.error("An error occurred while opening " + sFile + " - " + e);
				throw new IllegalStateException ("Requested File not found" + sFile);
			}
			
			mon = new InputMonitor(sFile);
			
			// get the first line
			String line = inputFile.readFile();
			
			if (!findDelimiter(line)) {
				throw new IllegalStateException("Failed to find file delimiter\n\rReview file and add delimiter to this class: " + 
													this.getClass().getSimpleName());
			}
			
			findColumns(line);
			
			findBestMap();
			
			// read the lines from each file
			
			ColumnFinder cf;
			ColumnController cc;
			
			try {
				while ((line = inputFile.readFile()) != null)  {
					
					if(mon.getRcdCount() < 10) { 
						log.info("processing record " + mon.getRcdCount() + " ==> " + new Date());
					}
					
					analyzeColumns(line);
					mon.setRcdCount(mon.getRcdCount() + 1);
					
					if(mon.getRcdCount() % 1000000 == 0) { 
					//if(mon.getRcdCount() % 100000 == 0) { 
						log.info("processing record " + mon.getRcdCount() + " ==> " + new Date());
						
						for (int i=0; i < mon.getCol_index().size(); i++ ) {
							
							cf = mon.getCol_index().get(i);
							cc = mon.getCol_cntrl().get(cf.col_name);
							
							float proportion = ((float) cc.getOccurrences()) / ((float) mon.getRcdCount());
							float f = proportion * 100.0f;
						    String percent =  df1.format(f);
						    
							log.info("Col: " +  cc.getCol_name() + " - "  + cc.getOccurrences() + " is " + percent + "%"); 	
					
						}
						

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
			} catch (StringIndexOutOfBoundsException e) {
				log.error("A string index was out of bounds.  Possible file structure error: " + sFile + " --- Record: " + mon.getRcdCount());
				throw new IllegalStateException("A string index was out of bounds.  Possible file structure error: " + sFile + " --- Record: " + mon.getRcdCount() + "\r\n --->" + line);
			}
			
			
			monitors.add(mon);
			
			log.info("Completed analysis of file: " + sFile + " containing " + mon.getRcdCount() + " records");
			
		}
		
		
		if (parameters.containsKey("nomonitorwrite")  && parameters.get("nomonitorwrite").startsWith("t"))
		{}
		else {
			log.info("Initiating input monitors storage");
			writeMonitors();
			log.info("Input monitors successfully stored"); 
		}
		
	}
	
	
	private void findBestMap ()  {
		//TODO once there are some maps, come back here and check column names against map source names
	}
	
	
	private void analyzeColumns (String line) {
		
		ColumnFinder cf = null;
		ColumnController cc = null;
		
		int col_ix = 0;
		String [] cols = splitter(line);
		
		try {
		
			for (String s : cols) {
				//log.info(s);
				cf = mon.getCol_index().get(col_ix);
				if (cf == null) continue;						// should only happen with records having more columns than header
				cc = mon.getCol_cntrl().get(cf.col_name);
				if (s == null  || s.trim().isEmpty()) {}
				else
					cc.setOccurrences(cc.getOccurrences() + 1);
				col_ix++;
			}
		
		
		}catch (StringIndexOutOfBoundsException e) {
			log.error("A string index was out of bounds.  Possible file structure error. Record no: " + mon.getRcdCount() + " Col: " +  cc.getCol_name() + "|" + cf.col_number);
			throw new IllegalStateException("A string index was out of bounds.  Possible file structure error. Record no: " + mon.getRcdCount() + " Col: " +  cc.getCol_name() + "|" + cf.col_number + "\r\n --->" + line);
		}
		
		
	}
	
	private String [] splitter (String line) {
		
		String [] sReturn;
		
		boolean b = false;
		
		try {
		
			if (mon.getDelimiter().contains(",")   &&   line.contains("\"")) {
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
			
		} catch (StringIndexOutOfBoundsException e) {
			log.error("A string index was out of bounds in splitter.  Possible file structure error. Record no: " + mon.getRcdCount() + " - Data: " +  line);
			throw new IllegalStateException("A string index was out of bounds in splitter.  Possible file structure error. Record no: " + mon.getRcdCount() + "\r\n --->" + line);
		}
		
		return sReturn;
		
	}
	
	/**
	 * gets the statistics for the raw input file and formats them into a html page
	 * @return
	 */
	public String getStatsAsHTML ()  {
		
		ColumnFinder cf;
		ColumnController cc;
		/*
		log.info("Processed " + mon.getRcdCount() + " data records");
		for (int i=0; i < mon.getCol_index().size(); i++ ) {
			cf = mon.getCol_index().get(i);
			cc = mon.getCol_cntrl().get(cf.col_name);
			log.info(cc.getCol_name() + " found " + cc.getOccurrences() );
		}
		*/
		
		StringBuffer sb = new StringBuffer();
		/*
		sb.append("<!DOCTYPE html>");
		sb.append("<html lang=\"en\">");
		sb.append("<head><title>Input File Statistics</title></head>"); 
		
		sb.append("<body>");
		*/
		
		sb.append("<table border=\"1\" cellspacing=\"1\" cellpadding=\"5\">");
		
		
		
		for (InputMonitor mon: monitors) {
			
			sb.append("<tr>");
			sb.append("<th>File Name</th>");
			//sb.append("<th colspan=\"3\">" + mon.getFilename() + "</th>");
			sb.append("<th>" + mon.getFilename() + "</th>");
			sb.append("<th>" + mon.getRcdCount() + "</th>");
			sb.append("<th>Records</th>");
			sb.append("</tr>");
		 
			sb.append("<tr>");
			sb.append("<td>" + "Col No." + "</td>"); 					// column number
			sb.append("<td>" + "Column Label" + "</td>"); 				// column name
			sb.append("<td>" + "No. of Entries" + "</td>"); 			// occurrences of non-blank, non-empty, entries
			sb.append("<td>" + "Percent Completed" + "</td>"); 			// percentage complete
			sb.append("</tr>");	 										// End of header

			for (int i=0; i < mon.getCol_index().size(); i++ ) {
			
				cf = mon.getCol_index().get(i);
				cc = mon.getCol_cntrl().get(cf.col_name);
				
				float proportion = ((float) cc.getOccurrences()) / ((float) mon.getRcdCount());
				float f = proportion * 100.0f;
			    String percent =  df1.format(f);
			    
				sb.append("<tr>");
				sb.append("<td>" + i + "</td>"); 					// column number
				sb.append("<td>" + cc.getCol_name() + "</td>"); 	// column name
				sb.append("<td align=\"right\">" + cc.getOccurrences() + "</td>"); 	// occurrences of non-blank, non-empty, entries
				sb.append("<td align=\"right\">" + percent + "</td>"); 				// percentage complete
				sb.append("</tr>");	 								// End of i'th Row
		
			}
		
		
		}
		
		sb.append("</table>");
		
		/*
		sb.append("</body>");
		sb.append("</html>");
		*/
		
		return sb.toString();
		
	}
	
	static private final DecimalFormat df1 = new DecimalFormat("0.0");
	
	
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
		
		String [] cols = line.replace("\"", "").split(mon.getDelimiter(), -1);
		for (String s : cols) {
			log.info("col: " + s);
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

	
	

	/**
	 * write all the input monitors out to SQL 
	 */
	private void writeMonitors () {
		
		
		List<Class<?>> cList = new ArrayList<Class<?>>();
		cList.add(InputMonitor.class);
		
		HibernateHelper h = new HibernateHelper(cList, "ecr", "ecr");
		SessionFactory factory = h.getFactory("ecr", "ecr");
		
		Session session = factory.openSession();
		Transaction tx = null;

		try{
			tx = session.beginTransaction();
			
			for (InputMonitor m : monitors) {
				m.setJobUid(Long.parseLong(parameters.get("jobuid")));
				session.save(m); 
			}
			
			tx.commit();
		}catch (HibernateException e) {
			if (tx!=null) tx.rollback();
			e.printStackTrace(); 
		}finally {
			session.close(); 

			try {
				h.close("prd", "ecr");
			} catch (Exception e) {
				e.printStackTrace();
			}
			
		}
	}
	

	public static void main(String[] args) {
		
		log.info("Starting Input Analysis");
		HashMap<String, String> parameters = RunParameters.parameters;
		
		
		parameters.put("testlimit", "200");
		parameters.put("configfolder", "C:\\workspace\\ECR_Analytics\\trunk\\EpisodeConstruction\\src\\");
		parameters.put("env", "prd");
		parameters.put("studybegin", "20130101");
		parameters.put("studyend", "20141231");
		
		parameters.put("stepname", ProcessJobStep.STEP_MAP);
		
		//parameters.put("jobuid", "1062");
		//parameters.put("clientID", "XG");
		//parameters.put("runname", "XG_GHP_SAMPLE");
		//parameters.put("rundate", "20150820");
		//parameters.put("jobname", "XG_XG_GHP_SAMPLE20150810");		
		
		//parameters.put("mapname", "xg_ghp");
		//parameters.put("claim_file2", "C:/input/XGGHP/opaclaimsx.csv");
		//parameters.put("claim_file1", "C:/input/XGGHP/ipclaimsx.csv");
		//parameters.put("claim_rx_file1", "C:/input/XGGHP/pharmclaimsx.csv");
		//parameters.put("enroll_file1", "C:/input/XGGHP/memberenrollx.csv");
		//parameters.put("member_file1", "C:/input/XGGHP/memberx.csv");
		//parameters.put("provider_file1", "C:/input/XGGHP/providersx.csv");
		
		//parameters.put("jobuid", "1067");
		//parameters.put("clientID", "Test");
		//parameters.put("runname", "5_3_freeze");
		//parameters.put("rundate", "20150824");
		//parameters.put("jobname", "Test_5_3_freeze20150824");
		//parameters.put("mapname", "hci3");

		//parameters.put("claim_file1", "C:/input/Test/clm_sample.csv");
		//parameters.put("claim_rx_file1", "C:/input/Test/rx_sample.csv");
		//parameters.put("enroll_file1", "C:/input/Test/member_sample.csv");
		//parameters.put("member_file1", "C:/input/Test/member_sample.csv");
		//parameters.put("provider_file1", "C:/input/Test/provider_sample.csv");

		//parameters.put("jobuid", "1082");
		//parameters.put("clientID", "XG");
		//parameters.put("runname", "");
		//parameters.put("rundate", "20150903");
		//parameters.put("jobname", "XG_GHC20150903");
		//parameters.put("mapname", "xg_ghc");

		//parameters.put("claim_file1", "C:/input/XGGHC/single_MED.dat");
		//parameters.put("claim_rx_file1", "C:/input/XGGHC/single_RX.dat");
		//parameters.put("enroll_file1", "C:/input/XGGHC/single_MBRMONTHS.dat");
		//parameters.put("member_file1", "C:/input/XGGHC/single_MBR.dat");
		//parameters.put("provider_file1", "C:/input/XGGHC/single_PRV.dat");


		//parameters.put("jobuid", "1069");
		//parameters.put("clientID", "XG");
		//parameters.put("runname", "GHP_Medicare");
		//parameters.put("rundate", "20150826");
		//parameters.put("jobname", "XG_GHP_Medicare20150826");
		//parameters.put("mapname", "xg_ghp");

		//parameters.put("claim_file1", "C:/input/XGGHP/ipclaims_medicare.csv");
		//parameters.put("claim_file2", "C:/input/XGGHP/opaclaimsx.csv");
		//parameters.put("claim_rx_file1", "C:/input/XGGHP/pharmclaims_medicare.csv");
		//parameters.put("enroll_file1", "C:/input/XGGHP/memberenroll_medicare.csv");
		//parameters.put("member_file1", "C:/input/XGGHP/member_medicare.csv");
		//parameters.put("provider_file1", "C:/input/XGGHP/providers.csv");
		
		/*
		parameters.put("jobuid", "1095");
		parameters.put("clientID", "Test");
		parameters.put("runname", "Dec50K");
		parameters.put("rundate", "20151209");
		parameters.put("jobname", "Test_Dec50K20151209");
		parameters.put("mapname", "hci3");

		parameters.put("claim_file1", "C:/input/Test/new_2015_11_05/claims_mod.csv");
		parameters.put("claim_rx_file1", "C:/input/Test/new_2015_11_05/pharmacy_claims.csv");
		parameters.put("enroll_file1", "C:/input/Test/new_2015_11_05/member.csv");
		parameters.put("member_file1", "C:/input/Test/new_2015_11_05/member.csv");
		parameters.put("provider_file1", "C:/input/Test/new_2015_11_05/provider.csv");
		*/

		parameters.put("jobuid", "1119");
		parameters.put("clientID", "PEBTF");
		parameters.put("runname", "PEBTF_maptest");
		parameters.put("rundate", "20160720");
		parameters.put("jobname", "PEBTF_maptest20160720");
		parameters.put("mapname", "pebtf");

		parameters.put("claim_file1", "C:/input/PEBTF/unitchk.csv");
		parameters.put("claim_rx_file1", "C:/input/PEBTF/Rx_sample.csv");
		parameters.put("enroll_file1", "C:/input/PEBTF/member_hdr.csv");
		parameters.put("member_file1", "C:/input/PEBTF/member_hdr.csv");
		parameters.put("provider_file1", "C:/input/PEBTF/unitchk.csv");
		

		
		InputAnalyzer instance = new InputAnalyzer(parameters);
		log.info(instance.getStatsAsHTML());
		log.info("Completed Input Analysis");
		
	}
	
	private static org.apache.log4j.Logger log = Logger.getLogger(InputAnalyzer.class);
	
	
	
	

}
