







public class PostConstruction {
	
	
	HashMap<String, String> parameters;
	private String scriptFolder;
	
	ArrayList<String> resultReport = new ArrayList<String>();
	
	PostConstruction (HashMap<String, String> parameters) {
		this.parameters = parameters;
	}
	

	
	
	boolean process () {
		
		DataRunTableHelper.updateRunTable(parameters);
		
		boolean result = true;
		
		switch (parameters.get("stepname")) {
		
		case ProcessJobStep.STEP_EPISODE_DEDUPE:

			scriptFolder = "post_ec";
			
			log.info(InfoUtil.getProcessId("") + " DeDupeAll");
			if (result) result = runSQLScript("DeDupeAll.sql");
			
			break;
		
		case ProcessJobStep.STEP_PROVIDER_ATTRIBUTION:

			scriptFolder = "post_ec";
			
			log.info(InfoUtil.getProcessId("") + " Provider Attribution");
			result = runSQLScript("Provider_Attribution.sql");
			
			break;
	
			
		case ProcessJobStep.STEP_COST_ROLLUPS:

			scriptFolder = "post_ec";
			
			log.info(InfoUtil.getProcessId("") + " Clearing for costrollups...");
			result = runSQLScript("clean_for_rerun.sql");
			
			log.info(InfoUtil.getProcessId("") + " Starting SQL-PostEC-AutoSQLSetup");
			if (result) result = runSQLScript("SQL-PostEC-AutoSQLSetup.sql");
			
			log.info(InfoUtil.getProcessId("") + " SQL-costRollups-unsplit");
			if (result) result = runSQLScript("SQL-costRollups-unsplit.sql");
			
			log.info(InfoUtil.getProcessId("") + " SQL-costRollups-split");
			if (result) result = runSQLScript("SQL-costRollups-split.sql");
			
			break;
		
		case ProcessJobStep.STEP_FILTERED_ROLLUPS:

			scriptFolder = "post_ec";
			
			log.info(InfoUtil.getProcessId("") + " Starting SQL_Filtering_DefaultAll");
			result = runSQLScript("SQL-Filtering-DefaultAll.sql");
			
			log.info(InfoUtil.getProcessId("") + " Starting SQL-costRollups-unsplit-filter1.sql");
			if (result) result = runSQLScript("SQL-costRollups-unsplit-filter1.sql");
			
			log.info(InfoUtil.getProcessId("") + " Starting SQL-costRollups-split-filter1.sql");
			if (result) result = runSQLScript("SQL-costRollups-split-filter1.sql");
			
			log.info(InfoUtil.getProcessId("") + " Starting SQL-costRollups-unsplit-ann-filter1.sql");
			if (result) result = runSQLScript("SQL-costRollups-unsplit-ann-filter1.sql");
			
			log.info(InfoUtil.getProcessId("") + " Starting SQL-costRollups-split-ann-filter1.sql");
			if (result) result = runSQLScript("SQL-costRollups-split-ann-filter1.sql");

			break;
			
		case ProcessJobStep.STEP_MASTER_UNFILTERED_RA_SA:

			scriptFolder = "ra_sa";
			
			log.info(InfoUtil.getProcessId("") + " Starting RA/SA");
			//log.info(InfoUtil.getProcessId("") + " setting up tables...");
			//result = runSQLScript("mdc_desc.sql");
			result = true;
			if (result) {
				log.info(InfoUtil.getProcessId("") + " Starting risk_factors_2017_04_20.sql");
				result = runSQLScript("risk_factors_2017_04_20.sql");
			}
			if (result) {
				log.info(InfoUtil.getProcessId("") + " Starting sub_type_codes_2017_04_21.sql");
				result = runSQLScript("sub_type_codes_2017_04_21.sql");
			}
			
			if (result) {
				//runScript("indexes.sql");  // indexes are all duplicates
			}
			
			
			//if (result) result = runSQLScript("Master_RA_Script_2015_10_01_UNFILTERED.sql");
			if (result) {
				log.info(InfoUtil.getProcessId("") + " Starting Master RA Script");
				result = runSQLScript("Master_RA_Script_MySQL_04252017.sql");
			}
			
			/*
			log.info(InfoUtil.getProcessId("") + " Cleaning URI...");
			if (result) result = runSQLScript("clean_uri.sql");
			*/
			
			log.info(InfoUtil.getProcessId("") + " Completed RA/SA");

			break;
			
		case ProcessJobStep.STEP_RA_SA_MODEL:

			scriptFolder = "ra_sa";
			
			log.info(InfoUtil.getProcessId("") + " Starting RA/SA Model");
			result = runRScript("HCI3-RASAModel_04102017.R");
			//result = runRScript("TestScript.R");
			
			log.info("Completed " + ProcessJobStep.STEP_RA_SA_MODEL);

			break;
			
		case ProcessJobStep.STEP_RED:

			scriptFolder = "post_ec";
			
			log.info(InfoUtil.getProcessId("") + " Starting RED");
			result = runSQLScript("red_with_filter_id_2016_01_20.sql");
			
			break;
		
		case ProcessJobStep.STEP_RES:

			scriptFolder = "post_ec";
			
			log.info(InfoUtil.getProcessId("") + " Starting RES");
			result = runSQLScript("res_with_filter_id_rvsd_2016_01_20.sql");
			
			break;
			
		case ProcessJobStep.STEP_SAVINGS_SUMMARY:

			scriptFolder = "post_ec";
			
			log.info(InfoUtil.getProcessId("") + " Starting descriptive_stats.sql");
			result = runSQLScript("descriptive_stats.sql");
			
			if (result) {
				log.info(InfoUtil.getProcessId("") + " Starting dyo_estimate_savings_ave_50pac.sql");
				result = runSQLScript("dyo_estimate_savings_ave_50pac.sql");
			}
			
			break;

		case ProcessJobStep.STEP_CORE_SERVICES:

			scriptFolder = "post_ec";
			
			log.info(InfoUtil.getProcessId("") + " Starting all_core_services_analysis.sql");
			result = runSQLScript("all_core_services_analysis.sql");
			
			break;
			
		case ProcessJobStep.STEP_PAC_ANALYSIS:

			scriptFolder = "post_ec";
			
			log.info(InfoUtil.getProcessId("") + " Starting pac_drill_down.sql");
			result = runSQLScript("pac_drill_down.sql");
			
			break;
			
		case ProcessJobStep.STEP_PAS_ANALYSIS:

			scriptFolder = "post_ec";
			
			log.info(InfoUtil.getProcessId("") + " Starting pas_analysis.sql");
			result = runSQLScript("pas_analysis.sql");
			
			break;
			
		case ProcessJobStep.STEP_MATERNITY:

			scriptFolder = "post_ec";
			
			log.info(InfoUtil.getProcessId("") + " Starting maternity_and_newborn_filter_flags.sql");
			result = runSQLScript("maternity_and_newborn_filter_flags.sql");
			
			break;
					
		default:
			log.error(InfoUtil.getProcessId("") + " Encountered an unknown Post-EC step name: " + parameters.get("stepname"));
			break;	
			
			
		}

		return result;
	
	}
	
	
	
	private boolean runSQLScript(String sName) {
		
		boolean bR = true;

		try {
	
			
			ConnectParameters cp = new ConnectParameters(parameters.get("env"));
			cp.setSchema(parameters.get("jobname"));
			
			Connection conn = ConnectionHelper.getConnection(cp);
	 
			Reader reader = Resources.getResourceAsReader("sql/"+ cp.getDialect().toLowerCase() + "/" + scriptFolder + "/" + sName); 

	 
			ScriptRunner runner = new ScriptRunner(conn); 
			runner.setDelimiter(";"); 
			//runner.setLogWriter(null); 
			//runner.setErrorLogWriter(null); 
			//runner.setSendFullScript(true);
			runner.runScript(reader); 
			conn.commit(); 
			reader.close();
			
		}
		catch (Exception e) {
			
			log.error(e);
			bR = false;
			
		}
		
		return bR;
	
	}
	
	
	private boolean runRScript (String sName) {
		
		boolean bR = true;
		
		RConnection connection = null;
		
		StartRserve.startNewLocalRserve();
		 
		try {
			/* Create a connection to Rserve instance running on default port
			 * 6311
			 */
			log.info("Opening Rserve connection");
			connection = new RConnection("localhost", StartRserve._port);
			//String scriptPath = "C:/workspace/ECR_Analytics/trunk/EpisodeConstruction/src/R/"+ scriptFolder + "/" + sName;

			//StringBuffer sbCmd = new StringBuffer().append("source(").append('"').append(scriptPath).append('"').append(')');
			
			
			Reader reader = Resources.getResourceAsReader("R/"+ scriptFolder + "/" + sName);
			BufferedReader ir = new BufferedReader(reader);

            String s=null;
            StringBuilder sb = new StringBuilder();
  
            while ( ( s = ir.readLine() ) != null) {
            	sb.append(s).append(' ').append(System.lineSeparator());
			}
 
			connection.assign(".tmp.", sb.toString());
            
            // set up connection variables for R
            ConnectParameters cp = new ConnectParameters(parameters.get("env"));
 
            //log.info("libPath");
            // connection.eval(getRLibraries());
            //log.info("detach");
            log.info("Evals");
            connection.eval("db<-" + quoteit(parameters.get("jobname")));
            log.info("Eval db ok");
            connection.eval("dbuser<-" + quoteit(cp.getDbUser()));
            log.info("Eval dbuser ok");
            connection.eval("dbpassword<-" + quoteit(cp.getDbPw()));
            log.info("Eval dbpassword ok");
            connection.eval("dbhost<-" + quoteit(cp.getDbUrl()));
            log.info("Eval dbhost ok");
            
            /*
            log.info("library load attempt");
            connection.eval("library(\"plyr\")");
            log.info("library ok");
            */
            
            log.info("script execution attempt");
		    REXP rx = connection.parseAndEval("try(eval(parse(text=.tmp.)),silent=TRUE)");
            //log.info("source(\"" + sFileName + "\")");
            //REXP rx = connection.eval("try(source(\"" + sFileName + "\"),silent=TRUE");
            //REXP rx = connection.eval("source(\"/home/ubuntu/Documents/TestScript.R\")");
            //connection.serverSource(sFileName);

            
		    if (rx.inherits("try-error")) {
		    	try {
	            	log.error("R error: " + rx.toDebugString());
	            	resultReport.add(rx.toDebugString());
	            	log.error("R expression: " + rx.toString());
	            }
	            catch (NullPointerException e) {
	            	log.info("R session did not set debug string");
	            	resultReport.add("An error occurred, but R session did not set debug string");
	            }
		    	bR = false;
		    }
		    else {
		    	try {
	            	log.info("R debug result: " + rx.toDebugString());
	            	resultReport.add(rx.toDebugString());
	            }
	            catch (NullPointerException e) {
	            	log.info("R session did not set debug string");
	            }
		    	bR = true;
		    }
		    
		    
		    try {
		    	String errMsg = connection.eval("errMsg").asString();
		    	resultReport.add(errMsg);
		    } catch (RserveException e) {}
		    
		    
		    
		} catch (RserveException e) {
			bR = false;
			log.error("Rserve Exception: " + e.getMessage());
			resultReport.add("Rserve Exception: " + e.getMessage());
			try {
				REXP rx = connection.parseAndEval("geterrmessage()");
				log.error("R Engine error: " + rx.toString());
				if (rx instanceof REXPString) {
					REXPString rexps = (REXPString) rx;
					log.error("R Engine error: " + rexps.toDebugString());
				}
			} catch (REngineException e1) {
				log.error("R engine exception on error message get: " + e.getMessage());
				e1.printStackTrace();
			} catch (REXPMismatchException e1) {
				log.error("R mismatch exception on error message get: " + e.getMessage());
				e1.printStackTrace();
			}
			e.printStackTrace();
		} catch (REXPMismatchException e) {
			bR = false;
			log.error("REXPMismatchException error: " + e);
			resultReport.add("REXPMismatchException error: " + e);
			e.printStackTrace(); 
		} catch (IOException e) {
			bR = false;
			log.error("File i/o error");
			resultReport.add("File i/o error: " + e);
			e.printStackTrace();
		} catch (REngineException e) {
			bR = false;
			log.error("REngineException error: " + e);
			resultReport.add("REngineException error: " + e);
			e.printStackTrace();
		} 
		finally {
			if (connection != null) {
				log.info("Closing Rserve connection");
				try {
					connection.shutdown();
				} catch (RserveException e) {
					e.printStackTrace();
				}
				connection.close();
			}
		}

		return bR;
		
	}
	
	/**
	private String getRLibraries () {
		
		StringBuilder sb = new StringBuilder();
		sb.append(".libPaths( c( ");
		
		for (String s : possibleRLibraries) {
			sb.append('"');
			sb.append(s);
			sb.append('"');
			sb.append(',');
		}
		
		sb.setLength(sb.length() - 1);
		
		sb.append(") ) ");
		
		return sb.toString();
	}
	
	
	private String [] possibleRLibraries = {
			"/home/ubuntu/R/x86_64-pc-linux-gnu-library/3.3",
			"/usr/local/lib/R/site-library",
			"/usr/lib/R/site-library",
			"/usr/lib/R/library"
	};
	*/
	
	
	private static String  quoteit (String s) {
		return new StringBuilder().append('"').append(s).append('"').toString();
	}
	

	public static void main(String[] args) {
		
		
		HashMap<String, String> parameters = RunParameters.parameters;
		parameters.put("clientID", "Test");
		parameters.put("runname", "TriggerFix");
		parameters.put("rundate", "20150911");
		parameters.put("jobuid", "1084");
		parameters.put("metadata", "/ecrfiles/input/5.3.004 Metadata - Licensee - 2015-09-11_FULL.xml");
		parameters.put("configfolder", "C:\\workspace\\ECR_Analytics\\trunk\\EpisodeConstruction\\src\\");
		parameters.put("env", "prd");
		parameters.put("studybegin", "20120101");
		parameters.put("studyend", "20140731");
		parameters.put("jobname", "Test_TriggerFix20150911");
		parameters.put("mapname", "hci3");
		parameters.put("stepname", ProcessJobStep.STEP_RA_SA_MODEL);
		
		
		PostConstruction instance =   new PostConstruction(parameters);
		instance.process();
		
		//System.out.println("Not what I meant to do");

	}
	
	
	
	private static org.apache.log4j.Logger log = Logger.getLogger(PostConstruction.class);
	
	

	
	

}
