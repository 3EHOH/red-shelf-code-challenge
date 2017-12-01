



public class ReportDriver {
	
	
	private HashMap<String, String> parameters;
	List<String> classes;
	String sPkg;
	

	public ReportDriver (HashMap<String, String> parameters) {
		this.parameters = parameters;
		initialize();
		process();
	}
	
	private void process() {
		
		for (String sC : classes) {
			
			Class<?> cl;
			
			try {
			
				sC = sC.replace("/", "");
				log.info("Searching for report class " + sC);
				cl = Class.forName(sPkg + "." + sC);
				log.info("Checking " + cl.getName() + " for proper constructor");
				Constructor<?> cons = cl.getConstructor(HashMap.class);
				log.info("Invoking " + cons.getName() + " constructor");
				Object o = cons.newInstance(parameters);
				
				log.info("Found report class " + cons.getName());
				
				Method mFine = o.getClass().getDeclaredMethod("process");
				mFine.setAccessible(true);
				mFine.invoke(o);
				
				log.info("Completed report(s) from class " + cons.getName());
				
			} catch (ClassNotFoundException | NoSuchMethodException | SecurityException e) {
				log.debug("Skipping class: " + sC);
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalArgumentException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvocationTargetException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		
	}
	
	
	/**
	 * get a list of all classes that create reports for this job step
	 */
	private void initialize() {
		
		if (!parameters.containsKey("stepname")) {
			log.error("stepname parameter required to invoke this class");
			throw new IllegalStateException ("stepname parameter required to invoke this class");
		}
		
		
		switch (parameters.get("stepname")) {
			case ProcessJobStep.STEP_POSTMAP_REPORT:
				sPkg = "report.postmap";
				break;
			case ProcessJobStep.STEP_POSTNORMALIZATION_REPORT:
				sPkg = "report.postnorm";
				break;
			case ProcessJobStep.STEP_CONSTRUCTION_REPORT:
				sPkg = "report.postconst";
				break;
			default:
				log.error("invalid stepname parameter used to invoke this class");
				throw new IllegalStateException ("invalid stepname parameter used to invoke this class");
		}
		
		try {
			classes = ReportHelper.getClassNamesFromPackage(sPkg );
		} catch (IOException | URISyntaxException e) {
			log.error("Report driver failure in obtaining report class(es): " + e.getMessage());
			e.printStackTrace();
		}
		
		if (classes == null  ||  classes.isEmpty()) {
			log.error("No reports found to invoke this class for " + parameters.get("stepname"));
			throw new IllegalStateException ("No reports found to invoke this class for " + parameters.get("stepname"));
		}
		else
			log.info(classes.size() + " reports found to invoke this class for " + parameters.get("stepname"));
		
	}
	
	
	private static org.apache.log4j.Logger log = Logger.getLogger(ReportDriver.class);
	
	
	
	/**
	 * main strictly for unit testing
	 * @param args
	 */
	public static void main(String[] args) {
		
		HashMap<String, String> parameters = RunParameters.parameters;
		parameters.put("clientID", "CHC");
		parameters.put("runname", "CHC_V5_3_6_2014_6_2015");
		parameters.put("rundate", "20150721");
		parameters.put("jobuid", "1056");
		parameters.put("testlimit", "200");
		parameters.put("configfolder", "C:\\workspace\\ECR_Analytics\\trunk\\EpisodeConstruction\\src\\");
		parameters.put("env", "prd");
		parameters.put("studybegin", "20120101");
		parameters.put("studyend", "21120101");
		parameters.put("jobname", "CHC_CHC_V5_3_6_2014_6_201520150721");
		//parameters.put("stepname", ProcessJobStep.STEP_POSTMAP_REPORT);
		parameters.put("stepname", ProcessJobStep.STEP_POSTNORMALIZATION_REPORT);
		
		new ReportDriver(parameters);
		
		//System.out.println("Not what I meant to do");

	}
	

}
