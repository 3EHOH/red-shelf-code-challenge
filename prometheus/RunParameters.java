

public class RunParameters {
	
	/**
	 * static parameters
	 * if you are sure you are not multi-threading, go ahead and use the static
	 */
	static public HashMap<String, String> parameters = new HashMap<String, String>();
	
	/**
	 * load default parameters and 
	 * put any run arguments in the hash map as well
	 * arguments should take the form keyword=value (e.g., studybegin=20140101)
	 * @param args
	 * @param parameterDefaults
	 * @param sRunName
	 * @return a parameter hash map
	 */
	@SuppressWarnings("unchecked")
	public synchronized HashMap<String, String> loadParameters (String[] args, String [][] parameterDefaults) {
		
		// load any default parameters from the default parameter array
		if (parameterDefaults != null)
		for (int i = 0; i < parameterDefaults.length; i++) {
			parameters.put(parameterDefaults[i][0], parameterDefaults[i][1]);
		}
		
		// overlay or add any incoming parameters
		for (int i = 0; i < args.length; i++) {
			parameters.put(args[i].substring(0, args[i].indexOf('=')), args[i].substring(args[i].indexOf('=')+1)) ;
		}
		
		// see if a run name was presented in the arguments
		String sRunName = parameters.get("runname");
		if (sRunName == null) {
			sRunName = "Test";
		}
		
		// see if a run name was presented in the arguments
		String sRunDate = parameters.get("rundate");
		if (sRunDate == null) {
			sRunDate = DateUtil.generateRunDate();
			parameters.put("rundate", sRunDate);
		}
		
		jobParameters.put(sRunName+sRunDate, (HashMap<String, String>) parameters.clone());
		
		return jobParameters.get(sRunName+sRunDate);
		
	}
	
	
	public HashMap<String, String> loadParameters (String[] args) {
		return loadParameters(args, null);
	}
	
	public HashMap<String, HashMap<String, String>> jobParameters = new HashMap<String, HashMap<String, String>>();

}
