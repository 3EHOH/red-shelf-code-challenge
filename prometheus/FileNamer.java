


public class FileNamer {
	
	
	static public String getFileName (String sBaseName) {
		
		String sPath = RunParameters.parameters.get("outputPath") == null  ?  sDefaultPath  :  RunParameters.parameters.get("outputPath");
		String today; 
		if (RunParameters.parameters.get("rundate") == null)  
		{
			Date date = Calendar.getInstance().getTime();
			DateFormat formatter = new SimpleDateFormat("yyyy_MM_dd_hhmm");
			today = formatter.format(date);
		}
		else
			 today =  RunParameters.parameters.get("rundate");
		String cleanName =  sBaseName.contains("/") ? sBaseName.substring(sBaseName.lastIndexOf("/") + 1) : sBaseName; 
		cleanName =  cleanName.contains("\\") ? cleanName.substring(cleanName.lastIndexOf("\\") + 1) : cleanName; 
		String sFileName = sPath + cleanName + "_"  + today;
		return sFileName;
	}

	static String sOS = System.getProperty("os.name").toLowerCase();
	static String sDefaultPath;
	
	static {
		if (sOS.startsWith("windows"))
			sDefaultPath = "C:/Users/" + System.getProperty("user.name") + "/My Documents/";
		else
			sDefaultPath = "/home/" + System.getProperty("user.name") + "/Documents/";
	}
	
}
