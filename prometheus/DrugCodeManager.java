



public class DrugCodeManager {
	
	
	// Production Episode Builder database 
	private static String		schema 	= "episode_builder";
	private static String 		dbUrl 	= "216.70.108.40:3306";
	private static String 		dbUser 	= "robuilder";
	private static String 		dbPw 	= "robu!ld3r";
	
	private static HashMap<String, String> ndcToCode = new HashMap<String, String>();
	
	private static org.apache.log4j.Logger log = Logger.getLogger(DrugCodeManager.class);
	
	
	static {
	
		Connection conn = null; 
	    
		try
		{

			// create a java mysql database connection
			String myDriver = "org.gjt.mm.mysql.Driver";
			String url = "jdbc:mysql://" + dbUrl + "/" + schema;
			Class.forName(myDriver);
			conn = DriverManager.getConnection(url, dbUser, dbPw);

			log.info("Starting Load of Drug Codes from Episode Builder");
					
			Statement stmt = conn.createStatement();
            ResultSet result = stmt.executeQuery("SELECT * FROM ndc_to_code");
            while(result.next()){
            	ndcToCode.put(result.getString("NDC"), result.getString("CODE_VALUE"));
            }

		}
		catch (Exception e)
		{
			log.error("Got an exception loading ndc to drug code table ");
			log.error(e.getMessage(), e);
		}
		finally
		{
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				log.error("Got an exception closing ndc to drug code table connection");
				log.error(e.getMessage(), e);
			}
		}
		log.info("Completed Load of Drug Codes from Episode Builder");
	}
	
	public static String getCodeFromNDC (String key) {
		return ndcToCode.containsKey(key) ? ndcToCode.get(key) : "";
	}

}
