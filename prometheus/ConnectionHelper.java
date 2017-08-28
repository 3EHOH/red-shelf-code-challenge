


public class ConnectionHelper {
	
	
	
	public static Connection getConnection (ConnectParameters cp)
	{
		
		Connection c = null;
			    
		try
		{
			    	
			log.info("Trying to connect to " + cp.getDbUrl() + "/" + cp.getSchema() + " db OK");
			    	
			// create a java mysql database connection
			String myDriver = "";
			String url = "";
			if (cp.getDialect() == null  ||  cp.getDialect().equalsIgnoreCase("mysql")) {
				myDriver = "org.gjt.mm.mysql.Driver";
				url = "jdbc:mysql://" + cp.getDbUrl() + "/" + cp.getSchema() + "?autoReconnect=true&rewriteBatchedStatements=true";
			} else if (cp.getDialect().equals("vertica")) {
				myDriver = "com.vertica.jdbc.Driver";
				url = "jdbc:vertica://" + cp.getDbUrl() + "/" + "hci3";
			}
			//+ "&useCompression=true";
			Class.forName(myDriver);
			c = DriverManager.getConnection(url, cp.getDbUser(), cp.getDbPw());
					
			//stmt = conn.createStatement();

			log.info("Connected to " + cp.getDbUrl() + "/" + cp.getSchema() + " db OK");

			log.info("Obtained new connection to db OK");
			    	
			//  a simple query to check the db connection
			//new SimpleQuery().testConnection(connection);
			//log.info("Test Query OK with new connection");

		}
		catch (Exception e)
		{
			log.error("DB Connnection exception");
			log.error(e.getMessage(), e);
		}
		
		return c;
		
	}
	
	
	private static org.apache.log4j.Logger log = Logger.getLogger(ConnectionHelper.class);

}
