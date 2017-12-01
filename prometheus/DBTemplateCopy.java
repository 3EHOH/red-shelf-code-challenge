



public class DBTemplateCopy {
	
	
	private String sourceEnv;
	private String targetEnv;

	

	private static final String SQL_CREATE_TEST_DB = "CREATE SCHEMA IF NOT EXISTS ";
	//private static final String SQL_SHOW_TABLES = "SHOW TABLES FROM ";
	//private static final String SQL_SHOW_TABLES_V = "SELECT table_name FROM v_catalog.all_tables WHERE schema_name = ";

	
	HashMap<String, String> parameters;
	
	

	public DBTemplateCopy (String sourceEnv, String targetEnv) {
		this.sourceEnv = sourceEnv;
		this.targetEnv = targetEnv;
	}
	
	public DBTemplateCopy(String sourceEnv, String targetEnv,
			HashMap<String, String> parameters) {
		this.sourceEnv = sourceEnv;
		this.targetEnv = targetEnv;
		this.parameters = parameters;
	}

	/**
	 * copy all table definitions from template db to target db
	 */
	public void process () {
		
		if( ! parameters.containsKey("jobname"))
			throw new IllegalStateException ("You must set the jobname parameter before invoking this method");
		
		ConnectParameters cpSource = new ConnectParameters(sourceEnv);
		Connection srcConn = ConnectionHelper.getConnection(cpSource);
		Statement query;
		
		ConnectParameters cpTarget = new ConnectParameters(targetEnv);
		Connection trgConn = ConnectionHelper.getConnection(cpTarget);
		Statement create;
		
		
		try {
			
			query = srcConn.createStatement();
		
			create = trgConn.createStatement();
			create.execute(SQL_CREATE_TEST_DB + parameters.get("jobname"));
			
			log.info("Schema created: " + parameters.get("jobname"));
			
			ArrayList<String> views = new ArrayList<String>();
			ArrayList<String> tables = new ArrayList<String>();
			ResultSet rs = null;
		    DatabaseMetaData meta = srcConn.getMetaData();
		    rs = meta.getTables(null, null, null, new String[]{"VIEW"});

		    while (rs.next()) {
		    	log.info("View: " + rs.getString("TABLE_SCHEM") + "|" + rs.getString("TABLE_NAME"));
		    	if (cpSource.getDialect().equalsIgnoreCase("vertica")  &&  rs.getString("TABLE_SCHEM").equals(cpSource.getSchema())) {
		    		views.add(rs.getString("TABLE_NAME"));
		    	} else if (cpSource.getDialect().equalsIgnoreCase("mysql")) {
		    		views.add(rs.getString("TABLE_NAME"));
		    	}
		    }
		    
		    rs = meta.getTables(null, null, null, new String[] {"TABLE"});
		    while (rs.next()) {
		    	if (cpSource.getDialect().equalsIgnoreCase("vertica")  &&  rs.getString("TABLE_SCHEM").equals(cpSource.getSchema())) {
		    		tables.add(rs.getString("TABLE_NAME"));
		    	} else if (cpSource.getDialect().equalsIgnoreCase("mysql")) {
		    		tables.add(rs.getString("TABLE_NAME"));
		    	}
		    	// log.info("Table: " + rs.getString("TABLE_SCHEM") + "|" + rs.getString("TABLE_NAME"));
		    }


		    /*
		    String sShow;
		    if (cpSource.getDialect().equalsIgnoreCase("vertica"))
		    	sShow = SQL_SHOW_TABLES_V + "'" + cpSource.getSchema() + "'";
		    else
		    	sShow = SQL_SHOW_TABLES + cpSource.getSchema();
		    
			ResultSet result = query.executeQuery(sShow);
		
			while(result.next()) {
				//String tableName = result.getString(result.getMetaData().getColumnName(1)); //Retrieves table name from column 1   
				String tableName = result.getString(1);
				if (views.contains(tableName))
					continue;
				create.execute("CREATE TABLE IF NOT EXISTS " + parameters.get("jobname") + "." + tableName + 
						" LIKE " +  cpSource.getSchema() + "." + tableName); 				//Create new table in test2 based on production structure
			}
			*/
		    for (String s : tables) {
		    	create.execute("CREATE TABLE IF NOT EXISTS " + parameters.get("jobname") + "." + s + 
						" LIKE " +  cpSource.getSchema() + "." + s +
						// (cpSource.getDialect().equalsIgnoreCase("vertica") ? " /*+ DIRECT */ " : "" )  	//  vertica 7.2
		    			(cpSource.getDialect().equalsIgnoreCase("vertica") ? " DIRECT " : "" ) 	)			//  vertica 8.0
						; 				//Create new table
		    }
			
			//create.execute("CREATE OR REPLACE VIEW " + parameters.get("jobname") + ".episode_information AS SELECT * FROM " + parameters.get("jobname") + ".build_episode_reference");
		    
		    log.info("Tables created: " + parameters.get("jobname"));
		    
		    if (cpSource.getDialect().equalsIgnoreCase("vertica")){
		    	for (String s : views) {
		    		ResultSet result = query.executeQuery("SELECT EXPORT_OBJECTS('','" + cpSource.getSchema() + "." + s + "')");
		    		result.next();
		    		log.info("SELECT EXPORT_OBJECTS('','" + cpSource.getSchema() + "." + s + "')");
		    		StringBuilder sDef = new StringBuilder(result.getString("EXPORT_OBJECTS"));
		    		sDef.delete(0, sDef.indexOf(" AS") + 3);
		    		sDef.setLength(sDef.indexOf(";"));
		    		log.info(sDef);
		    		create.execute("CREATE OR REPLACE VIEW " + parameters.get("jobname") + "." + s + " AS " + sDef);
		    	}
		    } else {
		    	for (String s : views) {
		    		ResultSet result = query.executeQuery("SELECT VIEW_DEFINITION FROM INFORMATION_SCHEMA.VIEWS"
		    				+ " WHERE TABLE_SCHEMA = '" + cpSource.getSchema() 
		    				+ "' AND TABLE_NAME = '" + s +"'");
		    		result.next();
		    		create.execute("CREATE OR REPLACE VIEW " + parameters.get("jobname") + "." + s + " AS " + result.getString("VIEW_DEFINITION"));
		    	}
		    }
		    
		    
		    log.info("Views created: " + parameters.get("jobname"));
		    
		    // create the sequence (or sequence table)
		    if (cpSource.getDialect().equalsIgnoreCase("vertica")){
		    	create.execute("CREATE SEQUENCE " + parameters.get("jobname") + ".hibernate_sequence INCREMENT BY " + HibernateHelper.BATCH_INSERT_SIZE);
			    log.info("Sequence created: " + parameters.get("jobname"));
		    } else {
		    	create.execute("CREATE TABLE IF NOT EXISTS " + parameters.get("jobname") + ".hibernate_sequence ( next_val BIGINT(20) NOT NULL AUTO_INCREMENT, PRIMARY KEY (next_val))");
		    	create.execute("INSERT INTO " + parameters.get("jobname") + ".hibernate_sequence VALUES (50001)");
			    log.info("Sequence table created: " + parameters.get("jobname"));
		    }

		
		} catch (SQLException e) {
			log.error(e);
			e.printStackTrace();
		}
		
	}
	
	
	

	

	public static void main(String[] args) {

		log.info("Starting DB Template Copy");

		DBTemplateCopy instance = new DBTemplateCopy("template", "prd");
		
		// get parameters and make them available to all
		RunParameters rp = new RunParameters();
		instance.parameters = rp.loadParameters(args, parameterDefaults);
				
		String s = instance.parameters.get("runname") + instance.parameters.get("rundate");
		instance.parameters.put("jobname", s);
		
				
		RunParameters.parameters.put("configfolder", instance.parameters.get("configfolder"));
		
		instance.process();
		
		
		log.info("Completed DB Template Copy");
		

	}
	
	
	static String [][] parameterDefaults = {
		{"configfolder", "c:/workspace/trunk/EpisodeConstruction/"},
		{"rundate", "20160720"},
		{"runname", "PEBTF_maptest"}
	};
	

	private static org.apache.log4j.Logger log = Logger.getLogger(DBTemplateCopy.class);
	
	

}
