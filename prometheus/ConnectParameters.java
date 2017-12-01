
//import java.io.FileNotFoundException;

/**
 * The intent of this class is to provide an object that contains connection information
 * for any type of file or table that we may read or write (let's see how that goes)
 * @author Warren
 *
 */

public class ConnectParameters {
	
	
	public ConnectParameters (String environment) {
		this.environment = environment;
		try {
			setDBValues();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	String environment;
	
	
	private void setDBValues() throws IOException {
		 

		Properties prop = new Properties();
		File f= new File(".");
		String propFileName = (f.getAbsolutePath()).substring(0, f.getAbsolutePath().lastIndexOf('.') ) + "database.properties";
		
		//String propFileName = "C:\\workspace\\ECR_Analytics\\trunk\\EpisodeConstruction\\database.properties";
		//System.out.println(propFileName);
		
		try (InputStream in = new FileInputStream(propFileName); 
			     BufferedReader r = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {     
			      //String str = null;
			      //StringBuilder sb = new StringBuilder(8192);
			      //while ((str = r.readLine()) != null) {
			      //  sb.append(str);
			      //}
			      prop.load(in);
			      //System.out.println("data from InputStream as String : " + sb.toString());
			} catch (IOException ioe) {
			  ioe.printStackTrace();
			}
		
		
		//InputStream inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);
		
		
		//System.out.println(getClass());
		//System.out.println(getClass().getClassLoader());
		
		//prop.load(getClass().getResourceAsStream(propFileName));
		
		
		/*
		if (inputStream != null) {
			prop.load(inputStream);
		} else {
			throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
		}
		*/
		
		
 
		
		setDbUrl(prop.getProperty(environment + ".host"));
		setDbUser(prop.getProperty(environment + ".user"));
		setDbPw(prop.getProperty(environment + ".password"));
		setSchema(prop.getProperty(environment + ".schema"));
		if ( prop.getProperty(environment + ".dialect") != null )
			setDialect(prop.getProperty(environment + ".dialect"));
		if ( prop.getProperty(environment + ".database") != null )
			setDatabase(prop.getProperty(environment + ".database"));
		
	}
	
	
	
	private String type;
	private String filename;

	private String		database = "hci3";
	private String		schema 	 = "ECR_Test";
	private String 		dbUrl 	 = "127.0.0.1";
	private String 		dbUser 	 = "epbuilder";
	private String 		dbPw 	 = "xxxxxxxx";
	private String		dialect  = "MySQL";
	
	
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public String getFilename() {
		return filename;
	}
	public void setFilename(String filename) {
		this.filename = filename;
	}
	public String getDatabase() {
		return database;
	}
	public void setDatabase(String database) {
		this.database = database;
	}
	public String getSchema() {
		return schema;
	}
	public void setSchema(String schema) {
		this.schema = schema;
	}
	public String getDbUrl() {
		return dbUrl;
	}
	public void setDbUrl(String dbUrl) {
		this.dbUrl = dbUrl;
	}
	public String getDbUser() {
		return dbUser;
	}
	public void setDbUser(String dbUser) {
		this.dbUser = dbUser;
	}
	public String getDbPw() {
		return dbPw;
	}
	public void setDbPw(String dbPw) {
		this.dbPw = dbPw;
	}
	
	public String getDialect() {
		return dialect;
	}
	public void setDialect(String dialect) {
		this.dialect = dialect;
	}
	public static void main(String[] args) throws IOException {
		
		ConnectParameters instance = new ConnectParameters("prd");
		
		System.out.println(instance.getDbUrl());
		System.out.println(instance.getDbUser());
		
		

	}
	

}
