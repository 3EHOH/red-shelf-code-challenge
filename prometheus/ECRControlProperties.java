



public class ECRControlProperties {
	
	private static boolean bLoaded = false;
	
	private InputStream inputStream;
	
	private ECRControl _e;
	
	
	public ECRControlProperties() throws IOException {
		loadPropValues();
	}
 
	
	public ECRControl get_ecrControl() {
		return _e;
	}


	private synchronized void loadPropValues() throws IOException {
		
		if (bLoaded)
			return;
 
		try {
			
			Properties prop = new Properties();
			File f= new File(".");
			String propFileName = (f.getAbsolutePath()).substring(0, f.getAbsolutePath().lastIndexOf('.') ) + "ecrcontrol.properties";
			
			//String propFileName = "C:\\workspace\\ECR_Analytics\\trunk\\EpisodeConstruction\\database.properties";
			//System.out.println(propFileName);
			
			//try (InputStream in = new FileInputStream(propFileName); 
 
			//inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);
			inputStream = new FileInputStream(propFileName); 
			if (inputStream != null) {
				prop.load(inputStream);
			} else {
				throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
			}
			
			_e = new ECRControl();
			
			_e.setArchiveScript(prop.getProperty("archive.script"));
 
			log.info("Loaded ecrcontrol.properties");
			bLoaded = true;
			
		} catch (Exception e) {
			log.error("Exception: " + e);
		} finally {
			inputStream.close();
		}
		
	}
	
	
	private static org.apache.log4j.Logger log = Logger.getLogger(ECRControlProperties.class);


}
