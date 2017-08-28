



public class MapToDatabaseLoader {

	public static void main(String[] args) {

		log.info("Map load to database started");
		MapToDatabaseLoader instance = new MapToDatabaseLoader();
		// get parameters and make them available to all
		RunParameters rp = new RunParameters();
		instance.parameters = rp.loadParameters(args, parameterDefaults);
		instance.processAllMaps(folder);
		log.info("Map load to database completed");

	}

	
	HashMap<String, String> parameters;
	
	
	private void processAllMaps (final File folder) {
		
		// get all the maps 
		for (final File fileEntry : folder.listFiles()) {
	        if (fileEntry.isDirectory()) {
	            processAllMaps(fileEntry);
	        } else {
	            System.out.println(fileEntry.getName());
	            processEachMap(fileEntry);
	        }
	    }
		
		List<Class<?>> cList = new ArrayList<Class<?>>();
		cList.add(MapEntry.class);
		HibernateHelper h = new HibernateHelper(cList, "ecr", "ecr");
		SessionFactory factory = h.getFactory("ecr", "ecr");
		Session session = factory.openSession();	

		// clear the table
		session.createSQLQuery("truncate table inputMap").executeUpdate();
				
		// put all the maps in
		Transaction tx = null;
		try{
			tx = session.beginTransaction();
			for (MapEntry me : maps ) {
				session.save(me); 
			}
			tx.commit();
		}catch (HibernateException e) {
			if (tx!=null) tx.rollback();
			e.printStackTrace(); 
		}finally {
			session.close(); 

			/*
			try {
				h.close("prd", "ecr");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			*/
			
		}
		
	}
	
	
	private void processEachMap (File f) {
		
		String sName = f.getName().substring(0, f.getName().indexOf('.'));
		String content = "";
		try {
			content = new String(Files.readAllBytes(f.toPath()));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		maps.add(new MapEntry(sName, content));
		log.info("Added " + sName + " with " + content);
		
	}
	
	
	ArrayList<MapEntry> maps = new ArrayList<MapEntry>();
	
	
	static String [][] parameterDefaults = {
		{"configfolder", "C:/workspace/ECR_Analytics/trunk/EpisodeConstruction/src/"}
	};
	
	
	private static final File folder = new File("C:\\workspace\\ECR_Analytics\\trunk\\EpisodeConstruction\\maps");


	private static org.apache.log4j.Logger log = Logger.getLogger(MapToDatabaseLoader.class);
	
	

}
