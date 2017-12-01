



public class MapDefinitionSQL implements MapDefinitionInterface {
	
	
	HibernateHelper h;
	private SessionFactory factory;
	Session session;

	
	public MapDefinitionSQL ()  {
		
		List<Class<?>> cList = new ArrayList<Class<?>>();
		cList.add(MapEntry.class);
		h = new HibernateHelper(cList, "ecr", "ecr");
		factory = h.getFactory("ecr", "ecr");
		
	}

	@Override
	public ArrayList<MapEntry> getAllMapDefinitions() {
		
		session = factory.openSession();
		Transaction tx = null;
		try {
			tx = session.beginTransaction();
			@SuppressWarnings("unchecked")
			List<MapEntry> maps = session.createQuery("FROM MapEntry").list(); 
			for (Iterator<MapEntry> iterator = maps.iterator(); iterator.hasNext();) {
				MapEntry map = iterator.next(); 
				allMaps.add(map);
			}
			tx.commit();
		} catch (HibernateException e) {
			if (tx!=null) tx.rollback();
			e.printStackTrace(); 
		} finally {
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
		  
		return allMaps;

	}
	
	ArrayList<MapEntry> allMaps = new ArrayList<MapEntry>();

	@Override
	public MapEntry getMapDefinition(String sName) {
		
		MapEntry map = null;
		
		if (allMaps.isEmpty())
			getAllMapDefinitions();
		
		for (Iterator<MapEntry> iterator = allMaps.iterator(); iterator.hasNext();) {
			map = iterator.next(); 
			if (map.mapName.equalsIgnoreCase(sName))
				break;
		}
		
		return map;
		  
	}

}
