






public class EpisodeMetaDataFromStore extends EpisodeMetaDataAbstract implements EpisodeMetaDataInterface {
	
	
	
	private List<Class<?>> cList = new ArrayList<Class<?>>();
	HibernateHelper h;
	SessionFactory factory;
	String schemaName;
	
	
	SerializedMetaData _smd = new SerializedMetaData();
	
	
	@Override
	public List<EpisodeMetaData> getAllEpisodes() {
		return epList;
	}
	
	
	public List<String> getAllEMCodes() {
		return emCodeList;
	}
	
	public MetaDataHeader getMetaDataHeader() {
		return mdh;
	}
	
	/*
	 * Constructor
	 * 
	 * actually reads an episode builder database and prepares metadata objects
	 */
	public EpisodeMetaDataFromStore (String name) {
		
		initialize();
		
		if (getMinAndMaxId(name)) {

			buildEpisodesFromTables(name);
		
		
			epList = _smd.epList;
			emCodeList = _smd.emCodeList;
			mdh = _smd.mdh;
		
			//bDebug = true;
			//log.info("MMM -- metadata retrieval check");
			outputCheck();
			//log.info("MMM -- metadata retrieval check over");
			//bDebug = false;
			
		}
		
	} 
	
	

	
	/**
	 * build an episode definition from the episode tag group
	 * @param ep
	 */
	@SuppressWarnings("unchecked")
	private void buildEpisodesFromTables (String name) {
		
		
		Session session = factory.openSession();
		Transaction tx = null;
		
		int chunksize = 10;
		
		
	
		try{
			
			for (int k = selRangeMin; k < selRangeMax; k = k + chunksize ) {
				
				tx = null;

			tx = session.beginTransaction();
		
			String hql = "FROM MetaDataStore WHERE id >= :min AND id <= :max";
			Query query = session.createQuery(hql);
			query.setParameter("min", k);
			query.setParameter("max", k + chunksize);
			if (h.getCurrentConfig().getConnParms().getDialect().equalsIgnoreCase("vertica")) 
			{} 
			else
				query.setMaxResults(chunksize);

			List<MetaDataStore> results = query.list();
			
			//MetaDataStore _mds = (MetaDataStore) results.get(0);
	
			for (MetaDataStore _mds : results ) {
				
				// convert to byte []
				Byte[] bs = _mds.getMetaData(); 
				bytes = new byte[bs.length];
				for (int i=0; i < bs.length; i++) {
					bytes [i] = bs[i];
				}
				
				// convert according to save type
				if (_mds.getClazz().equals(EpisodeMetaData.class.getName())) {
					_smd.epList.add(getObject(EpisodeMetaData.class));
				}
				else if (_mds.getClazz().equals(_smd.mdh.getClass().getName())) {
					_smd.mdh = getObject(_smd.mdh.getClass());
				}
				else if (_mds.getClazz().equals(_smd.emCodeList.getClass().getName())) {
					_smd.emCodeList = getObject(_smd.emCodeList.getClass());
				}
			
			}
			
			
			//_smd = getObject(SerializedMetaData.class);
			//_smd = getObject();

			tx.commit();

			
			}
		
		} catch (HibernateException e) {
		
			if (tx != null) tx.rollback();
			e.printStackTrace();
		
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			
			session.close(); 

		}
		
		
		

		
	}
	
	
	private byte[] bytes;
	
	
	private <T extends Serializable> T getObject(Class<T> type) throws IOException, ClassNotFoundException{
        
		if(bytes == null){
            return null;
        }
        ByteArrayInputStream byteIn = new ByteArrayInputStream(bytes);
        ObjectInputStream in = new ObjectInputStream(byteIn);
        @SuppressWarnings("unchecked")
		T obj = (T) in.readObject();
        in.close();
        return obj;
        
    }
	
	/**
	 * get id range for this metadata row set
	 * @param metadata key (file or table name)
	 * @return false if not found in store
	 */
	private boolean getMinAndMaxId (String name) {
		
		
		Session session = factory.openSession();
		Transaction tx = null;
	
		try{

			tx = session.beginTransaction();
		
			String SQL_QUERY = "SELECT MAX(id), MIN(id) FROM MetaDataStore WHERE loadDate = (SELECT max(loadDate) FROM MetaDataStore WHERE sourceName = :name)"; 
			Query query = session.createQuery(SQL_QUERY); 
			query.setParameter("name", name);

			Object[]  results = (Object[]) query.list().get(0);
			
			selRangeMax = (Integer) results[0];
			selRangeMin = (Integer) results[1];

			tx.commit();

		
		} catch (HibernateException e) {
		
			if (tx != null) tx.rollback();
			e.printStackTrace();
		

		} finally {
			
			session.close(); 

		}
		
		return selRangeMax == null ? false : true;
		
	}
	
	
	private Integer selRangeMax, selRangeMin;
	
	
	
	private void initialize () {
		
		
		cList.add(MetaDataStore.class);
		
		h = new HibernateHelper(cList, "prd", "ecr");
		factory = h.getFactory("prd", "ecr");
		
	}
	

	
	
	/**
	 * a main strictly for testing
	 * @param args
	 */
	public static void main(String[] args) {

		Date start = new Date();
		bDebug = true;
		loadParameters(args);
	
		new EpisodeMetaDataFromStore("sql://episode_builder-5.3.006");
		 
		log.info("Started: " + start + " Ended: " + new Date());
		
	}


	
	private static void loadParameters (String[] args) {
		// load any default parameters from the default parameter array
		for (int i = 0; i < parameterDefaults.length; i++) {
			parameters.put(parameterDefaults[i][0], parameterDefaults[i][1]);
		}
		// overlay or add any incoming parameters
		for (int i = 0; i < args.length; i++) {
			parameters.put(args[i].substring(0, args[i].indexOf('=')), args[i].substring(args[i].indexOf('=')+1)) ;
		}
		
	}
	
	static HashMap<String, String> parameters = RunParameters.parameters;
	static String [][] parameterDefaults = {
			{"IP_TABLE", "sql://episode_builder-5.3.006"},
			{"configfolder", "C:\\workspace\\ECR_Analytics\\trunk\\EpisodeConstruction\\src\\"}
	};
	
	
	
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	
	private static org.apache.log4j.Logger log = Logger.getLogger(EpisodeMetaDataFromSQL.class);
	

}
