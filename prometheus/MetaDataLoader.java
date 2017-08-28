






/**
 * This class loads metadata from various sources and stores it in the ECR schema
 * @author Warren
 *
 */


public class MetaDataLoader {
	
	
	private List<Class<?>> cList = new ArrayList<Class<?>>();
	HibernateHelper h;
	SessionFactory factory;

	
	
	private HashSet<String> metaDataSources;
	
	
	private void processAllMetaData () {
		
		Date start = new Date();
		
		initialize();
		
		metaDataSources = getAllMetaDataSources();

		EpisodeMetaDataInterface _epd = null;
		
		for (String s : metaDataSources) {
			
			if ( s.toLowerCase().startsWith(SQL_URL_PREFIX)) {
				_epd = new EpisodeMetaDataFromSQL(s);
			} else  {
				try {
					_epd = new EpisodeMetaDataFromXML(s);
				} catch (ParserConfigurationException e) {
					log.warn("Could not parse file: " + s);
				} catch (SAXException e) {
					log.warn("Could not parse XML in file: " + s);
				} catch (IOException e) {
					log.warn("Could not locate or read file: " + s);;
				}
			}
			
			if (_epd != null) {
			
				SerializedMetaData _sd = new SerializedMetaData();
				_sd.epList = (ArrayList<EpisodeMetaData>) _epd.getAllEpisodes();
				_sd.emCodeList = (ArrayList<String>) _epd.getAllEMCodes();
				_sd.mdh = _epd.getMetaDataHeader();
			
				writeMetaData(_sd, s, _epd.getMetaDataHeader().getExport_version());
			
			}
			
		}
		

		log.info("Started: " + start + " Ended: " + new Date());
		
	}
	
	
	private HashSet<String> getAllMetaDataSources () {
		
		HashSet<String> rSet = new HashSet<String>();
		
		Session session = factory.openSession();
		Transaction tx = null;
	
		try{

			tx = session.beginTransaction();
		
			String hql = "FROM ProcessJobParameter WHERE p_key = 'metadata')";
			Query query = session.createQuery(hql);

			@SuppressWarnings("unchecked")
			List<ProcessJobParameter> results = query.list();
			
			for (ProcessJobParameter _PJP : results) {
				rSet.add(_PJP.getP_value());
			}

			tx.commit();

		
		} catch (HibernateException e) {
		
			if (tx != null) tx.rollback();
			e.printStackTrace();
		
		} finally {
			
			session.close(); 

		}
		
		
		return rSet;
		
	}

	
	/**
	 * save metadata to db
	 * @param emd
	 */
	private void writeMetaData(SerializedMetaData _sd, String sName, String sVersion) {
		
		/*
		if (sName.startsWith(SQL_URL_PREFIX)) {
			log.info("SQL out");
			dBug(_sd);
		}
		*/
		
		// set common date for the metadata set
		Date loadDate = new Date();
		
		Session session = factory.openSession();
		Transaction tx = null;
	
		try{

			tx = session.beginTransaction();
			
			// metadata header
			MetaDataStore mds = new MetaDataStore();
			mds.setSourceName(sName);
			mds.setSourceType( sName.toLowerCase().startsWith(SQL_URL_PREFIX) ? MetaDataStore.SOURCE_TYPE_DB : MetaDataStore.SOURCE_TYPE_FILE);
			mds.setVersion(sVersion);
			mds.setLoadDate(loadDate);
			mds.setClazz(_sd.mdh.getClass().getName());
			mds.setMetaData(serializeMetaData(_sd.mdh));
			session.save(mds);
			
			// e&m codes
			mds = new MetaDataStore();
			mds.setSourceName(sName);
			mds.setSourceType( sName.toLowerCase().startsWith(SQL_URL_PREFIX) ? MetaDataStore.SOURCE_TYPE_DB : MetaDataStore.SOURCE_TYPE_FILE);
			mds.setVersion(sVersion);
			mds.setLoadDate(loadDate);
			mds.setClazz(_sd.emCodeList.getClass().getName());
			mds.setMetaData(serializeMetaData(_sd.emCodeList));
			session.save(mds);
			
			// episodes
			for (EpisodeMetaData _e : _sd.epList) {
				mds = new MetaDataStore();
				mds.setSourceName(sName);
				mds.setSourceType( sName.toLowerCase().startsWith(SQL_URL_PREFIX) ? MetaDataStore.SOURCE_TYPE_DB : MetaDataStore.SOURCE_TYPE_FILE);
				mds.setVersion(sVersion);
				mds.setLoadDate(loadDate);
				mds.setClazz(_e.getClass().getName());
				mds.setMetaData(serializeMetaData(_e));
				session.save(mds);
			}
			
			tx.commit();

		
		} catch (HibernateException e) {
		
			if (tx != null) tx.rollback();
			e.printStackTrace();
		
		} finally {
			
			session.close(); 

		}
		
	}
	
	
	/*
	private void dBug(SerializedMetaData _sd) {
		for (EpisodeMetaData e : _sd.epList) {
			log.info(e);
			int i=0;
			for (PxCodeMetaData p : e.getPx_codes()) {
				log.info(p);
				i++;
				if (i>10)
					break;
			}
		}
	}
	*/
	
	
	
	private Byte[] serializeMetaData (Object _o) {
		
		Byte[] bytes = null;
		byte[] bs = null;
		
		try {
			ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
			ObjectOutputStream objOut = new ObjectOutputStream(byteOut);
			objOut.writeObject(_o);
			objOut.close();
			byteOut.close();
			bs = byteOut.toByteArray();
			bytes = new Byte[bs.length];
			for (int i=0; i < bs.length; i++) {
				bytes [i] = new Byte(bs[i]);
			}

		}
		catch (IOException e) {
			e.printStackTrace();
		}
		
		
		return bytes;
		
	}
	
	
	
	
	private void initialize () {
		
		cList.add(MetaDataStore.class);
		cList.add(ProcessJobParameter.class);

		h = new HibernateHelper(cList, "ecr", "ecr");
		factory = h.getFactory("ecr", "ecr");
		
	}
	
	private static final String SQL_URL_PREFIX = "sql://";


	
	

	public static void main(String[] args) {

		log.info("MetaData load to database started");
		MetaDataLoader instance = new MetaDataLoader();
		// get parameters and make them available to all
		RunParameters rp = new RunParameters();
		instance.parameters = rp.loadParameters(args, parameterDefaults);
		instance.processAllMetaData();
		log.info("MetaData load to database completed");

	}

	
	HashMap<String, String> parameters = RunParameters.parameters;
	
	static String [][] parameterDefaults = {
		{"IP_TABLE", "sql://episode_builder-5.3.006"},
		{"configfolder", "C:/workspace/ECR_Analytics/trunk/EpisodeConstruction/src/"}
	};
	
	
	private static org.apache.log4j.Logger log = Logger.getLogger(MetaDataLoader.class);
	
	
	

}
