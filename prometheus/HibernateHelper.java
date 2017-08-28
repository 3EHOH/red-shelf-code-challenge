





public class HibernateHelper {
	
	
	
	public static int BATCH_INSERT_SIZE = 100;
	
	
	private static List<HibernateConfig> hibes = new ArrayList<HibernateConfig>();
	private HibernateConfig currentConfig;
	

	public HibernateHelper (List<Class<?>> cList, String env)  {
		this(cList, env, null);
	}

	
	public HibernateHelper (List<Class<?>> cList, String env, String schemaName)  {
		
		
		log.debug("Configuring SQL: " + env + ":" + schemaName);
		
		currentConfig = findHibernateConfig (env, schemaName);
		
		
		if (currentConfig == null) {
			genNewConfig(cList, env, schemaName);
		}
		else if (currentConfig.factory.isClosed()) {
			try {
				close(env, schemaName);
			} catch (Exception e) {
				e.printStackTrace();
			}
			genNewConfig(cList, env, schemaName);
		}
		else {
			
		
			log.debug("Reusing config for " + env + "|" + schemaName);
		
			registerService(cList);
			
			//currentConfig.factory.openSession();
			
			
		}
		
		
		
		
		
	}
	
	/**
	 * start a new factory for these parameters
	 * @param env
	 * @param schemaName
	 */
	private void genNewConfig (List<Class<?>> cList, String env, String schemaName)  {
		
		log.debug("Generating new config for " + env + "|" + schemaName);
		
		currentConfig = new HibernateConfig();
		hibes.add(currentConfig);
		
		currentConfig.schemaName = schemaName;
		currentConfig.env = env;
	
		// set location of config files
		String sDir;
		if ((sDir = RunParameters.parameters.get("configfolder")) == null) 
			sDir = "";
		File f = new File(sDir +"hibernate.cfg.xml");
	
		java.util.logging.Logger.getLogger("org.hibernate").setLevel(Level.SEVERE);
	
		// start a configuration
		currentConfig.configuration = new Configuration().configure(f);
	
		// get connect information from database.properties
		currentConfig.setConnParms(new ConnectParameters(env));
	
		// override schema name if constructed with non-null schema name
		if (schemaName != null)
			currentConfig.getConnParms().setSchema(currentConfig.schemaName);
	
		log.debug("Config file: " + f);
		log.debug("URL: " + currentConfig.getConnParms().getDbUrl() + " User: " + currentConfig.getConnParms().getDbUser());
	
	
		// override the hibernate.cfg.xml
		// url format jdbc:mysql://54.200.108.113/ecr
		
		currentConfig.configuration.setProperty("hibernate.connection.username", currentConfig.getConnParms().getDbUser());
		currentConfig.configuration.setProperty("hibernate.connection.password", currentConfig.getConnParms().getDbPw());
		if (currentConfig.getConnParms().getDialect().equalsIgnoreCase("mysql")) {
			currentConfig.configuration.setProperty("hibernate.connection.url", "jdbc:mysql://" + currentConfig.getConnParms().getDbUrl() + "/" + currentConfig.getConnParms().getSchema());
			currentConfig.configuration.setProperty("hibernate.dialect", "org.hibernate.dialect.MySQL5Dialect");
			currentConfig.configuration.setProperty("hibernate.connection.driver_class", "com.mysql.jdbc.Driver");
		}
		else if (currentConfig.getConnParms().getDialect().equalsIgnoreCase("vertica")) {
			currentConfig.configuration.setProperty("hibernate.connection.url", "jdbc:vertica://" + currentConfig.getConnParms().getDbUrl() + "/" + currentConfig.getConnParms().getDatabase()); //currentConfig.connParms.getSchema());
			currentConfig.configuration.setProperty("hibernate.dialect", "org.hibernate.dialect.VerticaDialect");
			currentConfig.configuration.setProperty("hibernate.connection.driver_class", "com.vertica.jdbc.Driver");
			currentConfig.configuration.setProperty("hibernate.default_schema", currentConfig.getConnParms().getSchema());
			BATCH_INSERT_SIZE = 1000;
			//currentConfig.configuration.addResource("com/hci3/cfg/vertica.hbm.xml");
		}
		currentConfig.configuration.setProperty("hibernate.show_sql", "false");
		currentConfig.configuration.setProperty("hibernate.generate_statistics", "false");
		currentConfig.configuration.setProperty("hibernate.use_sql_comments", "false");
	
		currentConfig.configuration.setProperty("hibernate.jdbc.batch_size", Integer.toString(BATCH_INSERT_SIZE));
		currentConfig.configuration.setProperty("hibernate.cache.use_second_level_cache", "false");
		currentConfig.configuration.setProperty("hibernate.jdbc.use_get_generated_keys", "true");
		currentConfig.configuration.setProperty("hibernate.order_inserts", "true");
		currentConfig.configuration.setProperty("hibernate.order_updates", "true"); 
		currentConfig.configuration.setProperty("hibernate.hbm2ddl.auto", ""); 
	
		currentConfig.configuration.setProperty("hibernate.connection.release_mode", "after_transaction");

		
		registerService(cList);
		
		
	}
	
	
	private void registerService (List<Class<?>> cList) {
		
		Iterator<PersistentClass> it; 
		PersistentClass x;
		
		// add any annotated classes that aren't already mapped
		boolean bChange = false;
		boolean bFound = false;
		for (Class<?> c : cList) {
			bFound = false;
			it = currentConfig.configuration.getClassMappings();
			while (it.hasNext() ) {
				x = it.next();
				if (x.getClassName().equals(c.getName())) {
					bFound = true;
					break;
				}
			}
			if (!bFound) {
				bChange = true;
				currentConfig.configuration.addAnnotatedClass(c);
				log.debug("Config change due to: " + c.getName());
			}
		}
				
		// if a factory exists with all the required classes, just use existing factory
		// otherwise, close the current factory and start a new one
		if (bChange) {
			
			if (currentConfig.factory != null)
				currentConfig.factory.close();
			
			// register the service
			currentConfig.reg = new StandardServiceRegistryBuilder().applySettings(
				currentConfig.configuration.getProperties()).build();
			
			java.util.logging.Logger.getLogger("org.hibernate").setLevel(Level.SEVERE);
			
		
			// readied factory, always get this when done
			currentConfig.factory = currentConfig.configuration.buildSessionFactory(currentConfig.reg);
		
			//currentConfig.session = currentConfig.factory.openSession();
			//currentConfig.session.setCacheMode(CacheMode.IGNORE);
			//currentConfig.session.setFlushMode(FlushMode.COMMIT);
		
		}
		
	}
	
	
	

	/**
	 * get a factory fully configured during object construction
	 * @return
	 */
	public SessionFactory getFactory(String env, String schemaName) {
		return findHibernateConfig(env, schemaName).factory;
	}
	
	/**
	 * get a factory fully configured during object construction
	 * @return
	 */
	/*
	public Session getSession(String env, String schemaName) {
		return findHibernateConfig(env, schemaName).session;
	}
	*/
	
	
	public void close(String env, String schemaName) throws Exception {
		
		for (int i=0; i < 25; i++) {
		
			HibernateConfig r = findHibernateConfig(env, schemaName);
		
			if (r != null) {
				//r.session.close();
				if (r.factory != null) {
					r.factory.close();
				}
				if(r.builder!= null) {
					StandardServiceRegistryBuilder.destroy(r.reg);
				}
				
				hibes.remove(r);
			
			}
			else
				break;
			
		
		}
		
	}
	
	/*
	public void close() throws Exception {
		
		if (factory != null) {
			factory.close();
		}
		if(builder!= null) {
			StandardServiceRegistryBuilder.destroy(reg);
		}
		
		
	}
	*/
	
	
	
	private HibernateConfig findHibernateConfig (String env, String schemaName) {
		
		HibernateConfig r = null;
		
		for (HibernateConfig _hc : hibes) {
			if (_hc.env.equals(env)) {
				if (_hc.schemaName == null)  {
					if (schemaName == null) {
						r = _hc;
						break;
					}
				}
				else if (_hc.schemaName.equals(schemaName)) {
					r = _hc;
					break;
				}
					 
			}
		}
		
		return r;
		
	}
	
	
	public HibernateConfig getCurrentConfig() {
		return currentConfig;
	}

	
	private static org.apache.log4j.Logger log = Logger.getLogger(HibernateHelper.class);
	
	
	public class HibernateConfig {
		
		SessionFactory factory;
		Configuration configuration;
		StandardServiceRegistryBuilder builder;
		ServiceRegistry reg;
		private ConnectParameters connParms;

		String env;
		String schemaName;
		
		
		public ConnectParameters getConnParms() {
			return connParms;
		}
		private void setConnParms(ConnectParameters connParms) {
			this.connParms = connParms;
		}
		
		//Session session; 
		
	}
	
	
	
}
