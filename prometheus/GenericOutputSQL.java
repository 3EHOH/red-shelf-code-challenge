



public class GenericOutputSQL implements GenericOutputInterface {

	
	ConnectParameters connParms;
	
	HashMap<String, String> parameters;
	
    
	public GenericOutputSQL () {
		this (RunParameters.parameters);
	}
	
	public GenericOutputSQL (HashMap<String, String> parameters) {
		this.parameters = parameters;
	}
 	
	
	
	@Override
	public String write(Object o) {
		
		
		
		// first time thru do file name, headers, etc.
		if (!bInitialized)
			initialize(o);
		
		
		Field[] f =  o.getClass().getDeclaredFields();
		Object object = o;
		Object value;
		
		for (int i=0; i < f.length; i++) {
			
			//log.info("help" + f[i].getName());
			
			try {
				if (f[i].getName().equals("serialVersionUID"))		// don't bother with any serialization UID's
					continue;
				if (f[i].getName().equals("errMgr"))				// or the error manager
					continue;
				if (Modifier.isTransient(f[i].getModifiers()))
					continue;
				if (Modifier.isStatic(f[i].getModifiers()))
					continue;
				
				f[i].setAccessible(true);
				value = f[i].get(object);
				
				
				Integer parameterIndex = columns.get(f[i].getName());
				
				if (value == null) {
					stmt.setNull(parameterIndex, java.sql.Types.NULL);
					continue;
				}
				
				if (f[i].getType().equals(String.class)) {
					stmt.setString(parameterIndex, ((String) value));
					//sbVals.append("'").append( ((String) value).replace("'", "''").replace('\\', '/') ).append("'").append(sDelimiter);
				}
				else if (f[i].getType().getName().equals("char")) {
					stmt.setString(parameterIndex, ((Character) value).toString());
				}
				else if (f[i].getType().equals(Integer.class)  ||  f[i].getType().equals(int.class)) {
					stmt.setInt(parameterIndex, ((Integer) value));
				}
				else if (f[i].getType().equals(Double.class)  ||  f[i].getType().equals(double.class)) {
					stmt.setDouble(parameterIndex, (Double) value);
				}
				else if (f[i].getType().equals(Long.class)  ||  f[i].getType().equals(long.class)) {
					stmt.setLong(parameterIndex, (Long) value);
				}
				else if (f[i].getType().equals(BigDecimal.class)) {
					stmt.setBigDecimal(parameterIndex, (BigDecimal) value);
				}
				else if (f[i].getType().equals(Boolean.class)  ||  f[i].getType().equals(boolean.class)) {
					stmt.setBoolean(parameterIndex, (Boolean) value);
				}
				else if (f[i].getType().equals(Date.class)) {
		            sqlDate.setTime(((Date) value).getTime());
					stmt.setDate(parameterIndex, sqlDate);
				}
				else if (f[i].getType().equals(List.class)  ||  f[i].getType().equals(ArrayList.class)) {
					stmt.setString(parameterIndex, doListField(f[i], o));
				}
				else if (f[i].getType().equals(HashMap.class)) {
					doHashMapField(f[i], o);
				}
				else {
					stmt.setString(parameterIndex, ((String) value));
				}
			} 
			catch (IllegalArgumentException | IllegalAccessException e) {
				// do nothing
			} catch (SQLException e) {
				log.error("Got an exception preparing insert " + stmt.toString());
				log.error(e.getMessage(), e);
				log.error(e.getLocalizedMessage());
				log.error("SQL State: " + e.getSQLState());
			}
		}
		
		doBatchInsert();
		
		return "";
		
	}
	
	
	private  java.sql.Date sqlDate = new java.sql.Date(1L);

	
	
	private void doHashMapField(Field field, Object o) {
		// TODO Auto-generated method stub
	}


	protected String doListField (Field f, Object o) throws IllegalArgumentException, IllegalAccessException {
		sbV.setLength(0);
		sbV.append('{');
		for(Object oList:(List<?>) f.get(o)){
			sbV.append(oList).append("; ");
		}
		if ( ((List<?>) f.get(o)).size() > 0)
			sbV.setLength(sbV.length()-2);
		sbV.append('}');
		if (sbV.length() > 1500)
			log.error("Found list item > 1500 characters long: " + sbV);
		return sbV.toString();
	}
	
	private StringBuffer sbV = new StringBuffer();
	

	private void doBatchInsert () {
		
		//getConnection();
		
		try {
			iBatchIx++;
			stmt.addBatch();
			if(iBatchIx % 10000 == 0) { 
				stmt.executeBatch();
				log.info("executing " + iBatchIx + " ==> " + new Date());
			}
		}
		catch (Exception e)
		{
			log.error("Got an exception populating table " + sTableName);
			log.error(e.getMessage(), e);
			if (conn != null)
				try {
					conn.close();
				} catch (SQLException e1) {
					log.error("Got an exception closing connection during population");
					e1.printStackTrace();
				}
				throw new IllegalStateException(e);
		}
		
		//log.info(sb);
		
		
	}
	
	
	
	
	private void initialize(Object o) {
		
		log.info("trying to initialize for " + o.getClass().getSimpleName());
		
		// define the output table
		Date date = Calendar.getInstance().getTime();
		DateFormat formatter = new SimpleDateFormat("yyyy_MM_dd_hhmm");
		today = formatter.format(date);
		sTableName = parameters.get("runname") == null ?  sDefaultRunName 
				+ o.getClass().getName().substring(o.getClass().getName().lastIndexOf(".") + 1) + today : 
			parameters.get("runname").replace(' ', '_') 
				+ o.getClass().getName().substring(o.getClass().getName().lastIndexOf(".") + 1) + today;
		
		// look for a getPrimaryKey method in the object
		try {
			log.info("looking for key");
			mKey = o.getClass().getDeclaredMethod("getPrimaryKey");
			log.info("got key");
		} catch (NoSuchMethodException e2) {
			log.info("no key");
		} catch (NoSuchMethodError ee) {
			log.info("no key error caught");
		} catch (SecurityException e2) {
			// nothing to do
		}
				 
		String env = parameters.get("env") == null ?  "lcl" :  parameters.get("env");

		connParms = new ConnectParameters(env);
		connParms.setSchema(parameters.get("jobname"));

		log.info("Initializing " + o.getClass().getName().substring(o.getClass().getName().lastIndexOf('.') + 1) + " ==> " + new Date());
		log.info(">>>  Connection information - U: " + connParms.getDbUrl() + "; S: " + connParms.getSchema() + "; I: " + connParms.getDbUser() + " P:" + "xxxxxxxx"); 
	    
		try
		{
			
			ConnectionHelper.getConnection(connParms);
			
					
			Statement create = conn.createStatement();
			
			// write out column header 
			Field[] f =  o.getClass().getDeclaredFields();
			StringBuffer sbCreate = new StringBuffer();
			sbCreate.append("CREATE TABLE ").append(connParms.getSchema()).append('.').append(sTableName).append(" (");
			StringBuffer sbInsert = new StringBuffer();
			sbInsert.append("INSERT INTO ").append(connParms.getSchema()).append('.').append(sTableName).append(" (");
			
			int colIdx = 1;

			for (int i=0; i < f.length; i++) {
				
				if (f[i].getName().equals("serialVersionUID"))		// don't bother with any serialization UID's
					continue;
				if (f[i].getName().equals("errMgr"))				// or the error manager object
					continue;
				if (Modifier.isTransient(f[i].getModifiers()))
					continue;
				if (Modifier.isStatic(f[i].getModifiers()))
					continue;
				
				columns.put(f[i].getName(), colIdx);
				colIdx++;
				
				sbInsert.append(f[i].getName()).append(',');
				
				// add a data type
				if (f[i].getType().equals(String.class)) {
					sbCreate.append(f[i].getName()).append(' ');				// add field name
					sbCreate.append("VARCHAR(100) NULL ");
				}
				else if (f[i].getType().getName().equals("char")) {
					sbCreate.append(f[i].getName()).append(' ');				// add field name
					sbCreate.append("VARCHAR(1) NULL ");
				}
				else if (f[i].getType().equals(Integer.class)  ||  f[i].getType().equals(int.class)) {
					sbCreate.append(f[i].getName()).append(' ');				// add field name
					sbCreate.append("MEDIUMINT NULL ");
				}
				else if (f[i].getType().equals(Double.class)  ||  f[i].getType().equals(double.class)) {
					sbCreate.append(f[i].getName()).append(' ');				// add field name
					sbCreate.append("FLOAT NULL ");
				}
				else if (f[i].getType().equals(BigDecimal.class)) {
					sbCreate.append(f[i].getName()).append(' ');				// add field name
					sbCreate.append("DECIMAL NULL ");
				}
				else if (f[i].getType().equals(Long.class)  ||  f[i].getType().equals(long.class)) {
					sbCreate.append(f[i].getName()).append(' ');				// add field name
					sbCreate.append("BIGINT NULL ");
				}
				else if (f[i].getType().equals(Boolean.class)  ||  f[i].getType().equals(boolean.class)) {
					sbCreate.append(f[i].getName()).append(' ');				// add field name
					sbCreate.append("TINYINT NULL ");
				}
				else if (f[i].getType().equals(Date.class)) {
					sbCreate.append(f[i].getName()).append(' ');				// add field name
					sbCreate.append("DATETIME NULL ");
				}
				else if (f[i].getType().equals(List.class)  ||  f[i].getType().equals(ArrayList.class)) {
					doListHeader(f[i], sbCreate);
				}	
				else if (f[i].getType().equals(HashMap.class)) {
					doHashMapHeader(f[i], sbCreate);
				}
				else {
					sbCreate.append(f[i].getName()).append(' ');				// add field name
					sbCreate.append("VARCHAR(1000) NULL ");
				}
				
				sbCreate.append(checkForPrimaryKey(o, f[i]));
				sbCreate.append(", ");

			}
			sbCreate.setLength(sbCreate.length()-2);
			sbCreate.append(") ENGINE=INNODB");
			
			create.execute(sbCreate.toString());
			
			// finish of prepared insert statement
			sbInsert.setLength(sbInsert.length() - 1);
			sbInsert.append(") VALUES (");
			for (int x=0; x<columns.size(); x++) {
				sbInsert.append("?,");
			}
			sbInsert.setLength(sbInsert.length() - 1);
			sbInsert.append(")");
			stmt = conn.prepareStatement(sbInsert.toString());

		}
		catch (SQLException e)
		{
			log.error("Got an exception creating table " + sTableName);
			log.error(e.getMessage(), e);
			log.error(e.getLocalizedMessage());
			log.error("SQL State: " + e.getSQLState());
			if (conn != null)
				try {
					conn.close();
				} catch (SQLException e1) {
					log.error("Got an exception closing connection ");
					e1.printStackTrace();
				}
			throw new IllegalStateException(e);
		}
		
		
		bInitialized = true;
		
	}
	
	private HashMap<String, Integer> columns = new HashMap<String, Integer>();
	
	
	Method mKey;
	
	private String checkForPrimaryKey(Object obj, Field f) {
		if(mKey == null)
			return "";
		try {
			if ( ((String) mKey.invoke(obj)).equalsIgnoreCase(f.getName()))
				return " PRIMARY KEY ";
			else
				return "";
		} catch (SecurityException e) {
			return "";
		} catch (IllegalAccessException e) {
			return "";
		} catch (IllegalArgumentException e) {
			return "";
		} catch (InvocationTargetException e) {
			return "";
		} catch (NoSuchMethodError e) {
			return "";
		}
	}


	private void doHashMapHeader(Field field, StringBuffer sb) {
		// TODO Auto-generated method stub
		
	}


	void doListHeader (Field f, StringBuffer sb) {
		sb.append(f.getName()).append(' ');				// add field name
		sb.append("VARCHAR(1500) NULL ");
	}

	boolean bInitialized = false;
	FileWriter outputFile = null;
	BufferedWriter bwOut;
	String sDefaultRunName = "Test";
	String sDelimiter = ",";
	String today;
	String sTableName;
	int iBatchIx = 0;
	PreparedStatement stmt;
	
	
	Connection conn = null;

	
	private static org.apache.log4j.Logger log = Logger.getLogger(GenericOutputSQL.class);

	@Override
	public void close() {
		try {
			log.info("Closing " + sTableName);
			//getConnection();
			stmt.executeBatch();
		} catch (SQLException e) {
			log.error("An error occurred while closing " + sTableName);
			e.printStackTrace();
		} 
		finally
		{
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				log.error("Got an exception closing connection ");
				log.error(e.getMessage(), e);
			}
		}
		
	}


}
