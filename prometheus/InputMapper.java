






public class InputMapper extends CommonInputDataAbstract {
	
	
	
	private Object workObject;

	private String objType;
	
	private ConcurrentHashMap<String, InputMapEntry> fieldMappings;
	//private HashMap<String, ArrayList<String>> finalFieldMap;
	private HashMap<String, ArrayList<InputMapEntry>> finalMethodMap = new HashMap<String, ArrayList<InputMapEntry>>();
	//private ArrayList<String> workList;
	//private HashMap<String, InputMapEntry> finalConditionMap;
	
	private HashMap<String, String> parameters;

	
	public InputMapper (String objType, HashMap<String, String> parameters) {
		

		this.objType = objType;
		this.parameters = parameters;
		oi = new GenericOutputMongo(parameters);
		
		log.info(this.parameters.get("client"));
		
	}
	
	

	public InputMapper (MappingRequest _mr, HashMap<String, String> parameters) {
		

		this.objType = _mr.getObjectName().substring(_mr.getObjectName().lastIndexOf('.') + 1);
		this.parameters = parameters;
		oi = new GenericOutputMongo(_mr.getCollectionName(), parameters);
		
		//log.info(this.parameters.get("client"));
		
	}
	


	private GenericOutputInterface oi;
	

	
	
	public void prepareAllWorkObjects (InputObjectMap objMap) {
		
		if (sourceMonitors.isEmpty())
			throw new IllegalStateException("No source monitors - must invoke addSourceMonitor prior to invoking this method");
		
		if (objMap == null)
			throw new IllegalStateException("You must supply an object map to this method");
		
		this.fieldMappings = objMap.getFieldMappings();
		
		
		// get each file in the source file list
		for (InputMonitor monitor : sourceMonitors) {
			
			String sFile = monitor.getFilename();
			int total_to_process = monitor.getRcdCount();
			super.setDelimiter( monitor.getDelimiter() );
			col_index = monitor.getCol_index();
			col_map = new HashMap<String, Integer>();
			
			for (ColumnFinder cf : col_index.values()) {
			    col_map.put(cf.col_name.toUpperCase(), cf.col_number);
			}
			
			// prepare mapping for all source columns for a particular field
			InputMapEntry ime;
			//String key;
			//finalFieldMap = new HashMap<String, ArrayList<String>>();
			//finalMethodMap = new HashMap<String, ArrayList<String>>();
			
			for (Entry<String, InputMapEntry> entry : fieldMappings.entrySet()) {
				//key = entry.getKey();
				ime = entry.getValue();
				for (ColumnFinder cf : col_index.values()) {
					for (SourceAttributes sSrc : ime.getSourceField()) {
						if (cf.col_name.matches( "(?i)" + sSrc.getSourceFieldName() ) ) {
							//workList = finalFieldMap.get(key);
							//if (workList == null)
							//	workList = new ArrayList<String>();
							//workList.add(cf.col_name);
							//finalFieldMap.put(key, workList);
							sSrc.setCf(cf);
						}
					}
				}
				
				for (SourceAttributes sSrc : ime.getSourceField()) {
					if (sSrc.getMapMethod() != null) {
						ArrayList<InputMapEntry> imeArray = finalMethodMap.get(sSrc.getMapMethod());
						if (imeArray == null) {
							imeArray = new ArrayList<InputMapEntry>();
							imeArray.add(ime);
							finalMethodMap.put(sSrc.getMapMethod().getName(), imeArray);
						}
						else if (! imeArray.contains(ime))
							imeArray.add(ime);
					}
				}
				/*
				if (ime.getSourceField().) {
					finalMethodMap.put(ime.getMapMethod().getName(), ime.getSourceField());
				}
				/*
				if (ime.get_condition() != null) {
					finalConditionMap.put(ime.getSourceField(), ime);
				}
				*/
			}

			InputManager inputFile = new InputManager(sFile);
			try {
				inputFile.openFile();
			} catch (IOException | ZipException e) {
				log.error("An error occurred while opening " + sFile + " - " + e);
			}
			
			String line;//, col_value;
			//int col_ix = 0;
			boolean bStart = false;
			int processed_count = 0;
			//col_index = new HashMap<Integer, ColumnFinder>();
			DateUtil.resetFieldFormatLookup();
			
			// read the lines from each file
			try {
				while ((line = inputFile.readFile()) != null)  {
					
					// check for a header in the first line  
					if (!bStart)
						bStart = checkForHeader(line);
					
					// map all data lines
					if (bStart) {
						
						mapDataLine(line, objMap);
					}
					
					else
						bStart = true;
					
					processed_count++;
					if(processed_count % 10000 == 0) { 
						log.info("Processing record " + processed_count + " of  "  + total_to_process + " at " + new Date());
					}
					
				}
				
				inputFile.closeFile();
				
			} catch (IOException e) {
				throw new IllegalStateException("An error occurred while reading " + sFile);
			}  
			
		}
		
		log.info("Completed load of all" + objType);
		
	}
	

	protected Object initializeWorkObject(String[] in) {
		return getNewInstance();
	}
	
	protected void storeWorkObject () {
		oi.write(workObject);
	}
	
	protected boolean isValid (ErrorManager errMgr) {
		return true;
	}
	
	
	
	/**
	 * mapper driven by mapping requests (socket client)
	 * @param objMap
	 * @param _mr
	 */
	public void prepareOneWorkObject(InputObjectMap objMap, MappingRequest _mr) {
		
		if (sourceMonitors.isEmpty())
			throw new IllegalStateException("No source monitors - must invoke addSourceMonitor prior to invoking this method");
		

		if (objMap == null)
			throw new IllegalStateException("You must supply an object map to this method");
		
		
		this.fieldMappings = objMap.getFieldMappings();
		
		
		// get each file in the source file list
		for (InputMonitor monitor : sourceMonitors) {
			
			super.setDelimiter( monitor.getDelimiter() );
			col_index = monitor.getCol_index();
			col_map = new HashMap<String, Integer>();
			
			for (ColumnFinder cf : col_index.values()) {
			    col_map.put(cf.col_name.toUpperCase(), cf.col_number);
			}
			
			// prepare mapping for all source columns for a particular field
			InputMapEntry ime;
			//String key;
			//finalFieldMap = new HashMap<String, ArrayList<String>>();
			//finalMethodMap = new HashMap<String, ArrayList<String>>();
			
			for (Entry<String, InputMapEntry> entry : fieldMappings.entrySet()) {
				//key = entry.getKey();
				ime = entry.getValue();
				for (ColumnFinder cf : col_index.values()) {
					for (SourceAttributes sSrc : ime.getSourceField()) {
						if (cf.col_name.matches( "(?i)" + sSrc.getSourceFieldName() ) ) {
							//workList = finalFieldMap.get(key);
							//if (workList == null)
							//	workList = new ArrayList<String>();
							//workList.add(cf.col_name);
							//finalFieldMap.put(key, workList);
							sSrc.setCf(cf);
						}
					}
				}
				
				for (SourceAttributes sSrc : ime.getSourceField()) {
					if (sSrc.getMapMethod() != null) {
						ArrayList<InputMapEntry> imeArray = finalMethodMap.get(sSrc.getMapMethod());
						if (imeArray == null) {
							imeArray = new ArrayList<InputMapEntry>();
							imeArray.add(ime);
							finalMethodMap.put(sSrc.getMapMethod().getName(), imeArray);
						}
						else if (! imeArray.contains(ime))
							imeArray.add(ime);
					}
				}
				/*
				if (ime.getSourceField().) {
					finalMethodMap.put(ime.getMapMethod().getName(), ime.getSourceField());
				}
				/*
				if (ime.get_condition() != null) {
					finalConditionMap.put(ime.getSourceField(), ime);
				}
				*/
			}



			//col_index = new HashMap<Integer, ColumnFinder>();
			DateUtil.resetFieldFormatLookup();
			
			// read the lines from the mapping request

			for (String line : _mr.getInput()) {
				
				mapDataLine(line, objMap);
			
			}
				
			
			
		}
		
		//log.info("Completed load of all" + objType);
		
	}
	
	
	
	
	private boolean mapDataLine (String line, InputObjectMap objMap) {
		
		boolean bAccept = false;
		
		//counts.providerRecordsRead++;
		//in = line.split(sDelimiter);
		in = this.splitter(line);
		
		// drop any records that meet exclusion criteria
		if (doExclusions(objMap))
			return false;
		
		workObject = initializeWorkObject(in);
		
		// map each column to the provider object
		bAccept = true;
		//col_ix = 0;
		
		bAccept  = doMap()  && bAccept;
		
		/*
		for (String col_value : in)  
		{  
			if (col_value != null)
				col_value = col_value.trim();
		    bAccept =  doMap(col_ix, col_value)  &&  bAccept;
		    col_ix++;
		} 
		*/
		
		
		if (errMgr != null)
			bAccept = bAccept && isValid(errMgr);
		
		if (bAccept)
		{
			doPostProcess();
			storeWorkObject();
			//counts.providerRecordsAccepted++;
		}
		
		return bAccept;
		
	}
	
	

	protected boolean doMap() {	
		
		
		Field[] f =  workObject.getClass().getDeclaredFields();
		
		for (int i=0; i < f.length; i++) {
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
				
				fld = f[i].getName();
				
				InputMapEntry workEntry = fieldMappings.get(fld);
				if (workEntry == null)
					continue;
				
				//workList = finalFieldMap.get(fld);
				//if (workList == null)
				//	continue;
				
				// set source value
				workValue.setLength(0);
				for (SourceAttributes _sa : workEntry.getSourceField()) {
					if(workValue.length() > 0 && !_sa.isConcatenate()) {
						continue;
					}
					if(_sa.get_conditions() == null) {}
					else if( !checkConditions (_sa.get_conditions()) ) {
						continue;
					}
					String s =  _sa.getSourceFieldName();
					if (s.toLowerCase().startsWith("literal"))
						workValue.append(s.substring(8));  // everything past "literal="
					else {
						Integer ix = col_map.get(s.toUpperCase());
						if (ix == null)
							continue;
						if (ix > in.length)
							continue;
						// if we don't need to trim or expand the incoming value, just move
						if(_sa.get_length() == null) {
							workValue.append(in [ix]);
						}
						// otherwise, figure out if it's short or long, and fill or trim
						else {
							if(in [ix].length() < _sa.get_length()) {
								workValue.append(in [ix]).append(String.format("%0" + (_sa.get_length() - in [ix].length())  + "d", 0).replace('0', ' '));
							}
							else {
								workValue.append(in [ix].substring(0, _sa.get_length()));
							}
						}
					}
				}
				
				if (f[i].getType().equals(String.class)) {
					f[i].set(workObject, workValue.toString().trim());
				}
				else if (f[i].getType().getName().equals("char")) {
					if(workValue == null  || workValue.length() == 0)
					{}
					else
						f[i].set(workObject, workValue.toString().trim().charAt(0));
				}
				else if (f[i].getType().equals(Integer.class)  ||  f[i].getType().equals(int.class)) {
					f[i].set(workObject, Integer.parseInt(workValue.toString().trim()));
				}
				else if (f[i].getType().equals(Double.class)  ||  f[i].getType().equals(double.class)) {
					f[i].set(workObject, Double.parseDouble(workValue.toString().trim().replace("$", "").replace(",", "")));
				}
				else if (f[i].getType().equals(Long.class)  ||  f[i].getType().equals(long.class)) {
					f[i].set(workObject, Long.parseLong(workValue.toString().trim()));
				}
				else if (f[i].getType().equals(BigDecimal.class)) {
					f[i].set(workObject, new BigDecimal(workValue.toString().trim().replace("$", "").replace(",", "")));
				}
				else if (f[i].getType().equals(Boolean.class)  ||  f[i].getType().equals(boolean.class)) {
					f[i].set(workObject, workValue.toString().trim().equalsIgnoreCase("true") ? true : false);
				}
				else if (f[i].getType().equals(Date.class)) {
					//f[i].set(object, new Date((String)doc.get(fld)));
					if (workValue.toString().trim().equals("0"))
						continue;
					if (workValue.toString().trim().equals("-1"))
						continue;
					try {
						f[i].set(workObject, DateUtil.doParse(fld, workValue.toString().trim()));
					} catch (ParseException e) {
						log.error("Problem parsing " + workValue.toString().trim() + " into " + fld);
					}
					catch (NullPointerException e) {
						log.error("Problem parsing " + workValue.toString().trim() + " into " + fld + " - likely cause is unknown date format");
					}
				}
				
				else if (f[i].getType().equals(List.class)  ||  f[i].getType().equals(ArrayList.class)) {
					@SuppressWarnings("unchecked")
					List<String> _al = (List<String>) f[i].get(workObject);
					_al.add(workValue.toString().trim());
				}
				/*
				else if (f[i].getType().equals(HashMap.class)) {
					//doHashMapField(f[i], o);
				}
				*/
				else {
					f[i].set(workObject, workValue.toString().trim());
				}
				
				
				
			} 
			catch (IllegalArgumentException e) {
				// do nothing
			} 
			catch (IllegalAccessException e) {
				// do nothing
			} 
			catch (java.lang.ArrayIndexOutOfBoundsException e) {
				if (!bLogHdrError) {
					bLogHdrError = true;
					log.warn("Probable mismatch on column headers - header has more columns than data");
				}
			}
		}
		
		
		for (Method m : workObject.getClass().getDeclaredMethods()) {
			
			//Type [] tt = m.getGenericParameterTypes();
			
			if (m.getGenericParameterTypes() == null  ||  m.getGenericParameterTypes().length == 0)
				continue;
			
			if (!(m.getGenericParameterTypes()[0] == String.class))
				continue;
			
			mth = m.getName();
			
			//workList = finalMethodMap.get(mth);
			//if (workList == null)
			//	continue;
			
			ArrayList<InputMapEntry> workEntry = finalMethodMap.get(mth);
			if (workEntry == null)
				continue;
			
			
				
			Integer ix; 
				
			for (ColumnFinder cf : col_index.values()) {
				for (InputMapEntry ime : workEntry) {
				
					for (SourceAttributes sSrc : ime.getSourceField()) {
						if (cf.col_name.matches( "(?i)" + sSrc.getSourceFieldName() ) ) {
							
							if(sSrc.get_conditions() == null) {}
							else if( !checkConditions (sSrc.get_conditions()) ) {
								continue;
							}
						
							ix = cf.col_number;
						
							try {
								m.invoke(workObject, in [ix]);
							} catch (IllegalAccessException | IllegalArgumentException
									| InvocationTargetException e) {
								log.error("Illegal mapping invocation.  Field name: " + sSrc.getSourceFieldName() + " Method: " + m.getName() );
								e.printStackTrace();
								throw new IllegalStateException("Illegal mapping invocation.  Field name: " + sSrc.getSourceFieldName() + " Method: " + m.getName() );
							}
						
						}
					}
				}
			}	
				
			
			
			
		}
		
		
		return true;
		
		
	}
	
	
	private boolean doExclusions (InputObjectMap objMap)  {
		
		boolean bR = false;
		
		for (Condition _c : objMap.getExclusions()) {
			bR = bR || checkCondition (_c);
		}
		
		return bR;
	}
	
	
	private boolean bLogHdrError = false;
	
	
	private boolean checkConditions(ArrayList<Condition> conditions) {
		
		boolean bR = false;
		
		for (Condition condition : conditions) {
		
			bR = checkCondition(condition);
			
			if (!bR)
				break;
			
		}
		
		return bR;
	}


	private boolean checkCondition(Condition condition) {
		
		boolean bR = false;
		
		String op1, op2, oper;
		
		oper = condition.operator;
		
		if(  (op1 = getColumnValue(condition.operand1)).length() == 0) {
			op1 = condition.operand1;
		}
		if(  (op2 = getColumnValue(condition.operand2)).length() == 0) {
			op2 = condition.operand2;
		}
		
		switch (oper) {
		case "=":
			bR = op1.equals(op2);
			break;
		case "!=":
			bR = ! op1.equals(op2);
		}
		
		return bR;
		
	}


	//private InputMapEntry workEntry;
	private String fld, mth;
	private StringBuffer workValue = new StringBuffer();
	
	
	
	
	/**
	 * check for a header record
	 * return true indicates first line is data (not a header)
	 * @param line - first line of the data file
	 * @return - true means
	 */
	protected boolean checkForHeader (String line) {
		
		boolean bIsHeader = false;
		
		in = splitter(line);
		
		for (String s : in) {
			for (ColumnFinder finder : col_index.values()) {
			    if (finder.col_name.equalsIgnoreCase(s)) {
			    	bIsHeader = true;
			    	break;
			    }
			}
			if (bIsHeader)
				break;
		}
		
		return !bIsHeader;
	}
	
	

	protected void doPostProcess() {
		
		switch (objType) {
		case "ClaimServLine":
			if ( ((ClaimServLine)workObject).getClaim_line_type_code() == null)
				break;
			if ( ((ClaimServLine)workObject).getClaim_line_type_code().equals("IP")) {
				if (((ClaimServLine)workObject).getAdmission_date() == null  &&
						((ClaimServLine)workObject).getBegin_date() != null)
					((ClaimServLine)workObject).setAdmission_date(((ClaimServLine)workObject).getBegin_date());
				if (((ClaimServLine)workObject).getDischarge_date() == null  &&
						((ClaimServLine)workObject).getEnd_date() != null)
					((ClaimServLine)workObject).setDischarge_date(((ClaimServLine)workObject).getEnd_date());
			}
			break;
		default:
			break;
		}
		
	}	




	private Object getNewInstance () {
		Object o = null;
		try {
			o = MappingController.getClassNameToClass().get(objType).newInstance();
		    //ctor.setAccessible(true);
	 	   // o = ctor.newInstance();
		} catch (InstantiationException x) {
		    x.printStackTrace();
	 	} catch (IllegalAccessException x) {
		    x.printStackTrace();
		}
		return o;
	}
	
	
	
	
	private static org.apache.log4j.Logger log = Logger.getLogger(InputMapper.class);


	
	
	
	

}
