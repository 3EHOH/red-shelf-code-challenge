








public abstract class ProviderDataAbstract extends CommonInputDataAbstract {
	
	protected Map<String, Provider> providers = new HashMap<String, Provider>();
	protected Provider provider;
	protected ProviderInputCounts counts = new ProviderInputCounts();
	

	public Map<String, Provider> getAllProviders() {
		if (providers.isEmpty())
			prepareAllProviders();
		return providers;
	}
	
	
	protected void prepareAllProviders () {
		
		if (sourceFiles.isEmpty())
			throw new IllegalStateException("No source files - must invoke addSourceFile prior to invoking this method");
		
		// get each file in the source file list
		for (String sFile : sourceFiles) {

			InputManager inputFile = new InputManager(sFile);
			try {
				inputFile.setPreserveQuotes(bPreserveQuotes);
				inputFile.openFile();
			} catch (IOException | ZipException e) {
				log.error("An error occurred while opening " + sFile + " - " + e);
			}
			
			String line;//, col_value;
			int col_ix = 0;
			boolean bStart = false;
			boolean bAcceptThisProvider = false;
			col_index = new HashMap<Integer, ColumnFinder>();
			
			// read the lines from each file
			try {
				while ((line = inputFile.readFile()) != null)  {
					// check for a header in the first line  
					if (!bStart) {
						findDelimiter(line);
						bStart = checkForHeader(line);
					}
					// map all data lines
					if (bStart) {
						counts.providerRecordsRead++;
						//in = line.split(sDelimiter);
						in = splitter(line);
						col_ix = 0;
						provider = initializeProvider(in);
						// map each column to the provider object
						bAcceptThisProvider = true;
						for (String col_value : in)  
						{  
						    bAcceptThisProvider = bAcceptThisProvider && doMap(col_ix, col_value);
						    col_ix++;
						} 
						if (errMgr != null)
							bAcceptThisProvider = bAcceptThisProvider && provider.isValid(errMgr);
						if (bAcceptThisProvider)
						{
							//providers.put(provider.getProvider_id(), provider);
							storeProvider();
							counts.providerRecordsAccepted++;
						}
						else
							counts.providerRecordsRejected++;
					}
					else
						bStart = true;
				}
				inputFile.closeFile();
			} catch (IOException e) {
				throw new IllegalStateException("An error occurred while reading " + sFile);
			}  
			
		}
		
		log.info("Completed load of all providers");
		
	}
	
	protected boolean bPreserveQuotes = true;
	

	protected Provider initializeProvider(String[] in) {
		return new Provider();
	}
	
	protected void storeProvider () {
		if (providers.isEmpty())  {
			if (provider.getNPI() == null  ||  provider.getNPI().isEmpty()) {
				bUseNPI = false;
			}
		}
		if (bUseNPI)
			providers.put(provider.getNPI(), provider);
		else
			providers.put(provider.getProvider_id(), provider);
	}
	
	private boolean bUseNPI = true;


	protected boolean doMap(int col_ix, String col_value) {
		throw new IllegalStateException("always override this method");
	}


	protected boolean checkForHeader(String line) {
		throw new IllegalStateException("always override this method");
	}


	

	
	static boolean bDebug = false;
	
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	
	private static org.apache.log4j.Logger log = Logger.getLogger(ProviderDataAbstract.class);
	
	
	

	public ProviderInputCounts getCounts() {
		return counts;
	}
	
	public List<String> getErrorMsgs() {
		return errors;
	}


	public InputStatistics getInputStatistics() {
		return stats;
	}
	



}
