





public class MultiThreadedSocketServer {

    ServerSocket myServerSocket;
    boolean ServerOn = true;
    
    private int portNumber = 7201;

    HashMap<String, String> parameters;
    
    MongoInputCollectionSet micky;
   
    FileDispatcher _fd;
    
    private HibernateHelper h;
	private SessionFactory factory;
	private Session session;
	
	private boolean [] tracker;
    
    
    public MultiThreadedSocketServer(HashMap<String, String> parameters, ServerControl _sc) 
    { 
    	
    	this.parameters = parameters;
    	micky = new MongoInputCollectionSet(parameters);
    	
    	if (_sc.objectName.equals(construction.model.ClaimServLine.class.getName()))
    		_sc.collectionName = micky.getsClaimColl();
    	else if (_sc.objectName.equals(construction.model.ClaimRx.class.getName()))
    		_sc.collectionName = micky.getsRxColl();
    	else if (_sc.objectName.equals(construction.model.PlanMember.class.getName()))
    		_sc.collectionName = micky.getsMemberColl();
    	else if (_sc.objectName.equals(construction.model.Enrollment.class.getName()))
    		_sc.collectionName = micky.getsEnrollColl();
    	else if (_sc.objectName.equals(construction.model.Provider.class.getName()))
    		_sc.collectionName = micky.getsProviderColl();
    	
    	_fd = new FileDispatcher(parameters, _sc);
    	
    	InputMonitor _im = monitorRetrieval(_sc.resourceName);
    	int blockCount = _im.getRcdCount() % _fd.getChunksize() == 0 ? _im.getRcdCount() / _fd.getChunksize() : (_im.getRcdCount() / _fd.getChunksize()) + 1;
    	tracker = new boolean [blockCount];
    	
    	log.info("Constructed server for " + _sc.resourceName + " to create " + _sc.collectionName + " from " + blockCount + " blocks");
    
    }
    
    

    public void process() 
    { 
        try 
        { 
            myServerSocket = new ServerSocket(portNumber); 
        } 
        catch(IOException ioe) 
        { 
            log.error("Could not create server socket on port " + portNumber + ". Quitting."); 
            System.exit(-1); 
        } 




        Calendar now = Calendar.getInstance();
        SimpleDateFormat formatter = new SimpleDateFormat("E yyyy.MM.dd 'at' hh:mm:ss a zzz");
        log.info("It is now : " + formatter.format(now.getTime()));




        // Successfully created Server Socket. Now wait for connections. 
        while(ServerOn) 
        {                        
            try 
            { 
                // Accept incoming connections. 
                Socket clientSocket = myServerSocket.accept(); 

                // accept() will block until a client connects to the server. 
                // If execution reaches this point, then it means that a client 
                // socket has been accepted. 

                // For each client, we will start a service thread to 
                // service the client requests. This is to demonstrate a 
                // Multi-Threaded server. Starting a thread also lets our 
                // MultiThreadedSocketServer accept multiple connections simultaneously. 

                // Start a Service thread 

                ClientServiceThread cliThread = new ClientServiceThread(clientSocket);
                cliThread.start(); 

            } 
            catch(IOException ioe) 
            { 
            	log.error("Exception encountered on accept. Ignoring. Stack Trace :"); 
                ioe.printStackTrace(); 
            } 

        }

        try 
        { 
            myServerSocket.close(); 
            log.info("Server Stopped"); 
        } 
        catch(Exception ioe) 
        { 
        	log.error("Problem stopping server socket"); 
            System.exit(-1); 
        } 



    } 
    
    
    
    /**
	 * precise get of job when uid is known
	 */
	private InputMonitor monitorRetrieval (String source) {
		
		initializeForMonitorRetrieval();
		
		Transaction tx = null;
		InputMonitor _IM = null;
		
		try {
			

			//
			
			tx = session.beginTransaction();
			

			 Query query = session.createQuery("from InputMonitor where jobuid = :jobuid AND filename = :source "); 
			 query.setParameter("jobuid", parameters.get("jobuid")); 
			 query.setParameter("source", source);
			 

			 List<?> list = query.list(); 
			 if (list.isEmpty()) {
				 log.error("Input Monitor not found for " + source + " in jobuid " + parameters.get("jobuid") + ".  Input Analysis may not have been run since file name was set.");
				 throw new IllegalStateException ("Input Monitor not found for " + source + " in jobuid " + parameters.get("jobuid") + ".  Input Analysis may not have been run since file name was set.");
			 }
			 _IM = (InputMonitor)list.get(0); 

			
			tx.commit();
			
		} catch (HibernateException e) {
			if (tx!=null) tx.rollback();
			e.printStackTrace(); 
		} finally {
			// don't close here - called from a loop
		}
		
		return _IM;
		
	}
	
    
	/**
	 * initialize Hibernate to enable input monitor retrieval
	 */
	private void initializeForMonitorRetrieval ()  {
		
		List<Class<?>> cList = new ArrayList<Class<?>>();
		cList.add(InputMonitor.class);
		
		h = new HibernateHelper(cList, "ecr", "ecr");
		factory = h.getFactory("ecr", "ecr");
		
		session = factory.openSession();
	
	}
	
    
    

    public static void main (String[] args) 
    { 
    	
    	HashMap<String, String> parameters = RunParameters.parameters;
		
		
		parameters.put("chunksize", "10");
		parameters.put("configfolder", "C:\\workspace\\ECR_Analytics\\trunk\\EpisodeConstruction\\src\\");
		parameters.put("env", "prd");
		
		/*
		BigKahuna bigKahuna = new BigKahuna();
		String _cf = parameters.get("configfolder");
    	
    	bigKahuna.parameters = parameters;
		
    	bigKahuna.reLoadParameters();
    	
    	// fake stuff for local testing
    	parameters.put("configfolder", _cf);
    	*/

    	
    	parameters.put("jobuid", "1095");
		parameters.put("clientID", "Test");
		parameters.put("runname", "Dec50K");
		parameters.put("rundate", "20151209");
		parameters.put("jobname", "Test_Dec50K20151209");
		parameters.put("mapname", "hci3");

		parameters.put("claim_file1", "C:/input/Test/new_2015_11_05/claims_mod.csv");
		parameters.put("claim_rx_file1", "C:/input/Test/new_2015_11_05/pharmacy_claims.csv");
		parameters.put("enroll_file1", "C:/input/Test/new_2015_11_05/member.csv");
		parameters.put("member_file1", "C:/input/Test/new_2015_11_05/member.csv");
		parameters.put("provider_file1", "C:/input/Test/new_2015_11_05/provider.csv");
    	
		ServerControl _sc = new ServerControl();
		_sc.resourceName = "C:/input/Test/new_2015_11_05/claims_mod.csv";
		_sc.objectName = construction.model.ClaimServLine.class.getName();
        new MultiThreadedSocketServer(parameters, _sc).process();       
    } 
    
    
    
    
    private static org.apache.log4j.Logger log = Logger.getLogger(MultiThreadedSocketServer.class);
    
    


    class ClientServiceThread extends Thread 
    { 
        Socket myClientSocket;
        boolean m_bRunThread = true; 

        public ClientServiceThread() 
        { 
            super(); 
        } 

        ClientServiceThread(Socket s) 
        { 
            myClientSocket = s; 

        } 

        public void run() 
        {            
            // Obtain the input stream and the output stream for the socket 
            // A good practice is to encapsulate them with a BufferedReader 
            // and a PrintWriter as shown below. 
            BufferedReader in = null; 
            PrintWriter out = null; 

            // Print out details of this connection 
            log.info("Accepted Client Address - " + myClientSocket.getInetAddress().getHostName()); 

            try 
            {                                
                in = new BufferedReader(new InputStreamReader(myClientSocket.getInputStream())); 
                out = new PrintWriter(new OutputStreamWriter(myClientSocket.getOutputStream())); 

                // At this point, we can read for input and reply with appropriate output. 

                // Run in a loop until m_bRunThread is set to false 
                String line;
                long repCount = 0L;
                while(m_bRunThread) 
                {                    
                    // read incoming stream 
                    String clientCommand = in.readLine(); 
                    if(repCount % 10000 == 0) {
                    	log.info("Client Says :" + clientCommand + " count: " + repCount);
                    	log.info(getQueueStats());
                    }

                    if(!ServerOn) 
                    { 
                        // Special command. Quit this thread 
                        log.info(ServerControl.MSG_SERVER_STOPPED); 
                        out.println(ServerControl.MSG_SERVER_STOPPED); 
                        out.flush(); 
                        m_bRunThread = false;   

                    } 

                    if(clientCommand == null)
                    	continue;
                    if(clientCommand.equalsIgnoreCase("quit")) { 
                        // Special command. Quit this thread 
                        m_bRunThread = false;   
                        log.info("Stopping client thread for client : "); 
                    } else if(clientCommand.equalsIgnoreCase("end")) { 
                        // Special command. Quit this thread and Stop the Server
                        m_bRunThread = false;   
                        log.info("Stopping client thread for client : "); 
                        ServerOn = false;
                    } else {
                    	// Process it 
                    	//out.println("Server Says : " + clientCommand);
                    	// process feedback from client
                    	if (clientCommand.startsWith("OK")) {
                    		tracker[Integer.parseInt(clientCommand.substring(2))] = true;
                    	}
                    	else if (clientCommand.startsWith("NG")) {
                    		log.error("Client failure on block " + Integer.parseInt(clientCommand.substring(2)));
                    	}
                    	// feed client more data
                    	line = _fd.getNextMapRequest();
                    	if (line == null) {
                    		m_bRunThread = false;   
                            log.info("File done. Stopping client thread for client : ");
                            ServerOn = false;
                    	}
                    	else if (line.contains(MappingRequest.FR_STATUS_FILEEND)) {
                    		out.println(line);
                    		out.flush();
                    		m_bRunThread = false;   
                            log.info("File done. Stopping client thread for client : ");
                            ServerOn = false;
                    	}
                    	else {
                    		out.println(line);
                    		out.flush(); 
                    	}
                    }
                    
                    repCount++;
                    
                } 
            } 
            catch(Exception e) 
            { 
                e.printStackTrace(); 
            } 
            finally 
            { 
                // Clean up 
                try 
                {                    
                    in.close(); 
                    out.close(); 
                    myClientSocket.close(); 
                    log.info("...Stopped"); 
                } 
                catch(IOException ioe) 
                { 
                    ioe.printStackTrace(); 
                } 
            } 
            
           
            
        }

		private String getQueueStats() {
			int _himrk = 0;
			int i = 0;
			int misses = 0;
			for (boolean _b : tracker) {
				if (_b)
					_himrk = i;
				i++;
			}
			i=0;
			for (boolean _b : tracker) {
				if (!_b)
					misses++;
				i++;
				if (i > _himrk )
					break;
			}
			return new StringBuilder().append("Himark: ").append(_himrk).append(" Misses: ").append(misses).toString();
		} 


    } 
    

}

