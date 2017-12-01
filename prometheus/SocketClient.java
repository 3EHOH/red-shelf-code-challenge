




/**
 * A client for our Multi-threaded SocketServer. 
 * @author Warren
 *
 */

public class SocketClient { 
	
	
	private String serverIP = "localhost";
	private int portNumber = 7201;
  
	HashMap<String, String> parameters = new HashMap<String, String>();
	
	
	SocketClient (HashMap<String, String> parameters)  {
		
		this.parameters = parameters;
		if (parameters.containsKey("server"))
			serverIP = parameters.get("server");
	
	}
	
	private void process() {
		
       
        Socket s = null; 
        
        // Create the socket connection to the MultiThreadedSocketServer port 11111 
        try 
        { 
            s = new Socket(serverIP, portNumber); 
        }        
        catch(UnknownHostException uhe) 
        { 
            // Server Host unreachable 
            log.error("Unknown Host :" + serverIP); 
            s = null; 
        } 
        catch(IOException ioe) 
        { 
            // Cannot connect to port on given server host 
            log.error("Cant connect to server " + serverIP + " at " + portNumber + ". Make sure it is running."); 
            s = null; 
        } 
        
        if(s == null) 
            System.exit(-1); 
        
        BufferedReader in = null; 
        PrintWriter out = null;
        String line = null;
        String _ask = "BG";
        
        try 
        { 
            // Create the streams to send and receive information 
            in = new BufferedReader(new InputStreamReader(s.getInputStream())); 
            out = new PrintWriter(new OutputStreamWriter(s.getOutputStream())); 
            
            long repCount = 0L;
            while(true) {
            	// Since this is the client, we will initiate the talking. 
            	// Send a string data and flush 
            	out.println(_ask); 
            	out.flush(); 
            	// Receive the reply. 
            	line = in.readLine();
            	if(repCount % 10000 == 0)
            		log.info("Rep: " + repCount + " | " + line ); 
            	
            	if (line == null) {
            		// Send the special string to tell server to quit. 
            		out.println("Quit"); 
            		out.flush(); 
            		break;
            	}
            	
            	if (line.equals(ServerControl.MSG_SERVER_STOPPED)) {
            		continue;
            	}
            	
            	_ask = doMapping(line);
            	
            	repCount++;
            }
        } 
        catch(IOException ioe) 
        { 
            System.out.println("Exception during communication. Server probably closed connection."); 
        } 
        finally 
        { 
            try 
            { 
                // Close the input and output streams 
                out.close(); 
                in.close(); 
                // Close the socket before quitting 
                s.close(); 
            } 
            catch(Exception e) 
            { 
                e.printStackTrace(); 
            }                
        }        
	}
	
	
	private String doMapping (String line)  {
		
		ResponseConsumer _r = new ResponseConsumer();
		
		Integer _seq = new Integer(-1);
		boolean _ok = true;
		
		try {
			
			MappingRequest _mr = _r.parseResponse(line);
			_seq = _mr.getSequence();
		
			if (! (_mr.equals(priorMR)) ) {
				priorMR = _mr;
				RunParameters.parameters = parameters;
				_mc = new MappingController(parameters, _mr);
			}
		
			_mc.processMapRequest(_mr);
			
		}
		catch (Throwable e) {
			_ok = false;
			log.error("Failure trying to process server string: " + line + " : " + e);
			e.printStackTrace();
		}
		

		return _ok ? "OK" + String.format("%01d", _seq) : "NG" + String.format("%01d", _seq);
		
	}
	
	MappingRequest priorMR;
	MappingController _mc;
	
	private static String OS = System.getProperty("os.name").toLowerCase();
	
    public static void main(String[] args)
    { 
    	HashMap<String, String> parameters = new HashMap<String, String>();
    	
    	
    	if (OS.indexOf("nix") >= 0 || OS.indexOf("nux") >= 0 || OS.indexOf("aix") > 0) {
			parameters.put("configfolder", "/ecrfiles/scripts/");
		} else if (OS.indexOf("win") >= 0)
			parameters.put("configfolder", "C:\\workspace\\ECR_Analytics\\trunk\\EpisodeConstruction\\src\\");
    	
    	for (int i = 0; i < args.length; i++) {
			parameters.put(args[i].substring(0, args[i].indexOf('=')), args[i].substring(args[i].indexOf('=')+1)) ;
		}
    	
    	SocketClient instance = new SocketClient(parameters);
    	instance.process();
       
    } 
    
    
    
    private static org.apache.log4j.Logger log = Logger.getLogger(SocketClient.class);
    
    
}

