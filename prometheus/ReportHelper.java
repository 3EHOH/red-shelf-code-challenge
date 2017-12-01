


public class ReportHelper {
	
	

		
	    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
	    ArrayList<String> names = new ArrayList<String>();;


	        String jarFileName;
	        JarFile jf ;
	        Enumeration<JarEntry> jarEntries;
	        String entryName;

	        // build jar file name, then loop through zipped entries
	        jarFileName = jarFileName.substring(5,jarFileName.indexOf("!"));
	        System.out.println(">"+jarFileName);
	        jf = new JarFile(jarFileName);
	        jarEntries = jf.entries();
	        while(jarEntries.hasMoreElements()){
	            entryName = jarEntries.nextElement().getName();
	                names.add(entryName);
	            }
	        }
	        jf.close();

	    // loop through files in classpath
	    }else{
	    
	    	File folder = new File(uri.getPath());
	        // won't work with path which contains blank (%20)
	        File[] contenuti = folder.listFiles();
	        String entryName;
	        for(File actual: contenuti){
	            entryName = actual.getName();
	            entryName = entryName.substring(0, entryName.lastIndexOf('.'));
	            names.add(entryName);
	        }
	    }
	    return names;
	}

    /**
     * strictly for unit testing of this utility
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception{
    	
        List<String> classes = getClassNamesFromPackage("report.postmap");
        for(String c:classes){
            System.out.println("Class: "+c);
        }
        
    }
	
}
