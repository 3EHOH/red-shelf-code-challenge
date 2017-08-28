



public class GenericOutputCSV implements GenericOutputInterface {

	@Override
	public String write(Object o) {
		
		//log.info("Write entry");
		// first time thru do file name, headers, etc.
		if (!bInitialized)
			initialize(o);
		
		StringBuffer sb = new StringBuffer();
		Field[] f =  o.getClass().getDeclaredFields();
		Object object = o;
		Object value;
		
		for (int i=0; i < f.length; i++) {
			//log.info(f[i].getName());
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
				if (value == null)
					sb.append('"').append('"').append(sDelimiter);
				else if (f[i].getType().getName().equals("char")) {
					sb.append('"').append( ((Character) value).toString()).append('"').append(sDelimiter);
				}
				else if (f[i].getType().equals(String.class)) {
					sb.append('"').append( (String) value ).append('"').append(sDelimiter);
				}
				else if (f[i].getType().equals(Integer.class)  ||  f[i].getType().equals(int.class)) {
					sb.append('"').append( (Integer) value ).append('"').append(sDelimiter);
				}
				else if (f[i].getType().equals(Double.class)  ||  f[i].getType().equals(double.class)) {
					sb.append('"').append( (Double) value ).append('"').append(sDelimiter);
				}
				else if (f[i].getType().equals(Long.class)  ||  f[i].getType().equals(long.class)) {
					sb.append('"').append( (Long) value ).append('"').append(sDelimiter);
				}
				else if (f[i].getType().equals(Boolean.class)  ||  f[i].getType().equals(boolean.class)) {
					sb.append('"').append( Boolean.toString((Boolean) value) ).append('"').append(sDelimiter);
				}
				else if (f[i].getType().equals(Date.class)) {
					sb.append('"').append( sdf.format( (Date) value) ).append('"').append(sDelimiter);
				}
				else if (f[i].getType().equals(List.class)) {
					sb.append('"').append('{');
					for(Object oList:(List<?>) f[i].get(o)){
						sb.append(oList).append("; ");
					}
					if ( ((List<?>) f[i].get(o)).size() > 0)
						sb.setLength(sb.length()-2);
					sb.append('}').append('"').append(sDelimiter);
				}	
				else
					sb.append('"').append(f[i].getType()).append('"').append(sDelimiter);
			} 
			catch (IllegalArgumentException | IllegalAccessException e) {
				log.error("Failure processing " + f[i].getName());
			}
		}
		
		sb.setLength(sb.length() == 0 ?  0  :  sb.length()-1);
		try {
			bwOut.write(sb.toString());
			bwOut.newLine();
		} catch (IOException e) {
			log.error("An error occurred while writing header for  " + sFileName);
			e.printStackTrace();
		}
		
		return "";
		
	}
	
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	private void initialize(Object o) {
		
		// define the output file
		Date date = Calendar.getInstance().getTime();
		DateFormat formatter = new SimpleDateFormat("yyyy_MM_dd_hhmm");
		today = formatter.format(date);
		sFileName = FileNamer.getFileName( o.getClass().getName() ) + ".csv";
		try {
			outputFile = new FileWriter(sFileName);
		} catch (IOException e1) {
			log.error("An error occurred while opening " + sFileName);
		}
		bwOut = new BufferedWriter(outputFile);
		
		// write out column header 
		Field[] f =  o.getClass().getDeclaredFields();
		StringBuffer sb = new StringBuffer();
		for (int i=0; i < f.length; i++) {
			if (f[i].getName().equals("serialVersionUID"))		// don't bother with any serialization UID's
				continue;
			if (f[i].getName().equals("errMgr"))				// or the error manager
				continue;
			if (Modifier.isTransient(f[i].getModifiers()))
				continue;
			if (Modifier.isStatic(f[i].getModifiers()))
				continue;
			sb.append('"').append(f[i].getName()).append('"').append(sDelimiter);
		}
		sb.setLength(sb.length()-1);
		try {
			bwOut.write(sb.toString());
			bwOut.newLine();
		} catch (IOException e) {
			log.error("An error occurred while writing header for  " + sFileName);
			e.printStackTrace();
		}
		
		bInitialized = true;
		
	}

	boolean bInitialized = false;
	FileWriter outputFile = null;
	BufferedWriter bwOut;
	String sDefaultPath = "C:\\temp\\";
	String sDelimiter = ",";
	String today;
	String sFileName;
	
	private static org.apache.log4j.Logger log = Logger.getLogger(GenericOutputCSV.class);

	@Override
	public void close() {
		try {
			bwOut.close();
		} catch (IOException e) {
			log.error("An error occurred while closing " + sFileName);
			e.printStackTrace();
		}
	}
	
	

}
