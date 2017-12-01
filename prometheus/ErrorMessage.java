


public class ErrorMessage {
	
	private String id;
	private int count;
	private int level;
	private int limit;
	private ArrayList<String> prepend;
	private ArrayList<String> append;
	private String text;
	private int accumulatorRow;							// spot where I intend to hold the 'nnn' more like this note
	
	// levels
	final static int INFO = 1;
	final static int WARN = 2;
	final static int ERROR = 3;
	
	ErrorMessage (String s) {
		this.id = s;
	}
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}
	public int getLevel() {
		return level;
	}
	public void setLevel(int level) {
		this.level = level;
	}
	
	public void setLevelFromString(String s) {
		String sN = s.toUpperCase();
		switch (sN) { 
		case "ERROR": 
			setLevel(3); 
			break;
		case "WARN": 
			setLevel(2); 
			break;
		case "INFO": 
			setLevel(1); 
			break;
		default:
			log.error("Found an error message with an invalid level: " + s);
			break;
		} 
	}
	
	public String getStringFromLevel() {
		String s = "n/a";
		switch (level) { 
		case ERROR: 
			s = "ERROR"; 
			break;
		case WARN: 
			s = "WARN"; 
			break;
		case INFO: 
			s = "INFO"; 
			break;
		default:
			log.error("Found an error message with an invalid level: " + level);
			break;
		} 
		return s;
	}

	public int getLimit() {
		return limit;
	}

	public void setLimit(int limit) {
		this.limit = limit;
	}

	public ArrayList<String> getPrepend() {
		return prepend;
	}

	public void setPrepend(ArrayList<String> prepend) {
		this.prepend = prepend;
	}

	public ArrayList<String> getAppend() {
		return append;
	}

	public void setAppend(ArrayList<String> append) {
		this.append = append;
	}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}
	
	public int getAccumulatorRow() {
		return accumulatorRow;
	}

	public void setAccumulatorRow(int accumulatorRow) {
		this.accumulatorRow = accumulatorRow;
	}

	private static org.apache.log4j.Logger log = Logger.getLogger(ErrorMessage.class);
	
}
