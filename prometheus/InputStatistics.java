

public class InputStatistics {
	
	public String fileName = new String();
	public String fileClass = new String();
	public LinkedHashMap<String, StatEntry>column_stats = new LinkedHashMap<String, StatEntry>();
	
	public class StatEntry {
		public String col_name;
		public boolean bExpected;
		public double filled_count;
		public double invalid_count;
		public double valid_count;
	}

}
