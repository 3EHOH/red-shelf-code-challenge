




@Entity
@Table(name = "inputMonitor")

@org.hibernate.annotations.GenericGenerator( name="hibernate-native", strategy="native")
/*
@org.hibernate.annotations.GenericGenerator( name="sequence-style-generator",
	strategy="org.hibernate.id.enhanced.SequenceStyleGenerator",
	parameters = {
		@Parameter(name = "optimizer", value = "pooled"),
		@Parameter(name = "increment_size", value = "1")}
) 
*/

public class InputMonitor {
	
	
	public InputMonitor(String fName) {
		this.filename = fName;
	}
	
	public InputMonitor () {}
	
	@Id
	@GeneratedValue(generator="hibernate-native")
	//@GeneratedValue(generator = "sequence-style-generator")
	@Basic(optional = false)
	@Column(name="uid")
	private long uid;
	private long jobUid;

	
	private String filename;
	private String delimiter = "";
	
	
	private int rcdCount = 0;
	
	
	@Transient
	private HashMap<Integer, ColumnFinder> col_index;
	
	@Transient
	private HashMap<String, ColumnController> col_cntrl;
	
    @Column(name="colFinder", columnDefinition="TEXT") 
	private String colFinder;
	public String getColFinder () {
		ColumnFinder cf;
		StringBuffer sb = new StringBuffer();
		for (Entry<Integer, ColumnFinder> entry : col_index.entrySet()) {
		    sb.append('|');
			sb.append(entry.getKey());
		    cf = entry.getValue();
		    sb.append("=" + cf.col_name + ":" + cf.col_number);
		}
		colFinder = sb.toString();
		return sb.toString();
	}
	
	public void setColFinder (String s) {
		col_index = new HashMap<Integer, ColumnFinder>();
		String [] sEntry = s.split("\\|");
		for (String sE : sEntry) {
			if (sE.isEmpty())
				continue;
			int ix = Integer.parseInt( sE.substring( sE.indexOf(':') + 1) ) ;
			ColumnFinder cf = new ColumnFinder(ix, sE.substring(sE.indexOf('=') + 1, sE.indexOf(':')));
			col_index.put(Integer.parseInt(sE.substring(0, sE.indexOf('='))), cf );
		}
	}
	
	 @Column(name="colController", columnDefinition="TEXT") 
		private String colController;
		public String getColController () {
			StringBuffer sb = new StringBuffer();
			for (Entry<String, ColumnController> entry : col_cntrl.entrySet()) {
			    sb.append('|').append(entry.getKey()).append('=').append(entry.getValue().getCol_name()).append(':').append(entry.getValue().getOccurrences());
			}
			colController = sb.toString();
			return sb.toString();
		}
		
		public void setColController (String s) {
			col_cntrl = new HashMap<String, ColumnController>();
			String [] sEntry = s.split("\\|");
			for (String sE : sEntry) {
				ColumnController cc = new ColumnController();
				cc.setCol_name(sE.substring( sE.indexOf('=') + 1, sE.indexOf(':') ) );
				cc.setOccurrences( Integer.parseInt(sE.substring( sE.indexOf(':') + 1 ) ) );
				col_cntrl.put(sE.substring(0, sE.indexOf('=')), cc );
			}
		}
	
	public long getJobUid() {
		return jobUid;
	}
	public void setJobUid(long jobUid) {
		this.jobUid = jobUid;
	}
	
	public String getFilename() {
		return filename;
	}
	public void setFilename(String filename) {
		this.filename = filename;
	}
	public String getDelimiter() {
		return delimiter;
	}
	public void setDelimiter(String delimiter) {
		this.delimiter = delimiter;
	}
	public HashMap<Integer, ColumnFinder> getCol_index() {
		if (col_index == null)
			setColFinder(colFinder);
		return col_index;
	}
	public void setCol_index(HashMap<Integer, ColumnFinder> col_index) {
		this.col_index = col_index;
	}
	public HashMap<String, ColumnController> getCol_cntrl() {
		if (col_cntrl == null)
			setColController(colController);
		return col_cntrl;
	}
	public void setCol_cntrl(HashMap<String, ColumnController> col_cntrl) {
		this.col_cntrl = col_cntrl;
	}
	public int getRcdCount() {
		return rcdCount;
	}
	public void setRcdCount(int rcdCount) {
		this.rcdCount = rcdCount;
	}
	

}
