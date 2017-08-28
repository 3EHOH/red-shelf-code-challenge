

public class ColumnController implements Serializable {
	
	
	private static final long serialVersionUID = 6340604121591044792L;
	
	private String col_name;
	private int occurrences = 0;
	

	public String getCol_name() {
		return col_name;
	}

	public void setCol_name(String col_name) {
		this.col_name = col_name;
	}

	public int getOccurrences() {
		return occurrences;
	}

	public void setOccurrences(int occurrences) {
		this.occurrences = occurrences;
	}
	

}
