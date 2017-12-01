

public class ColumnFinder implements Serializable {
	

	private static final long serialVersionUID = 642327244605766644L;
	
	
	public String col_name;
	public int col_number;
	public ColumnFinder (int i, String s) {
		col_number = i;
		col_name = s;
	}
	

}
