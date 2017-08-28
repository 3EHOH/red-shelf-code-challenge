
public class HelperPEBTF {
	
	
	public static String [] splitter (String line) {
		
		boolean open = false;
		
		StringBuffer sb = new StringBuffer();
		for (int i=0; i < line.length(); i++) {
			if (!open && line.charAt(i) != ',')
				open = true;
			if (open  && line.charAt(i) == ',')
				open = false;
			if (line.charAt(i) == '"') {
				open = !open;
				continue;
			}
			if (open) {
				sb.append(line.charAt(i));
			}
			else {
				sb.append('^');
			}
		}
		return sb.toString().split("\\^");
	}

}
