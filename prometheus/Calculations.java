
public class Calculations {
	
	
	
	public static double average (double total, double count) {
		double result = 0;
		if (count != 0)
			result = total / count;
		return result;
	}
	
	
	
	public static Double add (Double operand1, Double operand2) {
		
		Double result = new Double(0);
		
		if ( operand1 == null  && operand2 == null) {}
		else if ( operand1 == null)
			result = result + operand2;
		else if (operand2 ==  null) 
			result = result + operand1;
		else
			result = operand1 + operand2;
		
		return result;
	
	}
	
	public static double add (double operand1, double operand2) {
		return operand1 + operand2;
	}
	

	public static Integer add (Integer operand1, Integer operand2) {
		
		Integer result = new Integer(0);
		
		if ( operand1 == null  && operand2 == null) {}
		else if ( operand1 == null)
			result = result + operand2;
		else if (operand2 ==  null) 
			result = result + operand1;
		else
			result = operand1 + operand2;
		
		return result;
	
	}
	

}
