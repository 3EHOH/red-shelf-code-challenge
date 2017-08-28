
public class Condition {
	
	String operator;
	String operand1;
	String operand2;
	
	
	final static String [] validOperators = {"!=", "="};
	
	Condition (String s) {
		for (String operator : Condition.validOperators) {
			if (s.contains(operator)) {
				this.operator = operator;
				operand1 = s.substring(0, s.indexOf(operator));
				operand2 = s.substring(s.indexOf(operator) + operator.length());
				break;
			}
		}
	}
	
}
