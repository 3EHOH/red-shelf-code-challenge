
public class MathUtil {
	
	
	
	public static float getPercentage(long n, long total) {
		float proportion = 0.00f;
		if (total != 0) {
			proportion = ((float) n) / ((float) total);
			//proportion = proportion * 100;
		}
		return proportion;
	}
	
	
	/**
	 * @param in The integer value
	 * @param fill The number of digits to fill
	 * @return The given value left padded with the given number of digits
	 */
	public static String lPadZero(int in, int fill){

	    boolean negative = false;
	    int value, len = 0;

	    if(in >= 0){
	        value = in;
	    } else {
	        negative = true;
	        value = - in;
	        in = - in;
	        len ++;
	    }

	    if(value == 0){
	        len = 1;
	    } else{         
	        for(; value != 0; len ++){
	            value /= 10;
	        }
	    }

	    StringBuilder sb = new StringBuilder();

	    if(negative){
	        sb.append('-');
	    }

	    for(int i = fill; i > len; i--){
	        sb.append('0');
	    }

	    sb.append(in);

	    return sb.toString();       
	    
	}

}
