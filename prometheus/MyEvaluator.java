

public class MyEvaluator implements TriggeringEventEvaluator {
	  /**
	     Is this <code>event</code> the e-mail triggering event?

	     <p>This method returns <code>true</code>, if the event level
	     has ERROR level or higher. Otherwise it returns
	     <code>false</code>. */
	public
	boolean isTriggeringEvent(LoggingEvent event) {
		return true; //event.getLevel().isGreaterOrEqual(Level.ERROR);
	}
}
