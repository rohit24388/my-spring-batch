package learning.example.springbatch;

import javax.persistence.PersistenceException;

import org.hibernate.exception.ConstraintViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.step.skip.SkipLimitExceededException;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.stereotype.Component;

@Component("constraintViolationExceptionSkipper")
public class ConstraintViolationExceptionSkipper implements SkipPolicy {

	private static final Logger log = LoggerFactory.getLogger(ConstraintViolationExceptionSkipper.class);
	
	@Override
	public boolean shouldSkip(Throwable t, int skipCount) throws SkipLimitExceededException {
		log.info("inside ConstraintViolationExceptionSkipper.shouldSkip()");
		if(t instanceof PersistenceException && 
				t.getCause() instanceof ConstraintViolationException) {
			return true;
		}
		return false;
	}

}
