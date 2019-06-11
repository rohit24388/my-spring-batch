package learning.example.springbatch;

import java.sql.Timestamp;
import java.util.Date;

import org.hibernate.exception.ConstraintViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.listener.ChunkListenerSupport;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.scope.context.StepContext;
import org.springframework.batch.core.step.item.Chunk;
import org.springframework.batch.core.step.skip.SkipLimitExceededException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.retry.ExhaustedRetryException;
import org.springframework.stereotype.Component;

@Component("chunkListener")
public class CustomChunkListener extends ChunkListenerSupport {

	private static final Logger log = LoggerFactory.getLogger(CustomChunkListener.class);
	
	@Autowired
	@Qualifier("errorDao")
	private ErrorDao errorDao;
	
	@Override
	public void beforeChunk(ChunkContext context) {
		log.info("Before chunk");
	}

	@Override
	public void afterChunk(ChunkContext context) {
		log.info("After chunk");
	}

	@Override
	public void afterChunkError(ChunkContext chunkContext) {
		log.info("After chunk error");
		int chunkSize = 0;
		if(chunkContext.hasAttribute("INPUTS")) {
			log.info("INPUTS: " + chunkContext.getAttribute("INPUTS"));
			Chunk<?> inputs = (Chunk<?>) chunkContext.getAttribute("INPUTS");
			if(inputs.getItems() != null) {
				chunkSize = inputs.getItems().size();
			}
		}
		if(chunkContext.hasAttribute("sb_rollback_exception")) {
			log.info("sb_rollback_exception: " + chunkContext.getAttribute("sb_rollback_exception"));
			String error = chunkContext.getAttribute("sb_rollback_exception").toString();
			BatchErrorLog errorLog = new BatchErrorLog(new Timestamp(new Date().getTime()), error);
			StepContext stepContext = chunkContext.getStepContext();
			errorLog.setStepExecutionId(stepContext.getStepExecution().getId());
			errorLog.setStepName(stepContext.getStepName());
			errorLog.setJobExecutionId(stepContext.getStepExecution().getJobExecutionId());
			errorLog.setJobName(stepContext.getJobName());
			String actionTaken = null;
			if(chunkContext.getAttribute("sb_rollback_exception") instanceof SkipLimitExceededException ||
					chunkContext.getAttribute("sb_rollback_exception") instanceof ExhaustedRetryException) {
				actionTaken = "STEP FAILED";
			} else if(chunkContext.getAttribute("sb_rollback_exception") instanceof ConstraintViolationException) { 
				if(chunkSize < BatchConfiguration.CHUNK_SIZE) {
					actionTaken = "ITEM SKIPPED";
				} else {
					actionTaken = "CHUNK RETRIED";
				}
			} else {
				actionTaken = "CHUNK RETRIED";
			}
			errorLog.setActionTaken(actionTaken);
			log.info("Write skip count = " + stepContext.getStepExecution().getWriteSkipCount());
			errorDao.addLog(errorLog);
		}		
	}

}
