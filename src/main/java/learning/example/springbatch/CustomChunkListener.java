package learning.example.springbatch;

import java.sql.Timestamp;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.listener.ChunkListenerSupport;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.scope.context.StepContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
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
		for(String attributeName : chunkContext.attributeNames()) {
			log.info("attribute: " + attributeName);
			log.info("value: " + chunkContext.getAttribute(attributeName));
			if(attributeName.equals("sb_rollback_exception")) {
				ErrorLog errorLog = new ErrorLog(new Timestamp(new Date().getTime()), 
						chunkContext.getAttribute(attributeName).toString());
				StepContext stepContext = chunkContext.getStepContext();
				errorLog.setStepExecutionId(stepContext.getStepExecution().getId());
				errorLog.setStepName(stepContext.getStepName());
				errorLog.setJobExecutionId(stepContext.getStepExecution().getJobExecutionId());
				errorLog.setJobName(stepContext.getJobName());
				errorDao.addLog(errorLog);
			}
		}
		
	}

}
