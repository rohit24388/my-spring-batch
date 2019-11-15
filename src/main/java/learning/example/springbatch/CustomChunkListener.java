package learning.example.springbatch;

import java.util.concurrent.TimeUnit;

import org.springframework.batch.core.listener.ChunkListenerSupport;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Component("chunkListener")
@Slf4j
public class CustomChunkListener extends ChunkListenerSupport {
	
	@Autowired
	TaskExecutor taskExecutor;
	
	@Override
	public void beforeChunk(ChunkContext context) {
		log.info("Before chunk");
		if(taskExecutor instanceof CustomThreadPoolTaskExecutor) {
			ThreadPoolTaskExecutor taskExecutor = (ThreadPoolTaskExecutor) this.taskExecutor;
			log.info("Active thread count = {}", taskExecutor.getActiveCount());
			log.info("Current pool size = {}", taskExecutor.getPoolSize());
			log.info("Completed task count = {}", taskExecutor.getThreadPoolExecutor().getCompletedTaskCount());
			log.info("Scheduled task count = {}", taskExecutor.getThreadPoolExecutor().getTaskCount());
			log.info("Queue size = {}", taskExecutor.getThreadPoolExecutor().getQueue().size());
			log.info("Remaining queue capacity = {}", taskExecutor.getThreadPoolExecutor().getQueue().remainingCapacity());
			log.info("Keep Alive seconds = {}", taskExecutor.getKeepAliveSeconds());
			log.info("Keep Alive milliseconds = {}", taskExecutor.getThreadPoolExecutor().getKeepAliveTime(TimeUnit.MILLISECONDS));
		}
	}

	@Override
	public void afterChunk(ChunkContext context) {
		log.info("After chunk");
	}
	
	@Override
	public void afterChunkError(ChunkContext chunkContext) {
		log.info("After chunk error");
	}

}
