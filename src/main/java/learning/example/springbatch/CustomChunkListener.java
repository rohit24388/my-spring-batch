package learning.example.springbatch;

import org.springframework.batch.core.listener.ChunkListenerSupport;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Component("chunkListener")
@Slf4j
public class CustomChunkListener extends ChunkListenerSupport {

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
	}

}
