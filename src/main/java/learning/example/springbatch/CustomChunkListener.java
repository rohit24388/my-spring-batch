package learning.example.springbatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.listener.ChunkListenerSupport;
import org.springframework.batch.core.scope.context.ChunkContext;

public class CustomChunkListener extends ChunkListenerSupport {

	private static final Logger log = LoggerFactory.getLogger(CustomChunkListener.class);
	
	@Override
	public void beforeChunk(ChunkContext context) {
		log.info("Before chunk");
	}

	@Override
	public void afterChunk(ChunkContext context) {
		log.info("After chunk");
	}

	@Override
	public void afterChunkError(ChunkContext context) {
		log.info("After chunk error");
		for(String attributeName : context.attributeNames()) {
			log.info("attribute: " + attributeName);
			log.info("value: " + context.getAttribute(attributeName));
		}
	}

}
