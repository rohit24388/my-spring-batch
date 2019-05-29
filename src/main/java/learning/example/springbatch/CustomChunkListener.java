package learning.example.springbatch;

import org.springframework.batch.core.listener.ChunkListenerSupport;
import org.springframework.batch.core.scope.context.ChunkContext;

public class CustomChunkListener extends ChunkListenerSupport {

	@Override
	public void beforeChunk(ChunkContext context) {
		System.out.println("Before chunk");
	}

	@Override
	public void afterChunk(ChunkContext context) {
		System.out.println("After chunk");
	}

	@Override
	public void afterChunkError(ChunkContext context) {
		System.out.println("After chunk error");
		for(String attributeName : context.attributeNames()) {
			System.out.println("attribute: " + attributeName);
			System.out.println("value: " + context.getAttribute(attributeName));
		}
	}

}
