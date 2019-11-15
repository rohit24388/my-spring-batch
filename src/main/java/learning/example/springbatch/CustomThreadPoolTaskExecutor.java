package learning.example.springbatch;

import java.util.concurrent.TimeUnit;

import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

public class CustomThreadPoolTaskExecutor extends ThreadPoolTaskExecutor {

	private static final long serialVersionUID = -6132281659263247550L;

	public CustomThreadPoolTaskExecutor(int corePoolSize, int maxPoolSize, int queueCapacity) {
		super.setCorePoolSize(corePoolSize);
		super.setMaxPoolSize(maxPoolSize);
		super.setQueueCapacity(queueCapacity);
	}

	@Override
	public void afterPropertiesSet() {
		super.afterPropertiesSet();
		super.getThreadPoolExecutor().setKeepAliveTime(1, TimeUnit.MILLISECONDS);
	}

}
