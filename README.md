# spring-batch

This implementation demonstrates how to use a custom ItemWriter to flush records to database one by one and catch all bad records and re-throw the corresponding exceptions to be intercepted by the StepExecutionListener (CustomChunkListener in this implementation)