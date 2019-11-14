# spring-batch

This application demonstrates a simple multi-threaded batch job involving a database read and write. It uses SimpleAsyncTaskExecutor which as the name suggests is not synchronized and hence not thread safe. 
Run the application in quick succession repeatedly to see how it sometime may fail and sometimes may pass. 
If you are unlucky and always see the same behavior (always fail or always pass), replace the PersonWriter with and JpaItemWriter bean (commented out in BatchConfiguration.java), as that has a greater probability of showing both behaviors.