# spring-batch

This application demonstrates a simple multi-threaded batch job involving a database read and write. It uses SimpleAsyncTaskExecutor which as the name suggests is not synchronized and hence not thread safe. To overcome that, Person.reader() uses 'synchronized' keyword.
Run the application in quick succession repeatedly to see how it always passes.