# spring-batch

This application demonstrates an application using SyncTaskExecutor which doesn't need to use 'synchronized' keyword but the disadvantage is that it executes each task in the calling thread itself. So it's not really multi-threading.