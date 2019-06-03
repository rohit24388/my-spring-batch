# spring-batch

This implementation demonstrates how Spring Batch and Database Transaction Management work in case of chunked database read and write when we add skips for certain Exception types.

How to test this implementation?

I have used an external Oracle database. PERSON table as input and EMPLOYEE table as output. The EMPLOYEE table has person_id column as unique. 
You will need to run the application twice.
1. In the first run, set spring.jpa.hibernate.ddl-auto=create in application.properties so that both the tables are created and populated. The test data can be found in import.sql.
2. Now manually delete every record from EMPLOYEE table except the ones with person_id={3,5,7}. Also set spring.jpa.hibernate.ddl-auto=none in application.properties so that from the next run onward, tables are not dropped and created.
3. Run the application a second time. You will see that the job completes successfully but looking at the EMPLOYEE table will tell you that the records with person_id=(3,5,7} were not inserted by this run of the job (its employee_id will be lesser than other records). Look at the log and you will will be able to see the EMPLOYEE records with which exact PERSON_IDs were skipped. There is also a skipper for PersistenceException (a super class of ConstraintViolationException, because ConstraintViolationException, for some reason, isn't skipped). But inside shouldSkip() method, I am validating if the cause of the exception was ConstraintViolationException. I have also used SkipLimit=1, but that doesn't seem to be working, because I would expect just the first available EMPLOYEE record (PERSON_ID=3) to be skipped and 5 and 7 to result in a failure, but actually all 3 of them are skipped.