# spring-batch

This implementation demonstrates how Spring Batch and Database Transaction Management work in case of chunked database read and write when we add skips for certain Exception types.

How to test this implementation?

I have used an external Oracle database. PERSON table as input and EMPLOYEE table as output. The EMPLOYEE table has person_id column as unique. 
You will need to run the application twice.
1. In the first run, set spring.jpa.hibernate.ddl-auto=create in application.properties so that both the tables are created and populated. The test data can be found in import.sql.
2. Now manually delete every record from EMPLOYEE table except the one with person_id=5. Also set spring.jpa.hibernate.ddl-auto=none in application.properties so that from the next run onward, tables are not dropped and created.
3. Run the application a second time. You will see that the job completes successfully but looking at the EMPLOYEE table will tell you that the record with person_id=5 was not inserted by this run of the job (its employee_id will be lesser than other records). Look at the log and you will notice that the first chunk of 3 records (person_id = {1, 2, 3})were read and written just fine. It was the second chunk of 3 records (person_id = (4, 5, 6}) that first failed. It is so because the second chunk has a record with person_id=5, something which EMPLOYEE table already had, and hence ConstraintViolationException.
Since faultTolerant() is enabled for Step 'step1', retries till the retry-limit (default is 2 probably) will happen. There is also a skipper for PersistenceException (a super class of ConstraintViolationException, because ConstraintViolationException, for some reason, isn't skipped).The final result of the batch job is that Employee object with person_id=5 is skipped while the remaining 6 Employee objects are persisted. 