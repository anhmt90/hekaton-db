The semantic of the program is described in file '_project_report.pdf'.

The branch master contains the program executing the (original) transaction newOrder with single thread.
The other branches are used for correctness and performance testing:
	- Branch 'test-visibility-validation': test for read stability. 
	- Branch 'test-phantom-avoidance': test for phantom detection.
	- Branch 'test-cascaded-aborts: test for cascaded aborts in the context of commit dependencies.
	- Branch 'test-scanning': test scan speed of the program by scanning the OrderLine table multiple times.
	- Branch 'read-only': test performance of read-only transactions.
	- Branch 'update-only': test performance of update-only transactions.
In edge cases, a transaction behaves as follows:
	+ SELECT: if the required tuples donot exist, a transaction must abort and rollback because the variables from this SELECT-statement will be needed below.
	+ INSERT: if inserting an duplicate PK into a table, the transaction must abort and rollback.
	+ UPDATE and DELETE: if updating/deleting a non-existing tuple, the statement is ignored.
	
