(use freetds sql-null)
(define debug print)
(include "test-freetds-secret.scm")
(define connection (make-connection server username password))

(call-with-result-set
 connection
 "CREATE TABLE #harro (a VARCHAR(30), b VARCHAR(30), c VARCHAR(30), d VARCHAR(30), e VARCHAR(30), f VARCHAR(30))"
 (lambda (command) (debug (result-values connection command))))

(for-each
 (lambda (i)
   (call-with-result-set
    connection
    "INSERT INTO #harro VALUES(?, ?, ?, ?, ?, ?)"
    i (+ i 1) (exact->inexact (+ i 2)) (->string (+ i 3)) (+ i 4) (sql-null)
    (lambda (command) (result-values connection command))))
 (iota (expt 2 10)))

(call-with-result-set
 connection
 "SELECT a, b, c, d, e, f FROM #harro"
 (lambda (command) (debug (result-values connection command))))

(call-with-result-set
 connection
 "SELECT a, b, c, d, e, f FROM #harro"
 (lambda (command) (debug (result-values connection command))))

(call-with-result-set
 connection
 "SELECT a, b, c, d, e, f FROM #harro"
 (lambda (command) (debug (result-values connection command))))
