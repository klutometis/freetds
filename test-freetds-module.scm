#!/usr/bin/env chicken-scheme
(use freetds)
(define debug print)
(include "test-freetds-secret.scm")
(call-with-context
 (lambda (context)
   (call-with-connection
    context
    server
    username
    password
    (lambda (connection)
      (call-with-result-set
       connection
       "CREATE TABLE #harro (a VARCHAR(30), b VARCHAR(30), c VARCHAR(30), d VARCHAR(30), e VARCHAR(30), f VARCHAR(30))"
       (lambda (command) (debug (result-values context connection command))))
      (for-each
       (lambda (i)
         (call-with-result-set
          connection
          "INSERT INTO #harro VALUES(?, ?, ?, ?, ?, ?)"
          i (+ i 1) (+ i 2) (+ i 3) (+ i 4) (+ i 5)
          (lambda (command) (result-values context connection command))))
       (iota (expt 2 10)))
      (call-with-result-set
       connection
       "SELECT a, b, c, d, e, f FROM #harro"
       (lambda (command) (debug (result-values context connection command))))
      (call-with-result-set
       connection
       "SELECT a, b, c, d, e, f FROM #harro"
       (lambda (command) (debug (result-values context connection command))))
      (call-with-result-set
       connection
       "SELECT a, b, c, d, e, f FROM #harro"
       (lambda (command) (debug (result-values context connection command))))))))
