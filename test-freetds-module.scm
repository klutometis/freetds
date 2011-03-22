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
         (let ((i (number->string i)))
           (call-with-result-set
            connection
            (format "INSERT INTO #harro VALUES(~a, ~a, ~a, ~a, ~a, ~a)" i i i i i i)
            (lambda (command) (result-values context connection command)))))
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
