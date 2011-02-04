#!/usr/bin/env chicken-scheme
(use freetds debug)
(include "test-freetds-secret.scm")
(call-with-context
 (lambda (context)
   (call-with-connection
    context
    server
    username
    password
    (lambda (connection)
      #;(call-with-result-set
       connection
       "SELECT TOP 256 PhysicianId from RGILDS.dbo.ttAccession; SELECT TOP 256 PhysicianId from RGILDS.dbo.ttAccession"
       (lambda (command)
         (debug (result-values context connection command) (result-values context connection command))))
      (call-with-result-set
       connection
       "CREATE TABLE #harro (id INT PRIMARY KEY)"
       (lambda (command) (debug (result-values context connection command))))
      (call-with-result-set
       connection
       (format "INSERT INTO #harro VALUES(~a)" 1)
       (lambda (command) (debug (result-values context connection command))))
      (call-with-result-set
       connection
       "INSERT INTO #harro VALUES(2)"
       (lambda (command) (debug (result-values context connection command))))
      (call-with-result-set
       connection
       "SELECT id FROM #harro"
       (lambda (command) (debug (result-values context connection command))))
      #;(let ((command (make-command connection "CREATE TABLE #harro (id INT); INSERT INTO #harro VALUES(1); SELECT id FROM #harro; SELECT TOP 10 PhysicianId from RGILDS.dbo.ttAccession")))
        #;(let ((bound-variables (make-bound-variables connection command)))
          (if (eor-object? bound-variables)
              eor-object
              (let next ((results '()))
                (let ((row (row-fetch context command bound-variables)))
                  (if (eod-object? row)
                      results
                      (next (cons row results)))))))
        #;(debug (result-values context connection command)
               (result-values context connection command)))))))
