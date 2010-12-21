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
      (call-with-result-set
       connection
       "SELECT * from testDatabase.dbo.test"
       (lambda (command)
         (debug (result-values context connection command))))))))
