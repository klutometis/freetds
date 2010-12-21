#!/usr/bin/env chicken-scheme
(use freetds debug)
#;(debug (make-CS_INT*)
       CS_INT-size
       (make-CS_BINARY*)
       CS_BINARY-size)
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
