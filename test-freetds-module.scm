#!/usr/bin/env chicken-scheme
(use freetds debug)
#;(debug (make-CS_INT*)
       CS_INT-size
       (make-CS_BINARY*)
       CS_BINARY-size)
(include "test-freetds-secret.scm")
(let* ((context (make-context))
       (connection (make-connection context
                                    server
                                    username
                                    password)))
  (display connection))
