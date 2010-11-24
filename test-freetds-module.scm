#!/usr/bin/env chicken-scheme
(use freetds debug)
(debug (make-CS_INT*)
       CS_INT-size
       (make-CS_BINARY*)
       CS_BINARY-size)
