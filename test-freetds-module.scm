#!/usr/bin/env chicken-scheme
;; (require 'freetds)
;; (load "freetds.scm")
;; (import "freetds.so")
;; (use freetds)
,l freetds.scm
;; (require-library 'freetds)
(use freetds)
#;(display (make-CS_INT*))
(display (define-make-type* CS_INT))
