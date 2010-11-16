#!/usr/bin/env chicken-scheme
(use format
     foreigners
     lolevel
     debug
     srfi-1
     srfi-13
     miscmacros
     matchable)

(include "test-freetds-secret.scm")

(foreign-declare "#include <ctpublic.h>")

(define-foreign-type CS_RETCODE integer32)
(define-foreign-type CS_INT integer32)
(define-foreign-type CS_SMALLINT short)
(define-foreign-type CS_CHAR char)

(define (freetds-error location message retcode . arguments)
  (signal (make-composite-condition
           (make-property-condition 'exn
                                    'location location
                                    'message (format "(retcode ~a) ~a"
                                                     retcode
                                                     message)
                                    'arguments arguments)
           (make-property-condition 'freetds
                                    'retcode retcode))))

(define (success? retcode)
  (= retcode (foreign-value "CS_SUCCEED" CS_INT)))

;;; Should rather be called: `error-on-non-success'.
(define (error-on-failure thunk location message . arguments)
  (let ((retcode (thunk)))
    (if (not (success? retcode))
        (apply freetds-error location message retcode arguments))))

(define-foreign-record-type
  (CS_DATAFMT CS_DATAFMT)
  ;; 132 == CS_MAX_NAME
  (CS_CHAR (name 132) data-format-name)
  (CS_INT namelen data-format-name-length data-format-name-length-set!)
  (CS_INT datatype data-format-datatype data-format-datatype-set!)
  (CS_INT format data-format-format data-format-format-set!)
  (CS_INT maxlength data-format-max-length data-format-max-length-set!)
  (CS_INT scale data-format-scale data-format-scale-set!)
  (CS_INT precision data-format-precision data-format-precision-set!)
  (CS_INT status data-format-status data-format-status-set!)
  (CS_INT count data-format-count data-format-count-set!)
  (CS_INT usertype data-format-usertype data-format-usertype-set!)
  ((c-pointer "CS_LOCALE") locale data-format-locale data-format-locale-set!))

(define (char-null? char)
  (char=? char #\nul))

(define char-vector->string
  (case-lambda
   ((char-vector char-ref)
    (char-vector->string char-ref +inf))
   ((char-vector char-ref max-length)
    (define (chars->string chars)
      (reverse-list->string chars))
    (let loop ((index 0)
               (chars '())
               (length max-length))
      (if (zero? length)
          (chars->string chars)
          (let ((char (char-ref char-vector index)))
            (if (char-null? char)
                (chars->string chars)
                (loop (+ index 1)
                      (cons char chars)
                      (- length 1)))))))))

(define CS_CHAR*->string
  (case-lambda
   ((vector) (CS_CHAR*->string vector +inf))
   ((vector max-length)
    (char-vector->string
     vector
     (lambda (vector i)
       ((foreign-primitive
         CS_CHAR
         (((c-pointer "CS_CHAR") vector)
          (int i))
         "C_return(vector[i]);") vector i))
     max-length))))

(define (allocate-context! version context**)
  (error-on-failure
   (lambda ()
     ((foreign-lambda CS_RETCODE
                      "cs_ctx_alloc"
                      CS_INT
                      (c-pointer (c-pointer "CS_CONTEXT")))
      version
      context**))
   'cs_ctx_alloc
   "failed to allocate context"))

(define (initialize-context! context* version)
  (error-on-failure
   (lambda ()
     ((foreign-lambda CS_RETCODE
                      "ct_init"
                      (c-pointer "CS_CONTEXT")
                      CS_INT)
      context*
      version))
   'ct_init
   "failed to initialize context"))

(define (allocate-connection! context* connection**)
  (error-on-failure
   (lambda ()
     ((foreign-lambda CS_RETCODE
                      "ct_con_alloc"
                      (c-pointer "CS_CONTEXT")
                      (c-pointer (c-pointer "CS_CONNECTION")))
      context*
      connection**))
   'ct_con_alloc
   "failed to allocate a connection"))

(define (connection-property connection*
                             action
                             property
                             buffer*
                             buffer-length
                             out-length*)
  (error-on-failure
   (lambda ()
     ((foreign-lambda CS_RETCODE
                      "ct_con_props"
                      (c-pointer "CS_CONNECTION")
                      CS_INT
                      CS_INT
                      (c-pointer "CS_VOID")
                      CS_INT
                      (c-pointer "CS_INT"))
      connection*
      action
      property
      buffer*
      buffer-length
      out-length*))
   'ct_con_props
   (format "failed to perform ~a on the property ~a" action property)))

(define (connection-property-set! connection*
                                  property
                                  buffer*
                                  buffer-length
                                  out-length*)
  (connection-property connection*
                       (foreign-value "CS_SET" CS_INT)
                       property
                       buffer*
                       buffer-length
                       out-length*))

(define (connect! connection* server* server-length)
  (error-on-failure
   (lambda ()
     ((foreign-lambda CS_RETCODE
                      "ct_connect"
                      (c-pointer "CS_CONNECTION")
                      (c-pointer "CS_CHAR")
                      CS_INT)
      connection*
      server*
      server-length))
   'ct_connect
   "failed to connect to server"))

(define (allocate-command! connection* command**)
  (error-on-failure
   (lambda ()
     ((foreign-lambda CS_RETCODE
                      "ct_cmd_alloc"
                      (c-pointer "CS_CONNECTION")
                      (c-pointer (c-pointer "CS_COMMAND")))
      connection*
      command**))
   'ct_cmd_alloc
   "failed to allocate command"))

(define (command! command* type buffer* buffer-length option)
  (error-on-failure
   (lambda ()
     ((foreign-lambda CS_RETCODE
                      "ct_command"
                      (c-pointer "CS_COMMAND")
                      CS_INT
                      (const (c-pointer "CS_VOID"))
                      CS_INT
                      CS_INT)
      command*
      type
      buffer*
      buffer-length
      option))
   'ct_command
   (format "failed to issue command ~a" buffer*)))

(define (send! command*)
  (error-on-failure
   (lambda ()
     ((foreign-lambda CS_RETCODE
                      "ct_send"
                      (c-pointer "CS_COMMAND"))
      command*))
   'ct_send
   "failed to send command"))

(define (results! command* result-type*)
  ((foreign-lambda CS_RETCODE
                   "ct_results"
                   (c-pointer "CS_COMMAND")
                   (c-pointer "CS_INT"))
   command*
   result-type*))

(define (results-info! command* type buffer* buffer-length out-length*)
  (error-on-failure
   (lambda ()
     ((foreign-lambda CS_RETCODE
                      "ct_res_info"
                      (c-pointer "CS_COMMAND")
                      CS_INT
                      (c-pointer "CS_VOID")
                      CS_INT
                      (c-pointer "CS_INT"))
      command*
      type
      buffer*
      buffer-length
      out-length*))
   'ct_res_info
   (format "failed to get results info on ~a" type)))

(define (results-info! command* type buffer* buffer-length out-length*)
  (error-on-failure
   (lambda ()
     ((foreign-lambda CS_RETCODE
                      "ct_res_info"
                      (c-pointer "CS_COMMAND")
                      CS_INT
                      (c-pointer "CS_VOID")
                      CS_INT
                      (c-pointer "CS_INT"))
      command*
      type
      buffer*
      buffer-length
      out-length*))
   'ct_res_info
   "failed to get results info on ~a"))

(define (describe! command* item data-format*)
  (error-on-failure
   (lambda ()
     ((foreign-lambda CS_RETCODE
                      "ct_describe"
                      (c-pointer "CS_COMMAND")
                      CS_INT
                      (c-pointer "CS_DATAFMT"))
      command*
      item
      data-format*))
   'ct_describe
   "failed to describe column"))

(define (bind! command*
               item
               data-format*
               buffer*
               copied*
               indicator*)
  (error-on-failure
   (lambda ()
     ((foreign-lambda CS_RETCODE
                      "ct_bind"
                      (c-pointer "CS_COMMAND")
                      CS_INT
                      (c-pointer "CS_DATAFMT")
                      (c-pointer "CS_VOID")
                      (c-pointer "CS_INT")
                      (c-pointer "CS_SMALLINT"))
      command*
      item
      data-format*
      buffer*
      copied*
      indicator*))
   'ct_bind
   "failed to bind statement"))

(define (fetch! command* type offset option rows-read*)
  ((foreign-lambda CS_RETCODE
                   "ct_fetch"
                   (c-pointer "CS_COMMAND")
                   CS_INT
                   CS_INT
                   CS_INT
                   (c-pointer CS_INT))
   command*
   type
   offset
   option
   rows-read*))

(define-syntax define-make-type*
  (er-macro-transformer
   (lambda (expression rename compare)
     (import matchable)
     (match-let (((_ type) expression))
       (let ((malloc
              (conc "C_return(("
                    type
                    " *) malloc(length * sizeof("
                    type
                    ")));")))
         (let* ((type* (string->symbol (conc type "*")))
                (constructor (string->symbol (conc "make-" type*))))
           (let ((%let (rename 'let))
                 (%define (rename 'define))
                 (%case-lambda (rename 'case-lambda))
                 (%foreign-primitive (rename 'foreign-primitive))
                 (%c-pointer (rename 'c-pointer))
                 (%void (rename 'void))
                 (%int (rename 'int))
                 (%conc (rename 'conc))
                 (%null-pointer? (rename 'null-pointer?))
                 (%signal (rename 'signal))
                 (%make-property-condition
                  (rename 'make-property-condition))
                 (%let (rename 'let))
                 (%if (rename 'if))
                 (%format (rename 'format))
                 (%symbol->string (rename 'symbol->string))
                 (%type (rename 'type))
                 (%set-finalizer! (rename 'set-finalizer!))
                 (%lambda (rename 'lambda)))
             `(,%define ,constructor
                   (,%case-lambda
                    (()
                     (,constructor 1))
                    ((length)
                     (,%let ((type*
                              ((,%foreign-primitive
                                (c-pointer ,(symbol->string type))
                                ((int length))
                                ,malloc)
                               length)))
                       (,%if (,%null-pointer? type*)
                             (,%signal
                              (,%make-property-condition
                               'exn
                               'location ',constructor
                               'message (,%format "could not allocate ~a ~a(s)"
                                                  length
                                                  ',type))))
                       (,%set-finalizer!
                        type*
                        (,%lambda (type*)
                          ((,%foreign-primitive
                            void
                            (((c-pointer ,(symbol->string type)) type))
                            "free(type);")
                           type*)))
                       type*)))))))))))

(define-make-type* CS_CHAR)

(define-make-type* CS_DATAFMT)

(let ((version (foreign-value "CS_VERSION_100" int)))
  (let-location ((context* (c-pointer "CS_CONTEXT")))
    (allocate-context! version (location context*))
    (initialize-context! context* version)
    (let-location ((connection* (c-pointer "CS_CONNECTION")))
      (allocate-connection! context* (location connection*))
      (connection-property-set! connection*
                                (foreign-value "CS_USERNAME" CS_INT)
                                (location username)
                                (foreign-value "CS_NULLTERM" CS_INT)
                                (null-pointer))
      (connection-property-set! connection*
                                (foreign-value "CS_PASSWORD" CS_INT)
                                (location password)
                                (foreign-value "CS_NULLTERM" CS_INT)
                                (null-pointer))
      (connect! connection*
                (location server)
                (string-length server))
      (let-location ((command* (c-pointer "CS_COMMAND")))
        (allocate-command! connection* (location command*))
        (let ((query "SELECT name, refdate FROM SYSOBJECTS WHERE XTYPE = 'U';"))
          (command! command*
                    (foreign-value "CS_LANG_CMD" CS_INT)
                    (location query)
                    (foreign-value "CS_NULLTERM" CS_INT)
                    (foreign-value "CS_UNUSED" CS_INT))
          
          (send! command*)

          (let-location ((result-type CS_INT))
            (let more-results ((result-status
                                (results! command* (location result-type))))
              (if (success? result-status)
                  (begin
                    (select result-type
                      (((foreign-value "CS_ROW_RESULT" CS_INT))
                       (let-location ((column-count CS_INT))
                         (results-info! command*
                                        (foreign-value "CS_NUMDATA" CS_INT)
                                        (location column-count)
                                        (foreign-value "CS_UNUSED" CS_INT)
                                        (null-pointer))
                         (let ((values
                                (list-tabulate
                                 column-count
                                 (lambda (column)
                                   (let ((data-format* (make-CS_DATAFMT*))) 
                                     (describe! command*
                                                (+ column 1)
                                                data-format*)
                                     (data-format-datatype-set!
                                      data-format*
                                      (foreign-value "CS_CHAR_TYPE" int))
                                     (data-format-format-set!
                                      data-format*
                                      (foreign-value "CS_FMT_NULLTERM" int))
                                     (data-format-max-length-set!
                                      data-format*
                                      1024)
                                     (let ((value* (make-CS_CHAR* (+ 1024 1))))
                                       (let-location ((valuelen CS_INT)
                                                      (indicator CS_SMALLINT))
                                         (bind! command*
                                                (+ column 1)
                                                data-format*
                                                value*
                                                (location valuelen)
                                                (location indicator)))
                                       value*))))))
                           (let-location ((rows-read int))
                             (while (success?
                                     (fetch! command*
                                             (foreign-value "CS_UNUSED" CS_INT)
                                             (foreign-value "CS_UNUSED" CS_INT)
                                             (foreign-value "CS_UNUSED" CS_INT)
                                             (location rows-read)))
                                    (debug (map CS_CHAR*->string values))))))))
                    (more-results (results! command* (location result-type))))
                  (begin
                    ((foreign-lambda CS_RETCODE
                                     "ct_cmd_drop"
                                     (c-pointer "CS_COMMAND"))
                     command*)
                    ((foreign-lambda CS_RETCODE
                                     "ct_close"
                                     (c-pointer "CS_CONNECTION")
                                     CS_INT)
                     connection*
                     (foreign-value "CS_UNUSED" CS_INT))
                    ((foreign-lambda CS_RETCODE
                                     "ct_con_drop"
                                     (c-pointer "CS_CONNECTION"))
                     connection*)
                    ((foreign-lambda CS_RETCODE
                                     "ct_exit"
                                     (c-pointer "CS_CONTEXT")
                                     CS_INT)
                     context*
                     (foreign-value "CS_UNUSED" int))
                    ((foreign-lambda CS_RETCODE
                                     "cs_ctx_drop"
                                     (c-pointer "CS_CONTEXT"))
                     context*))))))))))
