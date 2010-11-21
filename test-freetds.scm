#!/usr/bin/env chicken-scheme
(use format
     foreigners
     lolevel
     debug
     srfi-1
     srfi-4
     srfi-13
     srfi-19
     miscmacros
     matchable)

(include "test-freetds-secret.scm")

(foreign-declare "#include <ctpublic.h>")

(define-foreign-type CS_RETCODE integer32)
(define-foreign-type CS_INT integer32)
(define-foreign-type CS_UINT unsigned-integer32)
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

(define (row-failure? retcode)
  (= retcode (foreign-value "CS_ROW_FAIL" CS_INT)))

;;; Should rather be called: `error-on-non-success'.
(define (error-on-failure thunk location message . arguments)
  (let ((retcode (thunk)))
    (if (not (success? retcode))
        (apply freetds-error location message retcode arguments))))

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
              (sprintf "C_return(malloc(length * (sizeof(~a))));"
                      type
                      type)))
         (let* ((type* (string->symbol (sprintf "~a*" type)))
                (make-type (string->symbol (sprintf "make-~a" type*))))
           (let ((%let (rename 'let))
                 (%define (rename 'define))
                 (%case-lambda (rename 'case-lambda))
                 (%foreign-primitive (rename 'foreign-primitive))
                 (%foreign-value (rename 'foreign-value))
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
                 (%lambda (rename 'lambda))
                 (%begin (rename 'begin))
                 (%free (rename 'free)))
             `(,%define ,make-type
                 (,%case-lambda
                  (()
                   (,make-type 1))
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
                             'location ',make-type
                             'message (,%format "could not allocate ~a ~a(s)"
                                                length
                                                ',type))))
                     (,%set-finalizer!
                      type*
                      (,%lambda (type*)
                        (,%free type*)
                        #;((,%foreign-primitive
                          void
                          (((c-pointer ,(symbol->string type)) type))
                          "free(type);")
                         type*)))
                     type*)))))))))))

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

(define-foreign-record-type
  (CS_DATETIME CS_DATETIME)
  (CS_INT dtdays datetime-days)
  (CS_INT dttime datetime-time))

(define-foreign-record-type
  (CS_DATEREC CS_DATEREC)
  (CS_INT dateyear daterec-year)
  (CS_INT datemonth daterec-month)
  (CS_INT datedmonth daterec-dmonth)
  (CS_INT datedyear daterec-dyear)
  (CS_INT datedweek daterec-week)
  (CS_INT datehour daterec-hour)
  (CS_INT dateminute daterec-minute)
  (CS_INT datesecond daterec-second)
  (CS_INT datemsecond daterec-msecond)
  (CS_INT datetzone daterec-timezone))

(define-foreign-record-type
  (CS_VARBINARY CS_VARBINARY)
  (CS_SMALLINT len varbinary-length)
  (CS_CHAR (array 256) varbinary-array))

(define-foreign-record-type
  (CS_VARCHAR CS_VARCHAR)
  (CS_SMALLINT len varchar-length)
  (CS_CHAR (str 256) varchar-string))

(define-foreign-record-type
  (CS_MONEY CS_MONEY)
  (CS_INT mnyhigh money-high)
  (CS_UINT mnylow money-low))

(define-foreign-record-type
  (CS_MONEY4 CS_MONEY4)
  (CS_INT mny4 small-money-value))

(define-make-type* CS_DATAFMT)
(define-make-type* CS_DATEREC)

(define-syntax define-type-size
  (er-macro-transformer
   (lambda (expression rename compare)
     (import matchable)
     (match-let (((_ type) expression))
       (let ((size (sprintf "sizeof(~a)" type))
             (type-size (string->symbol (sprintf "~a-size" type))))
         (let ((%define (rename 'define))
               (%foreign-value (rename 'foreign-value)))
           `(,%define ,type-size (,%foreign-value ,size int))))))))

(define-syntax define-make-type*/type-size
  (er-macro-transformer
   (lambda (expression rename compare)
     (import matchable)
     (match-let (((_ type) expression))
       (let ((%define-make-type* (rename 'define-make-type*))
             (%define-type-size (rename 'define-type-size))
             (%begin (rename 'begin)))
         `(,%begin (,%define-make-type* ,type)
                   (,%define-type-size ,type)))))))

(define type->make-type*/type-size/translate-type* '())

(define-syntax define-make-type*/type-size/update-type-table!
  (er-macro-transformer
   (lambda (expression rename compare)
     (import matchable)
     (match-let (((_ type) expression))
       (let ((make-type*
              (string->symbol (sprintf "make-~a*" type)))
             (type-size
              (string->symbol (sprintf "~a-size" type)))
             (translate-type*
              (string->symbol (sprintf "translate-~a*" type)))
             (type-type
              (sprintf "~a_TYPE" type)))
         (let ((%alist-cons (rename 'alist-cons))
               (%set! (rename 'set!))
               (%type->make-type*/type-size/translate-type*
                (rename 'type->make-type*/type-size/translate-type*))
               (%cons* (rename 'cons*))
               (%foreign-value (rename 'foreign-value))
               (%begin (rename 'begin))
               (%define-make-type*/type-size
                (rename 'define-make-type*/type-size)))
           `(,%begin
             (,%define-make-type*/type-size ,type)
             (,%set! ,%type->make-type*/type-size/translate-type*
                     (,%alist-cons
                      (,%foreign-value ,type-type int)
                      (,%cons* ,make-type*
                               ,type-size
                               ,translate-type*)
                      ,%type->make-type*/type-size/translate-type*)))))))))

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

(define (CS_DATETIME*->srfi-19-date datetime* type)
  (let ((daterec* (make-CS_DATEREC*)))
    (error-on-failure
     (lambda ()
       ((foreign-lambda CS_RETCODE
                        "cs_dt_crack"
                        (c-pointer "CS_CONTEXT")
                        CS_INT
                        (c-pointer "CS_VOID")
                        (c-pointer "CS_DATEREC"))
        (null-pointer)
        type
        datetime*
        daterec*))
     'cs_dt_crack
     "failed to crack date")
    (make-date (* (daterec-msecond daterec*) 1000000)
               (daterec-second daterec*)
               (daterec-minute daterec*)
               (daterec-hour daterec*)
               (daterec-dmonth daterec*)
               (add1 (daterec-month daterec*))
               (daterec-year daterec*)
               (daterec-timezone daterec*))))

(define-syntax CS_INT*->number
  (er-macro-transformer
   (lambda (expression rename compare)
     (import matchable)
     (match-let (((_ int* type return-type) expression))
       (let ((%foreign-safe-lambda*
              (rename 'foreign-safe-lambda*)))
         `((,%foreign-safe-lambda*
            ,return-type
            (((c-pointer ,type) i))
            "C_return((int) *i);")
           ,int*))))))

(define (CS_BINARY*->vector binary* length)
  (let ((vector (make-u8vector length 0)))
    ((foreign-safe-lambda*
      void
      (((c-pointer "CS_BINARY") from)
       (u8vector to)
       (int length))
      "memcpy(to, from, length * sizeof(CS_BINARY));")
     binary*
     vector
     length) 
    vector))

(define (translate-CS_BINARY* context* binary* length)
  (CS_BINARY*->vector binary* length))
(define translate-CS_LONGBINARY* noop)
(define (translate-CS_VARBINARY* context* varbinary* length)
  (debug length (varbinary-length varbinary*))
  ;; can't seems to retrieve a pointer to the beginning of the array
  ;; with object->pointer; resorting, therefore, to
  ;; foreign-safe-lambda*.
  ;; (CS_BINARY*->vector ((foreign-safe-lambda*
  ;;                       (c-pointer "CS_CHAR")
  ;;                       (((c-pointer "CS_VARBINARY") varbinary))
  ;;                       "C_return(varbinary->array);")
  ;;                      varbinary*)
  ;;                     (varbinary-length varbinary*)
  ;;                     #;256)
  (CS_BINARY*->vector varbinary*
                      256))
;;; boolean transformation?
(define (translate-CS_BIT* context* bit* length)
  (not (zero? (CS_INT*->number bit* "CS_BIT" short))))
(define (translate-CS_CHAR* context* char* length)
  (CS_CHAR*->string char* length))
(define translate-CS_LONGCHAR* noop)
(define (translate-CS_VARCHAR* context* varchar* length)
  (CS_CHAR*->string (varchar-string varchar*)
                    (varchar-length varchar*)))
(define (translate-CS_DATETIME* context* datetime* length)
  (CS_DATETIME*->srfi-19-date
   datetime*
   (foreign-value "CS_DATETIME_TYPE" CS_INT)))
(define (translate-CS_DATETIME4* context* datetime4* length)
  (CS_DATETIME*->srfi-19-date
   datetime4*
   (foreign-value "CS_DATETIME4_TYPE" CS_INT)))
(define (translate-CS_TINYINT* context* tinyint* length)
  (CS_INT*->number tinyint* "CS_TINYINT" short))
(define (translate-CS_SMALLINT* context* smallint* length)
  (CS_INT*->number smallint* "CS_SMALLINT" short))
(define (translate-CS_INT* context* int* length)
  (CS_INT*->number int* "CS_INT" integer32))
(define (translate-CS_BIGINT* context* bigint* length)
  (CS_INT*->number bigint* "CS_BIGINT" integer64))
(define translate-CS_DECIMAL* noop)
(define translate-CS_NUMERIC* noop)
(define (translate-CS_FLOAT* context* float* length)
  ((foreign-safe-lambda*
    double
    (((c-pointer "CS_FLOAT") n))
    "C_return((double) *n);")
   float*))
(define (translate-CS_REAL* context* real* length)
  ((foreign-safe-lambda*
    float
    (((c-pointer "CS_REAL") n))
    "C_return((float) *n);")
   real*))
(define (translate-CS_MONEY* context* money* length)
  (+ (* (money-high money*) (expt 2 32))
     (money-low money*)))
(define (translate-CS_MONEY4* context* small-money* length)
  (small-money-value small-money*))
(define (translate-CS_TEXT* context* text* length)
  (CS_CHAR*->string text* length))
(define translate-CS_IMAGE* noop)

(define-make-type*/type-size/update-type-table! CS_BINARY)
(define-make-type*/type-size/update-type-table! CS_LONGBINARY)
(define-make-type*/type-size/update-type-table! CS_VARBINARY)
(define-make-type*/type-size/update-type-table! CS_BIT)
(define-make-type*/type-size/update-type-table! CS_CHAR)
(define-make-type*/type-size/update-type-table! CS_LONGCHAR)
(define-make-type*/type-size/update-type-table! CS_VARCHAR)
(define-make-type*/type-size/update-type-table! CS_DATETIME)
(define-make-type*/type-size/update-type-table! CS_DATETIME4)
(define-make-type*/type-size/update-type-table! CS_TINYINT)
(define-make-type*/type-size/update-type-table! CS_SMALLINT)
(define-make-type*/type-size/update-type-table! CS_INT)
(define-make-type*/type-size/update-type-table! CS_BIGINT)
(define-make-type*/type-size/update-type-table! CS_DECIMAL)
(define-make-type*/type-size/update-type-table! CS_NUMERIC)
(define-make-type*/type-size/update-type-table! CS_FLOAT)
(define-make-type*/type-size/update-type-table! CS_REAL)
(define-make-type*/type-size/update-type-table! CS_MONEY)
(define-make-type*/type-size/update-type-table! CS_MONEY4)
(define-make-type*/type-size/update-type-table! CS_TEXT)
(define-make-type*/type-size/update-type-table! CS_IMAGE)

(define type->make-type*/type-size/translate-type*/default
  (case-lambda
   ((type)
    (type->make-type*/type-size/translate-type*/default
     type
     (lambda ()
       (freetds-error 'type->make-type*/type-size/translate-type*/default
                      "encountered a strange type"
                      type))))
   ((type default)
    (let ((make-type*/type-size
           (alist-ref type
                      type->make-type*/type-size/translate-type*)))
      (or make-type*/type-size (default))))))

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
        (let* ((query "SELECT * FROM SYSOBJECTS WHERE XTYPE = 'U';")
               (query "SELECT * FROM testDatabase.dbo.test;")
               ;; (query "SELECT binary, varbinary, DATALENGTH(varbinary) FROM testDatabase.dbo.test;")
               ;; (query "SELECT money FROM testDatabase.dbo.test;")
               )
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
                         (let ((values/translate-types*
                                (list-tabulate
                                 column-count
                                 (lambda (column)
                                   (let ((data-format* (make-CS_DATAFMT*))) 
                                     (describe! command*
                                                (+ column 1)
                                                data-format*)
                                     (select (data-format-datatype data-format*)
                                       (((foreign-value "CS_CHAR_TYPE" CS_INT)
                                         (foreign-value "CS_LONGCHAR_TYPE" CS_INT) 
                                         (foreign-value "CS_TEXT_TYPE" CS_INT)
                                         (foreign-value "CS_VARCHAR_TYPE" CS_INT)
                                         (foreign-value "CS_BINARY_TYPE" CS_INT)
                                         (foreign-value "CS_LONGBINARY_TYPE" CS_INT)
                                         (foreign-value "CS_VARBINARY_TYPE" CS_INT))
                                        (data-format-format-set!
                                         data-format*
                                         (foreign-value "CS_FMT_PADNULL" CS_INT))))
                                     (match-let
                                         (((make-type* type-size . translate-type*)
                                           (type->make-type*/type-size/translate-type*/default
                                            (data-format-datatype
                                             data-format*))))
                                       (let* ((length
                                               (inexact->exact
                                                (ceiling
                                                 (/ (data-format-max-length
                                                     data-format*)
                                                    type-size))))
                                              (value* (make-type* length)))
                                         (let-location ((valuelen CS_INT)
                                                        (indicator CS_SMALLINT))
                                           (bind! command*
                                                  (+ column 1)
                                                  data-format*
                                                  value*
                                                  (location valuelen)
                                                  (location indicator)))
                                         (cons* value* translate-type* length))))))))
                           (let-location ((rows-read int))
                             (while (let ((retcode
                                           (fetch! command*
                                                   (foreign-value "CS_UNUSED" CS_INT)
                                                   (foreign-value "CS_UNUSED" CS_INT)
                                                   (foreign-value "CS_UNUSED" CS_INT)
                                                   (location rows-read))))
                                      (debug retcode)
                                      (or (success? retcode)
                                          (row-failure? retcode)))
                                    (debug (map (lambda
                                                    (value/translate-type*/length)
                                                  (match-let
                                                      (((value
                                                         translate-type* .
                                                         length)
                                                        value/translate-type*/length))
                                                    (translate-type* context*
                                                                     value
                                                                     length)))
                                                values/translate-types*))))))))
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
