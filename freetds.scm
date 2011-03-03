(module
 freetds
 *
 (import scheme
         chicken
         foreign)

 (import-for-syntax matchable)

 (use lolevel
      matchable
      srfi-1
      srfi-4
      srfi-19
      foreigners
      data-structures
      foof-loop
      sql-null)

 (define alist-sentinel (cons #f #f))

 (define (alist-sentinel? object)
   (eq? alist-sentinel object))

 (define alist-ref/default
   (case-lambda
    ((key alist)
     (alist-ref/default key
                        alist
                        (lambda () (error "could not find key" key alist))))
    ((key alist default)
     (let ((value (alist-ref key alist eqv? alist-sentinel)))
       (if (alist-sentinel? value)
           (default)
           value)))))

 (define eor-object (cons #f #f))
 
 (define (eor-object? object) (eq? object eor-object))

 (set-read-syntax! 'eor (lambda (port) 'eor-object))

 (define eod-object (cons #f #f))
 
 (define (eod-object? object) (eq? object eod-object))

 (set-read-syntax! 'eod (lambda (port) 'eod-object))

 (foreign-declare "#include <ctpublic.h>")

 (define-foreign-type CS_CHAR char)
 (define-foreign-type CS_INT integer32)
 (define-foreign-type CS_UINT unsigned-integer32)
 (define-foreign-type CS_SMALLINT short)
 (define-foreign-type CS_RETCODE CS_INT)

 (define-syntax define-make-type*
   (er-macro-transformer
    (lambda (expression rename compare)
      (match-let (((_ type) expression))
        (let ((malloc
               (sprintf "C_return(malloc(length * (sizeof(~a))));"
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
                                ;; (,%free type*)
                                ((,%foreign-primitive
                                  void
                                  (((c-pointer ,(symbol->string type)) type))
                                  "free(type);")
                                 type*)))
                             type*)))))))))))

 (define-syntax define-type-size
   (er-macro-transformer
    (lambda (expression rename compare)
      (match-let (((_ type) expression))
        (let ((size (sprintf "sizeof(~a)" type))
              (type-size (string->symbol (sprintf "~a-size" type))))
          (let ((%define (rename 'define))
                (%foreign-value (rename 'foreign-value)))
            `(,%define ,type-size (,%foreign-value ,size int))))))))

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

 (define-foreign-record-type
   (CS_NUMERIC CS_NUMERIC)
   (CS_CHAR precision numeric-precision)
   (CS_CHAR scale numeric-scale)
   ;; 33 = CS_MAX_NUMLEN
   (CS_CHAR (array 33) numeric-array))

 (define-make-type* CS_DATAFMT)
 (define-make-type* CS_DATEREC)
 (define-make-type* CS_DATAFMT)

 (define-for-syntax datatypes
   '(CS_BINARY
     CS_LONGBINARY
     CS_VARBINARY
     CS_BIT
     CS_CHAR
     CS_LONGCHAR
     CS_VARCHAR
     CS_DATETIME
     CS_DATETIME4
     CS_TINYINT
     CS_SMALLINT
     CS_INT
     CS_BIGINT
     CS_DECIMAL
     CS_NUMERIC
     CS_FLOAT
     CS_REAL
     CS_MONEY
     CS_MONEY4
     CS_TEXT
     CS_IMAGE))

 (define-syntax define-make-types*/datatypes
   (er-macro-transformer
    (lambda (expression rename compare)
      (let ((%define-make-type* (rename 'define-make-type*))
            (%begin (rename 'begin)))
        (cons
         %begin
         (map (lambda (type)
                `(,%define-make-type* ,type))
              datatypes))))))

 (define-syntax define-type-sizes/datatypes
   (er-macro-transformer
    (lambda (expression rename compare)
      (let ((%define-type-size (rename 'define-type-size))
            (%begin (rename 'begin)))
        (cons
         %begin
         (map (lambda (type)
                `(,%define-type-size ,type))
              datatypes))))))

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

 (define (CS_DATETIME*->srfi-19-date context* datetime* type)
   (let ((daterec* (make-CS_DATEREC*)))
     (error-on-non-success
      (lambda ()
        ((foreign-lambda CS_RETCODE
                         "cs_dt_crack"
                         (c-pointer "CS_CONTEXT")
                         CS_INT
                         (c-pointer "CS_VOID")
                         (c-pointer "CS_DATEREC"))
         context*
         type
         datetime*
         daterec*))
      'cs_dt_crack
      "failed to crack date")
     ;; HACK: assuming that unparsable dates are NULL.
     (condition-case
      (make-date (* (daterec-msecond daterec*) 1000000)
                 (daterec-second daterec*)
                 (daterec-minute daterec*)
                 (daterec-hour daterec*)
                 (daterec-dmonth daterec*)
                 (add1 (daterec-month daterec*))
                 (daterec-year daterec*)
                 (daterec-timezone daterec*))
      ((exn) (sql-null)))))

 (define-syntax CS_INT*->number
   (er-macro-transformer
    (lambda (expression rename compare)
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
 (define translate-CS_LONGBINARY* translate-CS_BINARY*)
 (define (translate-CS_VARBINARY* context* varbinary* length)
   ;; (debug length (varbinary-length varbinary*))
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
 (define translate-CS_LONGCHAR*
   translate-CS_CHAR*)
 (define (translate-CS_VARCHAR* context* varchar* length)
   (CS_CHAR*->string (varchar-string varchar*)
                     (varchar-length varchar*)))
 (define (translate-CS_DATETIME* context* datetime* length)
   (CS_DATETIME*->srfi-19-date
    context*
    datetime*
    (foreign-value "CS_DATETIME_TYPE" CS_INT)))
 (define (translate-CS_DATETIME4* context* datetime4* length)
   (CS_DATETIME*->srfi-19-date
    context*
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
 (define (cardinality integer base)
   (loop ((for power (up-from 0))
          (until (> (expt base power) integer))) => power))
 (define (translate-CS_NUMERIC* context* numeric* length)
   (let ((maximum-number-length (foreign-value "CS_MAX_NUMLEN" int)))
     (let ((positive? (zero? (char->integer (numeric-array numeric* 0))))
           (base-256-digits
            (cardinality (expt 10 (sub1
                                   (char->integer
                                    (numeric-precision numeric*))))
                         256)))
       (let add ((augend 0) (index 1))
         (if (> index base-256-digits)
             (let* ((scale (char->integer (numeric-scale numeric*)))
                    (number
                     (if (zero? scale)
                         augend
                         (exact->inexact (/ augend (expt 10 scale))))))
               (if positive? number (* number -1)))
             (add (let ((base (char->integer (numeric-array numeric* index))))
                    (if (zero? base)
                        augend
                        (+ augend
                           (* base (expt 256 (- base-256-digits index))))))
                  (+ index 1)))))))
 (define translate-CS_DECIMAL* translate-CS_NUMERIC*)
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
   (inexact->exact
    (+ (* (money-high money*) (expt 2 32))
       (money-low money*))))
 (define (translate-CS_MONEY4* context* small-money* length)
   (small-money-value small-money*))
 (define (translate-CS_TEXT* context* text* length)
   (CS_CHAR*->string text* length))
 (define translate-CS_IMAGE* translate-CS_TEXT*)

 (define-for-syntax (datatype->integer datatype)
   (let ((datatype-type (format "~a_TYPE" datatype)))
     `(foreign-value ,datatype-type CS_INT)))

 (define-for-syntax (datatype->make-type* datatype)
   (string->symbol (format "make-~a*" datatype)))

 (define-for-syntax (datatype->type-size datatype)
   (string->symbol (format "~a-size" datatype)))

 (define-for-syntax (datatype->translate-type* datatype)
   (string->symbol (format "translate-~a*" datatype)))

 (define-syntax define-datatype->make-type*/datatypes
   (er-macro-transformer
    (lambda (expression rename compare)
      (let ((%define (rename 'define))
            (%quasiquote (rename 'quasiquote))
            (%unquote (rename 'unquote)))
        `(,%define datatype->make-type*
                   (,%quasiquote
                    ,(map (lambda (type)
                            (cons `(,%unquote ,(datatype->integer type))
                                  `(,%unquote ,(datatype->make-type* type))))
                          datatypes)))))))

 (define-syntax define-datatype->type-size/datatypes
   (er-macro-transformer
    (lambda (expression rename compare)
      (let ((%define (rename 'define))
            (%quasiquote (rename 'quasiquote))
            (%unquote (rename 'unquote)))
        `(,%define datatype->type-size
                   (,%quasiquote
                    ,(map (lambda (type)
                            (cons `(,%unquote ,(datatype->integer type))
                                  `(,%unquote ,(datatype->type-size type))))
                          datatypes)))))))

 (define-syntax define-datatype->translate-type*/datatypes
   (er-macro-transformer
    (lambda (expression rename compare)
      (let ((%define (rename 'define))
            (%quasiquote (rename 'quasiquote))
            (%unquote (rename 'unquote)))
        `(,%define datatype->translate-type*
                   (,%quasiquote
                    ,(map (lambda (type)
                            (cons `(,%unquote ,(datatype->integer type))
                                  `(,%unquote ,(datatype->translate-type* type))))
                          datatypes)))))))

 (define-make-types*/datatypes)
 (define-type-sizes/datatypes)
 (define-datatype->make-type*/datatypes)
 (define-datatype->type-size/datatypes)
 (define-datatype->translate-type*/datatypes)

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

 (define (row-result? retcode)
   (= retcode (foreign-value "CS_ROW_RESULT" CS_INT)))

 (define (row-format-result? retcode)
   (= retcode (foreign-value "CS_ROWFMT_RESULT" CS_INT)))

 (define (row-fail? retcode)
   (= retcode (foreign-value "CS_ROW_FAIL" CS_INT)))

 (define (end-results? retcode)
   (= retcode (foreign-value "CS_END_RESULTS" CS_INT)))

 (define (end-data? retcode)
   (= retcode (foreign-value "CS_END_DATA" CS_INT)))

 (define (fail? retcode)
   (= retcode (foreign-value "CS_FAIL" CS_INT)))

 (define (command-done? retcode)
   (= retcode (foreign-value "CS_CMD_DONE" CS_INT)))

 (define (command-succeed? retcode)
   (= retcode (foreign-value "CS_CMD_SUCCEED" CS_INT)))

 (define (error-on-non-success thunk location message . arguments)
   (let ((retcode (thunk)))
     (if (not (success? retcode))
         (apply freetds-error location message retcode arguments))))

 (define (allocate-context! version context**)
   (error-on-non-success
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
   (error-on-non-success
    (lambda ()
      ((foreign-lambda CS_RETCODE
                       "ct_init"
                       (c-pointer "CS_CONTEXT")
                       CS_INT)
       context*
       version))
    'ct_init
    "failed to initialize context"))

 (define make-context
   (case-lambda
    (()
     (let ((version (foreign-value "CS_VERSION_100" CS_INT)))
       (make-context version)))
    ((version)
     (let-location ((context* (c-pointer "CS_CONTEXT")))
       (allocate-context! version (location context*))
       (initialize-context! context* version)
       context*))))

 (define (allocate-connection! context* connection**)
   (error-on-non-success
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
   (error-on-non-success
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

 (define (connection-property-set-username! connection* username)
   (connection-property-set! connection*
                             (foreign-value "CS_USERNAME" CS_INT)
                             (location username)
                             (foreign-value "CS_NULLTERM" CS_INT)
                             (null-pointer)))

 (define (connection-property-set-password! connection* password)
   (connection-property-set! connection*
                             (foreign-value "CS_PASSWORD" CS_INT)
                             (location password)
                             (foreign-value "CS_NULLTERM" CS_INT)
                             (null-pointer)))

 (define (connect! connection* server* server-length)
   (error-on-non-success
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

 (define (use! context* connection* database)
   (call-with-result-set
    connection*
    ;; needs to be escaped!
    (format "USE ~a" database)
    (cut result-values context* connection* <>)))

 (define make-connection
   (case-lambda
    ((context* host username password)
     (make-connection context* host username password #f))
    ((context* host username password database)
     (let-location ((connection* (c-pointer "CS_CONNECTION")))
       (allocate-connection! context* (location connection*))
       (connection-property-set-username! connection* username)
       (connection-property-set-password! connection* password)
       (connect! connection* (location host) (string-length host))
       (if database (use! context* connection* database))
       connection*))))

 (define (allocate-command! connection* command**)
   (error-on-non-success
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
   (error-on-non-success
    (lambda ()
      ((foreign-lambda CS_RETCODE
                       "ct_command"
                       (c-pointer "CS_COMMAND")
                       CS_INT
                       ;; (const (c-pointer "CS_VOID"))
                       (const c-string)
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
   (error-on-non-success
    (lambda ()
      ((foreign-lambda CS_RETCODE
                       "ct_send"
                       (c-pointer "CS_COMMAND"))
       command*))
    'ct_send
    "failed to send command"))

 (define (make-command connection* query . parameters)
   (let-location ((command* (c-pointer "CS_COMMAND")))
     (allocate-command! connection* (location command*))
     (command! command*
               (foreign-value "CS_LANG_CMD" CS_INT)
               query
               (foreign-value "CS_NULLTERM" CS_INT)
               (foreign-value "CS_UNUSED" CS_INT))
     (send! command*)
     command*))

 (define (results! command* result-type*)
   ((foreign-lambda CS_RETCODE
                    "ct_results"
                    (c-pointer "CS_COMMAND")
                    (c-pointer "CS_INT"))
    command*
    result-type*))

 (define (results-info! command* type buffer* buffer-length out-length*)
   (error-on-non-success
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

 (define (results-info-column-count! command* column-count*)
   (results-info! command*
                  (foreign-value "CS_NUMDATA" CS_INT)
                  column-count*
                  (foreign-value "CS_UNUSED" CS_INT)
                  (null-pointer)))

 (define (describe! command* item data-format*)
   (error-on-non-success
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
   (error-on-non-success
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

 (define (command-drop! command*)
   (error-on-non-success
    (lambda ()
      ((foreign-lambda CS_RETCODE
                       "ct_cmd_drop"
                       (c-pointer "CS_COMMAND"))
       command*))
    'ct_cmd_drop
    "failed to drop command"))

 (define (connection-close! connection*)
   (error-on-non-success
    (lambda ()
      ((foreign-lambda CS_RETCODE
                       "ct_con_drop"
                       (c-pointer "CS_CONNECTION"))
       connection*))
    'ct_con_drop
    "failed to close connection"))

 (define (context-exit! context*)
   (error-on-non-success
    (lambda ()
      ((foreign-lambda CS_RETCODE
                       "ct_exit"
                       (c-pointer "CS_CONTEXT")
                       CS_INT)
       context*
       (foreign-value "CS_UNUSED" int)))
    'ct_exit
    "failed to exit context"))

 (define (context-drop! context*)
   (error-on-non-success
    (lambda ()
      ((foreign-lambda CS_RETCODE
                       "cs_ctx_drop"
                       (c-pointer "CS_CONTEXT"))
       context*))
    'cs_ctx_drop
    "failed to drop context"))

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

 (define cancel!
   (case-lambda
    ((connection* command*)
     (cancel! connection*
              command*
              (foreign-value "CS_CANCEL_ALL" CS_INT)))
    ((connection* command* type)
     ((foreign-lambda CS_RETCODE
                      "ct_cancel"
                      (c-pointer "CS_CONNECTION")
                      (c-pointer "CS_COMMAND")
                      CS_INT)
      connection*
      command*
      type))))

 (define close!
   (case-lambda
    ((connection*)
     (close! connection* (foreign-value "CS_FORCE_CLOSE" CS_INT)))
    ((connection* option)
     (error-on-non-success
      (lambda ()
        ((foreign-lambda CS_RETCODE
                         "ct_close"
                         (c-pointer "CS_CONNECTION")
                         CS_INT)
         connection*
         option))
      'ct_close
      "failed to close connection"))))

 (define (make-bound-variables connection* command*)
   (let-location ((result-type CS_INT))
     (let ((result-status (results! command* (location result-type))))
       (match result-status
         ((? success?)
          (match result-type
            ;; need to deal with CS_ROW_RESULT, CS_END_RESULTS; and
            ;; possibly CS_CMD_SUCCEED, CS_CMD_FAIL, ...
            ((or (? row-result?)
                 (? row-format-result?))
             (let-location ((column-count CS_INT))
               (results-info-column-count! command* (location column-count))
               (list-tabulate
                column-count
                (lambda (column)
                  (let ((data-format* (make-CS_DATAFMT*)))
                    (describe! command*
                               (add1 column)
                               data-format*)
                    ;; let's have a table here for modifying,
                    ;; if necessary, the data-format*.
                    (let ((datatype
                           (data-format-datatype data-format*)))
                      (select datatype
                        (((foreign-value "CS_CHAR_TYPE" CS_INT)
                          (foreign-value "CS_LONGCHAR_TYPE" CS_INT) 
                          (foreign-value "CS_TEXT_TYPE" CS_INT)
                          (foreign-value "CS_VARCHAR_TYPE" CS_INT)
                          (foreign-value "CS_BINARY_TYPE" CS_INT)
                          (foreign-value "CS_LONGBINARY_TYPE" CS_INT)
                          (foreign-value "CS_VARBINARY_TYPE" CS_INT)
                          (foreign-value "CS_DATETIME_TYPE" CS_INT)
                          (foreign-value "CS_DATETIME4_TYPE" CS_INT))
                         (data-format-format-set!
                          data-format*
                          (foreign-value "CS_FMT_PADNULL" CS_INT)))) 
                      (let ((make-type*
                             (alist-ref/default
                              datatype
                              datatype->make-type*))
                            (type-size
                             (alist-ref/default

                              datatype
                              datatype->type-size))
                            (translate-type*
                             (alist-ref/default
                              datatype
                              datatype->translate-type*)))
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
                                   (location indicator))
                            (cons* value* translate-type* length))))))))))
            ((? command-done?)
             ;; is this appropriate? do we need to deallocate the
             ;; command here?
             eor-object)
            ((? command-succeed?)
             '())
            (_
             (freetds-error 'make-bound-variables
                            "ct_results returned a bizarre result-type"
                            result-type))))
         ((? fail?)
          (let ((retcode (cancel! connection* command*)))
            (match retcode
              ((? fail?)
               (close! connection*)
               (freetds-error 'make-bound-variables
                              (string-append "ct_results and ct_cancel failed, "
                                             "prompting the connection to close")
                              retcode))
              (_
               (freetds-error 'make-bound-variables
                              "ct_results failed, cancelling command"
                              retcode)))))
         ((? end-results?)
          ;; #!eor
          (command-drop! command*)
          eor-object)
         (_
          (freetds-error 'make-bound-variables
                         "ct_results returned a bizarre result status"
                         result-status))))))

 (define (row-fetch context* command* bound-variables)
   (let-location ((rows-read int))
     (let ((retcode (fetch! command*
                            (foreign-value "CS_UNUSED" CS_INT)
                            (foreign-value "CS_UNUSED" CS_INT)
                            (foreign-value "CS_UNUSED" CS_INT)
                            (location rows-read))))
       (match retcode
         ((? success? row-fail?)
          (map (lambda (bound-variable)
                 (match-let (((value translate-type* . length)
                              bound-variable))
                   (translate-type* context*
                                    value
                                    length)))
               bound-variables))
         ((? fail?)
          ;; cancel
          ;; fail again -> close
          (command-drop! command*)
          (freetds-error 'row-fetch
                         "fetch! returned CS_FAIL"
                         retcode))
         ((? end-data?)
          eod-object)
         (_
          (freetds-error 'row-fetch
                         "fetch! returned unknown retcode"
                         retcode))))))

 (define (result-values context* connection* command*)
   (let ((bound-variables (make-bound-variables connection* command*)))
     (if (eor-object? bound-variables)
         eor-object
         (let next ((results '()))
           (let ((row (row-fetch context* command* bound-variables)))
             (if (eod-object? row)
                 results
                 (next (cons row results))))))))

 ;; this version won't even let me use the ((lambda () (format ...)))
 ;; trick for procedural strings.
 ;; (define (call-with-result-set connection* query process-command)
 ;;   (let ((command* #f))
 ;;     (dynamic-wind
 ;;         (lambda () (set! command* (make-command connection* query)))
 ;;         (lambda () (process-command command*))
 ;;         (lambda ()
 ;;           ;; Only cancel the command here, so that the connection is
 ;;           ;; reusable.
 ;;           (cancel! (null-pointer) command*)
 ;;           (command-drop! command*))))

 (define (call-with-result-set connection* query process-command)
   (let ((command* (make-command connection* query)))
     (dynamic-wind
         noop
         (lambda () (process-command command*))
         (lambda ()
           ;; Only cancel the command here, so that the connection is
           ;; reusable.
           (cancel! (null-pointer) command*)
           (command-drop! command*)))))

 (define (call-with-context process-context)
   (let ((context* (make-context)))
     (dynamic-wind
         noop
         (lambda () (process-context context*))
         (lambda ()
           (context-exit! context*)
           (context-drop! context*)))))

 (define call-with-connection
   (case-lambda
    ((context* server username password process-connection)
     (call-with-connection context*
                           server
                           username
                           password
                           #f
                           process-connection))
    ((context* server username password database process-connection)
     (let ((connection* (make-connection context*
                                         server
                                         username
                                         password
                                         database)))
       (dynamic-wind
           noop
           (lambda () (process-connection connection*))
           (lambda ()
             (connection-close! connection*))))))))
