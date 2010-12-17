(module
 freetds
 *
 (import scheme
         chicken
         foreign)

 (use lolevel
      matchable
      srfi-1
      foreigners
      data-structures
      expand-full)

 (foreign-declare "#include <ctpublic.h>")

 (define-foreign-type CS_CHAR char)
 (define-foreign-type CS_INT integer32)
 (define-foreign-type CS_RETCODE CS_INT)

 (define-syntax define-make-type*
   (er-macro-transformer
    (lambda (expression rename compare)
      (import matchable)
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
                                #;(,%free type*)
                                ((,%foreign-primitive
                                  void
                                  (((c-pointer ,(symbol->string type)) type))
                                  "free(type);")
                                 type*)))
                             type*)))))))))))

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

 (define translate-CS_IMAGE* noop)
 (define translate-CS_TEXT* noop)
 (define translate-CS_MONEY4* noop)
 (define translate-CS_MONEY* noop)
 (define translate-CS_REAL* noop)
 (define translate-CS_FLOAT* noop)
 (define translate-CS_NUMERIC* noop)
 (define translate-CS_DECIMAL* noop)
 (define translate-CS_BIGINT* noop)
 (define translate-CS_INT* noop)
 (define translate-CS_SMALLINT* noop)
 (define translate-CS_TINYINT* noop)
 (define translate-CS_DATETIME4* noop)
 (define translate-CS_DATETIME* noop)
 (define translate-CS_VARCHAR* noop)
 (define translate-CS_LONGCHAR* noop)
 (define translate-CS_CHAR* noop)
 (define translate-CS_BIT* noop)
 (define translate-CS_VARBINARY* noop)
 (define translate-CS_LONGBINARY* noop)
 (define translate-CS_BINARY* noop)

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

 (define (make-connection context* host username password)
   (let-location ((connection* (c-pointer "CS_CONNECTION")))
     (allocate-connection! context* (location connection*))
     (connection-property-set-username! connection* username)
     (connection-property-set-password! connection* password)
     (connect! connection* (location host) (string-length host))
     connection*))

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
               (location query)
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

 #;(define (make-bound-variables command*)
 (let-location ((result-type CS_INT))
 (let ((result-status (results! command* (location result-type))))
 (match result-status
 ((? success?)
 (match result-type
 ;; need to deal with CS_ROW_RESULT, CS_END_RESULTS; and
 ;; possibly CS_CMD_SUCCEED, CS_CMD_FAIL, ...
 ((? row-result?)
 (let-location ((column-count CS_INT))
 (results-info-column-count! command* (location column-count))
 (let ((bound-variables
 (list-tabulate
 column-count
 (lambda (column)
 (let ((data-format* (make-CS_DATAFMT*)))
 (describe! command*
 (add1 column)
 data-format*)
 ;; let's have a table here for modifying,
 ;; if necessary, the data-format*.
 (let ((data-format-datatype
 (data-format-datatype data-format*)))
 'harro))))))
 'oh-jes))))))))))
