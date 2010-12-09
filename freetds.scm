(module
 freetds
 *
 (import scheme
         chicken
         foreign)

 (use lolevel)

 (foreign-declare "#include <ctpublic.h>")

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

 (define-syntax define-make-type*/type-size
   (er-macro-transformer
    (lambda (expression rename compare)
      (import matchable)
      (match-let (((_ . types) expression))
        (let ((%define-make-type* (rename 'define-make-type*))
              (%define-type-size (rename 'define-type-size))
              (%begin (rename 'begin)))
          (cons
           %begin
           (map (lambda (type)
                  `(,%begin
                    (,%define-make-type* ,type)
                    (,%define-type-size ,type)))
                types)))))))

 (define-make-type*/type-size
   CS_BINARY
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
   CS_IMAGE)

 (define-foreign-type CS_INT integer32)
 (define-foreign-type CS_RETCODE CS_INT)

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

 (define (make-connection context* host username password)
   (let-location ((connection* (c-pointer "CS_CONNECTION")))
     (allocate-connection! context* (location connection*))
     (connection-property-set-username! connection* username)
     (connection-property-set-password! connection* password)
     (connect! connection* (location host) (string-length host))
     connection*)))
