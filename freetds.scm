(module
 freetds
 *
 (import scheme
         chicken
         foreign
         lolevel)

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
 (define-foreign-type CS_RETCODE CS_INT))
