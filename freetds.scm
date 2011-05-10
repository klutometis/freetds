|#

Copyright 2011 Response Genetics, Inc.

This file is part of the FreeTDS egg.

The FreeTDS egg is free software: you can redistribute it and/or
modify it under the terms of the GNU Lesser Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

Foobar is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser Public License
for more details.

You should have received a copy of the GNU Lesser Public License along
with the FreeTDS egg.  If not, see <http://www.gnu.org/licenses/>.

#|

(module
 freetds
 (make-connection connection? connection-open? connection-close connection-reset!
  send-query send-query* result? result-cleanup!
  result-value result-values result-values/alist
  result-row result-row/alist result-column column-name column-names
  call-with-result-set call-with-connection
  row-map row-map* row-for-each row-for-each*
  row-fold row-fold* row-fold-right row-fold-right*
  column-map column-map* column-for-each column-for-each*
  column-fold column-fold* column-fold-right column-fold-right*)

 (import scheme chicken foreign)

 (use lolevel srfi-1 srfi-4 data-structures
      foreigners srfi-19 sql-null numbers)

 (foreign-declare "#include <ctpublic.h>")

 (define-foreign-type CS_CHAR char)
 (define-foreign-type CS_INT integer32)
 (define-foreign-type CS_UINT unsigned-integer32)
 (define-foreign-type CS_SMALLINT short)
 (define-foreign-type CS_RETCODE CS_INT)
 ;; Not really necessary; ctlib doesn't consistently use this type either
 (define-foreign-type CS_BOOL CS_INT)

 (define CS_UNUSED (foreign-value "CS_UNUSED" CS_INT))
 (define CS_TRUE   (foreign-value "CS_TRUE"   CS_BOOL))
 (define CS_FALSE  (foreign-value "CS_FALSE"  CS_BOOL))

;; Create a global application context upon library load.  Contexts
;; aren't really used for anything, except a couple of properties and
;; other global settings.  FreeTDS doesn't impose a limit of
;; connections per context like Sybase's library does, so there's no
;; need to maintain separate contexts.  If necessary, we can always
;; decide to move context into the connection maintenance procedures.
;; This should be completely transparent, so it won't break backwards
;; compatibility.
(define *app-context*
  (let ((ctx ((foreign-lambda* (c-pointer "CS_CONTEXT") ()
                               "CS_CONTEXT *ctx;"
                               "if (cs_ctx_alloc(CS_VERSION_100, &ctx) != CS_SUCCEED)"
                               "    C_return(NULL);"
                               "if (ct_init(ctx, CS_VERSION_100) != CS_SUCCEED) {"
                               "    cs_ctx_drop(ctx);"
                               "    C_return(NULL);"
                               "}"
                               "C_return(ctx);"))))
    (unless ctx (error (conc "Could not allocate and initialize FreeTDS context! "
                             "This should never happen.  Out of memory?")))
    (on-exit (lambda ()
               ((foreign-lambda* void (((c-pointer "CS_CONTEXT") ctx))
                                 "if (ct_exit(ctx, CS_FORCE_EXIT) == CS_SUCCEED)"
                                 "    cs_ctx_drop(ctx);") ctx)))
    ctx))

 ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
 ;;;; Custom types and FreeTDS<->Scheme type conversion
 ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

 (define-foreign-record-type
   (CS_DATAFMT CS_DATAFMT)
   (constructor: make-CS_DATAFMT*)
   (destructor: free-CS_DATAFMT*)
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
        ((foreign-lambda*
          CS_CHAR
          (((c-pointer "CS_CHAR") vector)
           (int i))
          "C_return(vector[i]);") vector i))
      max-length))))

 (define (CS_DATETIME*->srfi-19-date datetime* type)
   (let ((date-components (make-vector 8)))
     (error-on-non-success
      #f
      (lambda ()      
        ((foreign-lambda* CS_INT (((c-pointer "CS_CONTEXT") ctx)
                                  (CS_INT type)
                                  ((c-pointer "CS_VOID") dtp)
                                  (scheme-object v))
                          "CS_RETCODE res; CS_DATEREC dr;"
                          ""
                          "res = cs_dt_crack(ctx, type, dtp, &dr);"
                          "if (res != CS_SUCCEED)"
                          "  C_return(res);"
                          ""
                          "/* Prepare vector in the order make-date accepts its arguments */"
                          "/* Milliseconds->nanoseconds for make-date */"
                          "C_set_block_item(v, 0, C_fix(dr.datemsecond*1000000));"
                          "C_set_block_item(v, 1, C_fix(dr.datesecond));"
                          "C_set_block_item(v, 2, C_fix(dr.dateminute));"
                          "C_set_block_item(v, 3, C_fix(dr.datehour));"
                          "C_set_block_item(v, 4, C_fix(dr.datedmonth));"
                          "/* Day must be 1-31 for make-date */"
                          "C_set_block_item(v, 5, C_fix(dr.datemonth+1));"
                          "C_set_block_item(v, 6, C_fix(dr.dateyear));"
                          "C_set_block_item(v, 7, C_fix(dr.datetzone));"
                          "C_return(res);")
         *app-context* type datetime* date-components))
      'cs_dt_crack
      "failed to crack date")
     ;; HACK: assuming that unparsable dates are NULL.
     (condition-case
         (apply make-date (vector->list date-components))
       ((exn) (sql-null)))))

 (define-syntax CS_INT*->number
   (syntax-rules ()
     ((_ int* type return-type)
      ((foreign-safe-lambda* return-type (((c-pointer type) i))
                             "C_return((int) *i);") int*))))

 (define (CS_BINARY*->vector binary* length)
   (let ((vector (make-u8vector length 0)))
     ((foreign-safe-lambda* void (((c-pointer "CS_BINARY") from)
                                  (u8vector to)
                                  (int length))
                            "memcpy(to, from, length * sizeof(CS_BINARY));")
      binary* vector length)
     vector))

 (define null-indicator?
   (foreign-lambda* bool (((c-pointer "CS_SMALLINT") indicator))
                    "C_return(*indicator == -1);"))

 (define (translate-CS_BINARY* binary* length)
   (CS_BINARY*->vector binary* length))
 (define translate-CS_LONGBINARY* translate-CS_BINARY*)
 (define (translate-CS_VARBINARY* varbinary* length)
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
 (define (translate-CS_BIT* bit* length)
   (not (zero? (CS_INT*->number bit* "CS_BIT" short))))
 (define (translate-CS_CHAR* char* length)
   (CS_CHAR*->string char* length))
 (define translate-CS_LONGCHAR*
   translate-CS_CHAR*)
 (define (translate-CS_VARCHAR* varchar* length)
   (let* ((len (varchar-length varchar*))
          (str (make-string len)))
     (let lp ((idx 0))
       (if (= idx len)
           str
           (begin
             (string-set! str idx (varchar-string varchar* idx)))))))
 (define (translate-CS_DATETIME* datetime* length)
   (CS_DATETIME*->srfi-19-date
    datetime*
    (foreign-value "CS_DATETIME_TYPE" CS_INT)))
 (define (translate-CS_DATETIME4* datetime4* length)
   (CS_DATETIME*->srfi-19-date
    datetime4*
    (foreign-value "CS_DATETIME4_TYPE" CS_INT)))
 (define (translate-CS_TINYINT* tinyint* length)
   (CS_INT*->number tinyint* "CS_TINYINT" short))
 (define (translate-CS_SMALLINT* smallint* length)
   (CS_INT*->number smallint* "CS_SMALLINT" short))
 (define (translate-CS_INT* int* length)
   (CS_INT*->number int* "CS_INT" integer32))
 (define (translate-CS_BIGINT* bigint* length)
   (CS_INT*->number bigint* "CS_BIGINT" integer64))
 (define (cardinality integer base)
   (do ((power 0 (add1 power)))
       ((> (expt base power) integer) power)))
 (define (translate-CS_NUMERIC* numeric* length)
   (let ((maximum-number-length (foreign-value "CS_MAX_NUMLEN" int)))
     (let ((positive? (zero? (char->integer (numeric-array numeric* 0))))
           (base-256-digits
            (cardinality (expt 10 (char->integer
                                   (numeric-precision numeric*)))
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
 (define (translate-CS_FLOAT* float* length)
   ((foreign-safe-lambda*
     double
     (((c-pointer "CS_FLOAT") n))
     "C_return((double) *n);")
    float*))
 (define (translate-CS_REAL* real* length)
   ((foreign-safe-lambda*
     float
     (((c-pointer "CS_REAL") n))
     "C_return((float) *n);")
    real*))
 (define (translate-CS_MONEY* money* length)
   (/ (+ (* (money-high money*) (expt 2 32))
         (money-low money*))
      10000.0))
 (define (translate-CS_MONEY4* small-money* length)
   (/ (small-money-value small-money*) 10000.0))
 (define (translate-CS_TEXT* text* length)
   (CS_CHAR*->string text* length))
 (define translate-CS_IMAGE* translate-CS_TEXT*)

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

 (define-for-syntax (datatype->integer datatype)
   `(foreign-value ,(format "~a_TYPE" datatype) CS_INT))

 (define-for-syntax (datatype->size datatype)
   `(foreign-value ,(sprintf "sizeof(~a)" datatype) CS_INT))

 (define-for-syntax (datatype->make-type* datatype)
   (string->symbol (format "make-~a*" datatype)))

 (define-for-syntax (datatype->translate-type* datatype)
   (string->symbol (format "translate-~a*" datatype)))

 (define-syntax define-type-maker
   (syntax-rules ()
     ((_ type data-type-size constructor)
      (define (constructor #!optional (length 1))
        (let ((type* (allocate (* length data-type-size))))
          (unless type*
            (error 'constructor
                   (format "could not allocate ~a ~a(s)" length type)))
          (set-finalizer! type* free)
          type*)))))

 ;; Create a make-FOO* for every FOO in the datatypes list.
 (define-syntax define-make-types*/datatypes
   (er-macro-transformer
    (lambda (expression rename compare)
      `(begin . ,(map (lambda (type)
                        (let ((type-string (symbol->string type))
                              (size (datatype->size type))
                              (constructor (datatype->make-type* type)))
                          `(define-type-maker ,type-string ,size ,constructor)))
                      datatypes)))))

 (define-make-types*/datatypes)         ; Run immediately

 ;; Create an alist mapping of datatype integer value to datatype constructor.
 ;; So you'd get `((8 . ,make-CS_INT*) (9 . ,make-CS_REAL*) ...) where 8 is the
 ;; constant value of CS_INT_TYPE, 9 is the constant value of CS_REAL, etc.
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

 (define-datatype->make-type*/datatypes) ; Run immediately

 ;; Create a alist mapping of datatype integer values to datatype byte sizes.
 ;; So you'd get '((8 . 2) (9 . 2) ...) where 8 is the constant value of
 ;; CS_INT_TYPE, 9 is the constant value of CS_REAL, etc.
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
                                  `(,%unquote ,(datatype->size type))))
                          datatypes)))))))

 (define-datatype->type-size/datatypes) ; Run immediately

 ;; Create a alist mapping of datatype integer values to translater procedures.
 ;; So you'd get `((8 . ,translate-CS_INT*) (9 . ,translate-CS_REAL*) ...)
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

 (define-datatype->translate-type*/datatypes) ; Run immediately

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

 ;;;;;;;;;;;;;;;;;;;;;;
 ;;;; Error handling
 ;;;;;;;;;;;;;;;;;;;;;;

 ;; TODO: Eventually we won't need to pass a connection pointer anymore
 ;; but just a normal connection object.  This should make things safe.
 (define (error-on-retcode retcode connection* location message . arguments)
   (when connection*
     (apply check-server-errors! retcode connection* location message arguments)
     (apply check-client-errors! retcode connection* location message arguments))
   ;; Only drops down to here if we get no error from the checks
   ;; or when connection is #f
   (apply freetds-error location message retcode arguments))

 (define (error-on-non-success connection* thunk location message . arguments)
   (let ((retcode (thunk)))
     (unless (success? retcode)
       (apply error-on-retcode retcode connection* location message arguments))))

(define-syntax with-retcode-check
  (syntax-rules ()
    ((_ retcode connection* (loc message arguments ...) forms ...)
     (let-location ((retcode CS_INT))
       (receive results (begin forms ...)
         (if (success? retcode)
             (apply values results)
             (error-on-retcode retcode connection* 'loc message arguments ...)))))))

(define (check-server-errors! retcode conn loc . args)
  (and-let* ((res ((foreign-safe-lambda*
                    scheme-object (((c-pointer "CS_CONNECTION") conn))
                    "CS_SERVERMSG msg; CS_INT res;"
                    "C_word *str; C_word fin;"
                    "int i;"
                    ""
                    "for(i = 1; i > 0 /* No limit */ ; ++i) {"
                    "  res = ct_diag(conn, CS_GET, CS_SERVERMSG_TYPE, i, &msg);"
                    "  if (res == CS_NOMSG)"
                    "    C_return(C_SCHEME_FALSE);"
                    "  else if (res != CS_SUCCEED)"
                    "    C_return(C_fix(res));"
                    "  "
                    "  if (msg.severity == CS_SV_INFORM) /* Skip info-messages */"
                    "    continue;"
                    "  "
                    "  res = ct_diag(conn, CS_CLEAR, CS_SERVERMSG_TYPE, CS_UNUSED, NULL);"
                    "  "
                    "  if (res != CS_SUCCEED)"
                    "    C_return(C_fix(res));"
                    "  "
                    "  str = C_alloc(C_SIZEOF_STRING(msg.textlen));"
                    "  fin = C_string(&str, msg.textlen, msg.text);"
                    "  C_return(fin);"
                    "}"
                    "/* If we get here, something's seriously wrong (overflow) */"
                    "res = ct_diag(conn, CS_CLEAR, CS_SERVERMSG_TYPE, CS_UNUSED, NULL);"
                    ""
                    "if (res != CS_SUCCEED)"
                    "  C_return(C_fix(res));"
                    "C_return(C_fix(-1));") conn)))
    (if (number? res)
        (apply freetds-error 'ct_diag "could not retrieve error message" res args)
        (apply freetds-error loc res retcode args))))

(define (check-client-errors! retcode conn loc . args)
  (and-let* ((res ((foreign-safe-lambda*
                    scheme-object (((c-pointer "CS_CONNECTION") conn))
                    "CS_CLIENTMSG msg; CS_INT res;"
                    "C_word *str; C_word fin;"
                    "int i;"
                    ""
                    "for(i = 1; i > 0 /* No limit */ ; ++i) {"
                    "  res = ct_diag(conn, CS_GET, CS_CLIENTMSG_TYPE, 1, &msg);"
                    "  if (res == CS_NOMSG)"
                    "    C_return(C_SCHEME_FALSE);"
                    "  else if (res != CS_SUCCEED)"
                    "    C_return(C_fix(res));"
                    "  "
                    "  if (msg.severity == CS_SV_INFORM) /* Skip info-messages */"
                    "    C_return(C_SCHEME_FALSE);"
                    "  "
                    "  res = ct_diag(conn, CS_CLEAR, CS_CLIENTMSG_TYPE, CS_UNUSED, NULL);"
                    "  if (res != CS_SUCCEED)"
                    "    C_return(C_fix(res));"
                    "  "
                    "  str = C_alloc(C_SIZEOF_STRING(msg.msgstringlen));"
                    "  fin = C_string(&str, msg.msgstringlen, msg.msgstring);"
                    "  C_return(fin);"
                    "}"
                    "/* If we get here, something's seriously wrong (overflow) */"
                    "res = ct_diag(conn, CS_CLEAR, CS_SERVERMSG_TYPE, CS_UNUSED, NULL);"
                    ""
                    "if (res != CS_SUCCEED)"
                    "  C_return(C_fix(res));"
                    "C_return(C_fix(-1));") conn)))
    (if (number? res)
        (apply freetds-error 'ct_diag "could not retrieve error message" res args)
        (apply freetds-error loc res retcode args))))

 ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
 ;;;; Connection management
 ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

 (define (allocate-connection!)
   (with-retcode-check retcode #f (ct_con_alloc "failed to allocate a connection")
     ((foreign-lambda* (c-pointer "CS_CONNECTION") (((c-pointer "CS_CONTEXT") ctx)
                                                    ((c-pointer int) res))
                       "CS_CONNECTION *con;"
                       "*res = ct_con_alloc(ctx, &con);"
                       "C_return(con);")
      *app-context* (location retcode))))

 (define (drop-connection! connection*)
   (error-on-non-success
    connection*
    (lambda ()
      ((foreign-lambda CS_RETCODE "ct_con_drop" (c-pointer "CS_CONNECTION"))
       connection*))
    'ct_con_drop
    "failed to drop connection"))

 (define (connection-property connection* action property
                              buffer buffer-length
                              out-length*)
   (error-on-non-success
    connection*
    (lambda ()
      ((foreign-lambda CS_RETCODE
                       "ct_con_props"
                       (c-pointer "CS_CONNECTION") CS_INT CS_INT
                       scheme-pointer CS_INT (c-pointer "CS_INT"))
       connection* action property buffer buffer-length out-length*))
    'ct_con_props
    (format "failed to perform ~a on the property ~a" action property)))

 (define (connection-property-set! connection property value)
   (let ((set-prop! (lambda (buf len)
                      (connection-property (freetds-connection-ptr connection)
                                           (foreign-value "CS_SET" CS_INT)
                                           property buf len (null-pointer)))))
     ;; Readonly: CS_CHARSETCNV, CS_CON_STATUS, CS_EED_CMD, CS_ENDPOINT,
     ;;           CS_LOGIN_STATUS, CS_NOTIF_CMD, CS_PARENT_HANDLE,
     ;;           CS_SERVERNAME,
     ;;
     ;; Handled:
     ;;  - boolean: CS_ANSI_BINDS, CS_ASYNC_NOTIFS, CS_BULK_LOGIN,
     ;;             CS_DIAG_TIMEOUT, CS_DISABLE_POLL, CS_EXPOSE_FMTS,
     ;;             CS_EXTRA_INF, CS_HIDDEN_KEYS, CS_SEC_APPDEFINED,
     ;;             CS_SEC_CHALLENGE, CS_SEC_ENCRYPTION, CS_SEC_NEGOTIATE
     ;; - string: CS_HOSTNAME, CS_PASSWORD, CS_TRANSACTION_NAME, CS_USERNAME
     ;;
     ;; Not handled:
     ;; - CS_LOC_PROPERTY needs a CS_LOCALE property (only before connecting)
     ;; - CS_NETIO needs CS_SYNC_IO or CS_ASYNC_IO
     ;; - CS_PACKETSIZE needs an integer value (only before connecting)
     ;; - CS_TDS_VERSION needs a "symbolic version level"
     ;; - CS_TEXTLIMIT needs an integer value
     ;; - CS_USERDATA needs "user-allocated data" but we don't need it
     ;;
     ;; TODO: Instead of dispatching on type we should probably dispatch on
     ;;       property.  This ensures safety and also handles those strange
     ;;       cases CS_LOC_PROPERTY, CS_NETIO, CS_TDS_VERSION (and CS_USERDATA)
     ;;       It could make the API more Schemely by getting rid of the need to
     ;;       pass foreign property values, so we can use it from the REPL.
     (cond ((string? value)   (set-prop! value (string-length value)))
           ((boolean? value)  (set-prop! (if value CS_TRUE CS_FALSE) CS_UNUSED))
           ((fixnum? value)   (set-prop! value CS_UNUSED))
           #;((u8vector? value) (set-prop! value (u8vector-length value)))
           #;((blob? value)     (set-prop! value (blob-size value)))
           (else (error "Unrecognized property value type" property value)))))

 (define (connection-property-set-username! connection username)
   (connection-property-set! connection
                             (foreign-value "CS_USERNAME" CS_INT) username))

 (define (connection-property-set-password! connection password)
   (connection-property-set! connection
                             (foreign-value "CS_PASSWORD" CS_INT) password))

 (define (connect! connection* server)
   (error-on-non-success
    connection*
    (lambda ()
      ((foreign-lambda CS_RETCODE
                       "ct_connect"
                       (c-pointer "CS_CONNECTION")
                       c-string
                       CS_INT)
       connection*
       server
       (string-length server)))
    'ct_connect
    "failed to connect to server")
   (error-on-non-success
    connection*
    (lambda ()
      ((foreign-lambda* CS_RETCODE (((c-pointer "CS_CONNECTION") conn))
                        "C_return(ct_diag(conn, CS_INIT, CS_UNUSED, "
                        "                 CS_UNUSED, NULL));") connection*))
    'ct_connect
    "could not initialize error handling"))

 (define (use! connection database)
   (result-cleanup!                     ; premature optimization? ;)
    (send-query connection
                ;; needs to be escaped!
                (format "USE ~a" database))))

 (define-record freetds-connection ptr)

 (define connection? freetds-connection?)

 (define (connection-reset! conn)
   (cancel* (freetds-connection-ptr conn)
            #f
            (foreign-value "CS_CANCEL_ALL" CS_INT)))

 (define (make-connection host username password #!optional database)
   (let ((ptr (allocate-connection!)))
     (let ((connection (make-freetds-connection ptr)))
       (connection-property-set-username! connection username)
       (connection-property-set-password! connection password)
       (connect! ptr host)
       (set-finalizer! connection connection-close)
       (when database (use! connection database))
       connection)))

 (define (connection-close* connection*)
   (error-on-non-success
    #f                                  ; Not anymore?
    (lambda ()
      ((foreign-lambda CS_RETCODE
                       "ct_close"
                       (c-pointer "CS_CONNECTION")
                       CS_INT)
       ;; TODO: CS_FORCE_CLOSE might be necessary in some cases. Perhaps try
       ;; graceful disconnect first, and force it only if that fails?
       connection* CS_UNUSED))
    'connection-close
    "failed to close connection"))

 (define (connection-close connection)
   (and-let* ((ptr (freetds-connection-ptr connection)))
     (connection-close* ptr)
     (freetds-connection-ptr-set! connection #f) ; Mark as closed
     (drop-connection! ptr))
   (void))

 (define call-with-connection
   (case-lambda
    ((host username password procedure)
     (call-with-connection host username password #f procedure))
    ((host username password database procedure)
     (let ((connection #f))
       (dynamic-wind
           (lambda ()
             (set! connection
                   (make-connection host username password database)))
           (lambda ()
             (procedure connection))
           (lambda ()
             (connection-close connection)))))))

 (define (connection-open? connection)
   (pointer? (freetds-connection-ptr connection)))

 ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
 ;;;; Command/query management
 ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

 (define (allocate-command! connection)
   (let ((connection* (freetds-connection-ptr connection)))
    (with-retcode-check retcode connection* (ct_cmd_alloc "failed to allocate command")
      ((foreign-lambda* (c-pointer "CS_COMMAND") (((c-pointer "CS_CONNECTION") ctx)
                                                  ((c-pointer int) res))
                        "CS_COMMAND *cmd;"
                        "*res = ct_cmd_alloc(ctx, &cmd);"
                        "C_return(cmd);")
       connection* (location retcode)))))

 (define (command! connection command* type buffer* option)
   (error-on-non-success
    (freetds-connection-ptr connection)
    (lambda ()
      ((foreign-lambda CS_RETCODE "ct_command" (c-pointer "CS_COMMAND")
                       CS_INT #;(const (c-pointer "CS_VOID"))
                       (const c-string) CS_INT CS_INT)
       command* type buffer* (foreign-value "CS_NULLTERM" CS_INT) option))
    'ct_command
    (format "could not create command structure for \"~a\"" buffer*)))

 (define (send! command* connection)
   (error-on-non-success
    (freetds-connection-ptr connection)
    (lambda ()
      ((foreign-lambda CS_RETCODE "ct_send" (c-pointer "CS_COMMAND")) command*))
    'ct_send
    "failed to send command"))

 (define (add-param! connection command* param)
   (let* ((fmt* (make-CS_DATAFMT*))
          (datalen 1) ;; Only used for char types
          ;; TODO: Figure out a way to make this sane
          (mem* (cond
                 ((string? param)
                  (when (> (string-length param) 255)
                    (error "Cannot store strings > 255 characters!"))
                  (data-format-datatype-set!
                   fmt* (foreign-value "CS_CHAR_TYPE" CS_INT))
                  (data-format-max-length-set! fmt* (string-length param))
                  (set! datalen (string-length param))
                  ((foreign-lambda* c-pointer
                                    ((scheme-pointer s) (int len))
                                    "CS_CHAR *res;"
                                    "res = malloc(sizeof(CS_CHAR) * len);"
                                    "if (res == NULL)"
                                    "  C_return(res);"
                                    "memcpy(res, s, len);"
                                    "C_return(res);") param (string-length param)))
                 ((fixnum? param)
                  (data-format-datatype-set!
                   fmt* (foreign-value "CS_INT_TYPE" CS_INT))
                  ((foreign-lambda* c-pointer
                                    ((int i))
                                    "CS_INT *res;"
                                    "res = malloc(sizeof(CS_INT));"
                                    "if (res == NULL)"
                                    "  C_return(res);"
                                    "*res = i;"
                                    "C_return(res);") param))
                 ((flonum? param)
                  (data-format-datatype-set!
                   fmt* (foreign-value "CS_FLOAT_TYPE" CS_INT))
                  ((foreign-lambda* c-pointer
                                    ((double f))
                                    "CS_FLOAT *res;"
                                    "res = malloc(sizeof(CS_FLOAT));"
                                    "if (res == NULL)"
                                    "  C_return(res);"
                                    "*res = f;"
                                    "C_return(res);") param))
                 ((sql-null? param)
                  ;; Any value is ok, but if we don't set *something*,
                  ;; ct_send will complain
                  (data-format-datatype-set!
                   fmt* (foreign-value "CS_INT_TYPE" CS_INT))
                  #t)
                 ((date? param)
                  (data-format-datatype-set!
                   fmt* (foreign-value "CS_DATETIME_TYPE" CS_INT))
                  ((foreign-lambda* c-pointer
                                    (((c-pointer "CS_CONTEXT") ctx)
                                     (c-string s)
                                     ((c-pointer "CS_DATAFMT") fmt))
                                    "CS_CHAR *tmp = (CS_CHAR *)s;"
                                    "CS_DATAFMT src;"
                                    "CS_DATETIME *res;"
                                    "CS_RETCODE ret;"
                                    "res = malloc(sizeof(CS_DATETIME));"
                                    "if (res == NULL)"
                                    "  C_return(res);"
                                    "src.namelen = 0;"
                                    "src.datatype = CS_CHAR_TYPE;"
                                    "src.maxlength = strlen(s);"
                                    "ret = cs_convert(ctx, &src, s, fmt, res, NULL);"
                                    "if (ret != CS_SUCCEED) {"
                                    "  free(res);"
                                    "  C_return(NULL);"
                                    "}"
                                    "C_return(res);")
                   ;; We can't use ~4 or ~5 because FreeTDS chokes on the 'T'
                   ;; time separator.  Also, it doesn't grok timezones.
                   *app-context* (date->string param "~1 ~3") fmt*))
                 (else (error "Unknown parameter type" param)))))
     (data-format-name-length-set! fmt* 0) ; All params are nameless
     (data-format-status-set! fmt* (foreign-value "CS_INPUTVALUE" CS_INT))
     (unless mem* (error "Could not allocate memory for parameter" param))
     ;; This shouldn't really be necessary
     (set-finalizer! fmt* free-CS_DATAFMT*)
     ;; Set up the parameter pointer's memory to be cleaned up when the command
     ;; is cleaned up (but no earlier!) -- it's not a pointer when sql-null
     (when (pointer? mem*) (set-finalizer! command* (lambda (c) (free mem*))))
     (error-on-non-success
      (freetds-connection-ptr connection)
      (lambda ()
        ((foreign-lambda CS_RETCODE
                         "ct_param"
                         (c-pointer "CS_COMMAND")
                         (c-pointer "CS_DATAFMT")
                         (c-pointer "CS_VOID")
                         CS_INT
                         CS_SMALLINT)
         command* fmt* (and (pointer? mem*) mem*) datalen (if (sql-null? param) -1 0)))
      'ct_param
      "failed to add parameter to command"
      command*
      param)))

 ;; Convenience wrapper
 (define (send-query connection query . parameters)
   (send-query* connection query parameters))

 (define (send-query* connection query parameters)
   (let ((command* (allocate-command! connection)))
     (command! connection
               command*
               (foreign-value "CS_LANG_CMD" CS_INT)
               query
               CS_UNUSED)
     (for-each (lambda (p) (add-param! connection command* p)) parameters)
     (send! command* connection)
     ;; TODO: Memory leak when send! or add-param! fails
     (receive (bound-vars rows)
       (consume-results-and-bind-variables connection command*)
       (let* ((column-names (map freetds-bound-variable-name bound-vars))
              (result (make-freetds-result command* column-names rows)))
         (set-finalizer! result result-cleanup!)
         result))))

 (define (drop-command! command*)
   (error-on-non-success
    #f
    (lambda ()
      ((foreign-lambda CS_RETCODE "ct_cmd_drop" (c-pointer "CS_COMMAND"))
       command*))
    'ct_cmd_drop
    "failed to drop command"))

 ;; Description from Open Client Client-Library/C Reference Manual:
 ;; "For CS_CANCEL_CURRENT cancels, connection must be NULL.
 ;;  For CS_CANCEL_ATTN and CS_CANCEL_ALL cancels, one of
 ;;  connection or cmd must be NULL. If connection is supplied and cmd
 ;;  is NULL, the cancel operation applies to all commands pending
 ;;  for this connection."
 (define cancel*
   (foreign-lambda CS_RETCODE
                   "ct_cancel"
                   (c-pointer "CS_CONNECTION")
                   (c-pointer "CS_COMMAND")
                   CS_INT))

 ;;;;;;;;;;;;;;;;;;;;;;;;;
 ;;;; Result processing
 ;;;;;;;;;;;;;;;;;;;;;;;;;

 (define-record freetds-result command-ptr column-names rows)
 (define result? freetds-result?)

 (define (cancel-command! cmd*)
   (cancel* #f cmd* (foreign-value "CS_CANCEL_ALL" CS_INT)))

 (define (result-cleanup! result)
   (and-let* ((command* (freetds-result-command-ptr result)))
     (cancel-command! command*)
     (drop-command! command*)
     (freetds-result-command-ptr-set! result #f)
     (freetds-result-column-names-set! result #f)
     (freetds-result-rows-set! result #f))
   (void))

 ;; The results returned by ct_results are not complete result sets,
 ;; but just a descriptive structure with info on number and types of
 ;; columns etc.  Row-fetch retreives the next value from the server.
 (define (results! connection* command*)
   (let-location ((rettype CS_INT))
     (let* ((results (foreign-lambda CS_RETCODE
                                     "ct_results"
                                     (c-pointer "CS_COMMAND")
                                     (c-pointer "CS_INT")))
            (retcode (results command* (location rettype))))
       (values retcode rettype))))

 ;; Limited to integer types
 ;; (ie, not to be used for CS_BROWSE_INFO, CS_MSGTYPE, CS_ORDERBY_COLS)
 (define (results-info connection* command* type)
   (let-location ((result int))
     (error-on-non-success
      connection*
      (lambda ()
        ((foreign-lambda CS_RETCODE
                         "ct_res_info"
                         (c-pointer "CS_COMMAND")
                         CS_INT
                         (c-pointer "CS_VOID")
                         CS_INT
                         (c-pointer int))
         command* type (location result) CS_UNUSED #f))
      'ct_res_info
      "Could not fetch result information")
     result))

 (define (describe! connection* command* item data-format*)
   (error-on-non-success
    connection*
    (lambda ()
      ((foreign-lambda CS_RETCODE
                       "ct_describe"
                       (c-pointer "CS_COMMAND") CS_INT (c-pointer "CS_DATAFMT"))
       command* item data-format*))
    'ct_describe
    "failed to describe column"))

 (define (bind! connection*
                command*
                item
                data-format*
                buffer*
                indicator*)
   (error-on-non-success
    connection*
    (lambda ()
      ((foreign-lambda CS_RETCODE
                       "ct_bind"
                       (c-pointer "CS_COMMAND") CS_INT
                       (c-pointer "CS_DATAFMT") (c-pointer "CS_VOID")
                       (c-pointer "CS_INT")     (c-pointer "CS_SMALLINT"))
       command* item data-format* buffer* #f indicator*))
    'ct_bind
    "failed to bind result value"))

 (define (name-from-data-format data-format*)
   (let* ((len (data-format-name-length data-format*))
          (str (make-string len)))
     (let lp ((idx 0))
       (if (= idx len)
           (string->symbol str)
           (begin
             (string-set! str idx (data-format-name data-format* idx))
             (lp (add1 idx)))))))

 (define-record freetds-bound-variable value indicator translator length name)

 (define (make-bound-variables connection* command*)
   (list-tabulate
    ;; Fetch number of columns
    (results-info connection* command* (foreign-value "CS_NUMDATA" CS_INT))
    (lambda (column)
      (let ((data-format* (make-CS_DATAFMT*)))
        ;; This shouldn't really be necessary
        (set-finalizer! data-format* free-CS_DATAFMT*)
        (describe! connection* command* (add1 column) data-format*)
        ;; let's have a table here for modifying,
        ;; if necessary, the data-format*.
        (let ((datatype (data-format-datatype data-format*)))
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
          (let ((make-type* (alist-ref datatype datatype->make-type*))
                (type-size (alist-ref datatype datatype->type-size))
                (translate-type* (alist-ref datatype datatype->translate-type*)))
            (unless (and make-type* type-size translate-type*)
              (error "Encountered an unknown datatype in result set!" datatype))
            (let* ((length (inexact->exact
                            (ceiling
                             (/ (data-format-max-length data-format*)
                                type-size))))
                   (value* (make-type* length))
                   (indicator* (make-CS_SMALLINT* 1))
                   (name (name-from-data-format data-format*)))
              (and (bind! connection* command* (+ column 1)
                          data-format* value* indicator*)
                   (make-freetds-bound-variable
                    value* indicator* translate-type* length name)))))))))

 ;; Currently this assumes a command can only return one result
 ;; (actually it returns only the last; anything else is consumed but discarded)
 (define (consume-results-and-bind-variables connection command*)
   (let loop ((bound-variables #f)
              (resultset #f))
     (let*-values (((connection*) (freetds-connection-ptr connection))
                   ((result-status result-type) (results! connection* command*)))
       (cond
        ((success? result-status)
         ;; need to deal with CS_ROW_RESULT, CS_END_RESULTS; and
         ;; possibly CS_CMD_SUCCEED, CS_CMD_FAIL, ...
         (cond ((or (row-format-result? result-type) (row-result? result-type))
                (let ((bound-vars (make-bound-variables connection* command*)))
                  (loop bound-vars
                        (consume-resultset connection* command* bound-vars))))
               ((or (command-done? result-type) (command-succeed? result-type))
                ;; Must continue because there might be more resultsets and we
                ;; _must_ consume END_RESULTS.  Otherwise, the connection gets
                ;; "stuck" until the command is dropped.
                (loop bound-variables resultset))
               (else
                (check-server-errors! result-type connection*
                                      'consume-results-and-bind-variables)
                ;; If no server errors, something's up.
                ;; TODO: Maybe we need to clean up?
                (freetds-error 'consume-results-and-bind-variables
                               "ct_results returned a bizarre result type"
                               result-type))))
        ((fail? result-status)
         (let ((retcode (cancel-command! command*)))
           (cond ((fail? retcode)
                  (connection-close connection) ; Is this neccessary?
                  (freetds-error 'consume-results-and-bind-variables
                                 (conc "ct_results and ct_cancel failed, "
                                       "prompting the connection to close")
                                 retcode))
            (else (check-server-errors! result-type connection*
                                        'consume-results-and-bind-variables)
                  ;; If no server errors, something's up.
                  (freetds-error 'consume-results-and-bind-variables
                                 "ct_results failed, cancelling command"
                                 retcode)))))
        ((end-results? result-status)
         ;; This is here to work around a bug in FreeTDS (0.82); in some cases,
         ;; it returns no error code when an invalid query was sent.
         ;; See http://lists.ibiblio.org/pipermail/freetds/2007q3/022269.html
         (check-server-errors! result-type connection*
                               'consume-results-and-bind-variables)
         (values bound-variables resultset))
        (else
         (freetds-error 'consume-results-and-bind-variables
                        "ct_results returned a bizarre result status"
                        result-status))))))

 ;; It's unfortunate that we need to call list->vector, but this is neccessary
 ;; because ct_res_info doesn't return a useful result for CS_ROW_COUNT until
 ;; all result values have been read out, at which point we have a list already.
 (define (consume-resultset connection* command* bound-vars)
   (let loop ((rows '())
              (row (row-fetch connection* command* bound-vars)))
     (if (not row)
         (list->vector (reverse! rows))
         (loop (cons row rows) (row-fetch connection* command* bound-vars)))))

 ;; Fetch doesn't return anything useful, it fills in previously bound variables
 (define (fetch! command*)
   ((foreign-lambda CS_INT "ct_fetch"
                    (c-pointer "CS_COMMAND")
                    CS_INT CS_INT CS_INT (c-pointer "CS_INT"))
    command* CS_UNUSED CS_UNUSED CS_UNUSED #f))

 (define (row-fetch connection* command* bound-vars)
   (let ((retcode (fetch! command*)))
     (cond ((or (success? retcode)
                (row-fail? retcode))
            (map (lambda (var)
                   (if (null-indicator? (freetds-bound-variable-indicator var))
                       (sql-null)
                       ((freetds-bound-variable-translator var)
                        (freetds-bound-variable-value var)
                        (freetds-bound-variable-length var))))
                 bound-vars))
           ((fail? retcode)
            ;; cancel
            ;; fail again -> close
            (freetds-error 'row-fetch "fetch! returned CS_FAIL" retcode))
           ((end-data? retcode) #f)
           (else
            (freetds-error 'row-fetch "fetch! returned unknown retcode" retcode)))))

 (define (column-name result #!optional (column-number 0))
   (list-ref (freetds-result-column-names result) column-number))

 (define column-names freetds-result-column-names)

 (define (result-row result #!optional (row-number 0))
   (vector-ref (freetds-result-rows result) row-number))

 (define (result-column result #!optional (column-number 0))
   (let ((rows (freetds-result-rows result)))
     (let loop ((column-values '())
                (row-number (vector-length rows)))
       (if (zero? row-number)
           column-values
           (let ((row (vector-ref rows (sub1 row-number))))
             (loop (cons (list-ref row column-number) column-values)
                   (sub1 row-number)))))))

 (define (result-value result #!optional (column 0) (row 0))
   (list-ref (result-row result row) column))

 (define (result-row/alist result #!optional (row-number 0))
   (and-let* ((row (result-row result row-number))
              (names (column-names result)))
     (map cons names row)))

 (define (result-values result)
   (vector->list (freetds-result-rows result)))

 (define (result-values/alist result)
   (map (lambda (row)
          (map cons (column-names result) row))
        (result-values result)))

 (define (call-with-result-set connection query . rest-args)
   ;; TODO: This is not too efficient
   (receive (params last)
     (split-at rest-args (sub1 (length rest-args)))
     (let ((process-result (car last))
           (result (send-query* connection query params)))
       (dynamic-wind
           void
           (lambda () (process-result result))
           (lambda () (result-cleanup! result))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; High-level interface
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define (make-result-fold item-count extract-item)
  (lambda (kons knil result)
   (let ((items (item-count result)))
     (let loop ((seed knil)
                (item 0))
       (if (= item items)
           seed
           (loop (kons (extract-item result item) seed) (add1 item)))))))

(define (column-count result)
  (length (freetds-result-column-names result)))
(define (row-count result)
  (vector-length (freetds-result-rows result)))

(define row-fold (make-result-fold row-count result-row))
(define (row-fold* kons knil result)
  (row-fold (lambda (values seed)
              (apply kons (append values (list seed)))) knil result))

(define column-fold (make-result-fold column-count result-column))
(define (column-fold* kons knil result)
  (column-fold (lambda (values seed)
                 (apply kons (append values (list seed)))) knil result))


(define (make-result-fold-right item-count extract-item)
  (lambda (kons knil result)
    (let loop ((seed knil)
               (item (item-count result)))
      (if (= item 0)
          seed
          (loop (kons (extract-item result (sub1 item)) seed) (sub1 item))))))

(define row-fold-right (make-result-fold-right row-count result-row))
(define (row-fold-right* kons knil result)
  (row-fold-right (lambda (values seed)
                    (apply kons (append values (list seed)))) knil result))

(define column-fold-right (make-result-fold-right column-count result-column))
(define (column-fold-right* kons knil result)
  (column-fold-right (lambda (values seed)
                       (apply kons (append values (list seed)))) knil result))


(define (row-for-each proc result)
  (row-fold (lambda (values seed) (proc values)) #f result)
  (void))
(define (row-for-each* proc result)
  (row-fold (lambda (values seed) (apply proc values)) #f result)
  (void))

(define (column-for-each proc result)
  (column-fold (lambda (values seed) (proc values)) #f result)
  (void))
(define (column-for-each* proc result)
  (column-fold (lambda (values seed) (apply proc values)) #f result)
  (void))

;; Like regular Scheme map, the order in which the procedure is applied is
;; undefined.  We make good use of that by traversing the resultset from
;; the end back to the beginning, thereby avoiding a reverse! on the result.
(define (row-map proc res)
  (row-fold-right (lambda (row lst) (cons (proc row) lst)) '() res))
(define (row-map* proc res)
  (row-fold-right (lambda (row lst) (cons (apply proc row) lst)) '() res))
(define (column-map proc res)
  (column-fold-right (lambda (col lst) (cons (proc col) lst)) '() res))
(define (column-map* proc res)
  (column-fold-right (lambda (col lst) (cons (apply proc col) lst)) '() res))
)
