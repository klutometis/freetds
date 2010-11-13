#!/usr/bin/env chicken-scheme
(use format
     foreigners
     lolevel
     debug
     srfi-1
     srfi-13
     miscmacros)

(include "test-freetds-secret.scm")

(foreign-declare "#include <ctpublic.h>")

(define-foreign-type cs-context* (c-pointer "CS_CONTEXT"))
(define-foreign-type cs-connection* (c-pointer "CS_CONNECTION"))
(define-foreign-type cs-command* (c-pointer "CS_COMMAND"))
(define-foreign-type cs-client-message* (c-pointer "CS_CLIENTMSG"))
(define-foreign-type cs-server-message* (c-pointer "CS_SERVERMSG"))
(define-foreign-type cs-void* (c-pointer "CS_VOID"))
(define-foreign-type cs-char* (c-pointer "CS_CHAR"))
(define-foreign-type cs-int integer32)
(define-foreign-type cs-retcode integer32)

(define-foreign-variable cs-version-100 cs-int "CS_VERSION_100")
(define-foreign-variable cs-fail cs-retcode "CS_FAIL")
(define-foreign-variable cs-succeed cs-retcode "CS_SUCCEED")
(define-foreign-variable cs-force-exit int "CS_FORCE_EXIT")
(define-foreign-variable cs-set int "CS_SET")
(define-foreign-variable cs-message-callback int "CS_MESSAGE_CB")
(define-foreign-variable cs-client-message-callback int "CS_CLIENTMSG_CB")
(define-foreign-variable cs-server-message-callback int "CS_SERVERMSG_CB")
(define-foreign-variable cs-unused int "CS_UNUSED")
(define-foreign-variable cs-nullterm int "CS_NULLTERM")
(define-foreign-variable cs-username int "CS_USERNAME")
(define-foreign-variable cs-password int "CS_PASSWORD")
(define-foreign-variable cs-language-command int "CS_LANG_CMD")

(define cs-ctx-alloc
  (foreign-lambda cs-retcode cs_ctx_alloc cs-int (c-pointer cs-context*)))

(define ct-init
  (foreign-lambda cs-retcode ct_init cs-context* cs-int))

(define cs-config
  (foreign-lambda cs-retcode
                  cs_config
                  cs-context*
                  cs-int
                  cs-int
                  cs-void*
                  cs-int
                  (c-pointer cs-int)))

(define ct-callback
  (foreign-lambda cs-retcode
                  ct_callback
                  cs-context*
                  cs-connection*
                  cs-int
                  cs-int
                  cs-void*))

(define ct-exit
  (foreign-lambda cs-retcode ct_exit cs-context* cs-int))

(define cs-ctx-drop
  (foreign-lambda cs-retcode cs_ctx_drop cs-context*))

(define ct-con-alloc
  (foreign-lambda cs-retcode
                  ct_con_alloc
                  cs-context*
                  (c-pointer cs-connection*)))

(define ct-con-props
  (foreign-lambda cs-retcode
                  ct_con_props
                  cs-connection*
                  cs-int
                  cs-int
                  ;; cs-void*
                  c-string
                  cs-int
                  (c-pointer cs-int)))

(define ct-connect
  (foreign-lambda cs-retcode
                  ct_connect
                  cs-connection*
                  ;; cs-char*
                  c-string
                  cs-int))

(define ct-cmd-alloc
  (foreign-lambda cs-retcode
                  ct_cmd_alloc
                  cs-connection*
                  (c-pointer cs-command*)))

(define ct-command
  (foreign-lambda cs-retcode
                  ct_command
                  cs-command*
                  int
                  (const cs-void*)
                  cs-int
                  cs-int))

(define ct-send
  (foreign-lambda cs-retcode
                  ct_send
                  cs-command*))

(define ct-results
  (foreign-lambda cs-retcode
                  ct_results
                  cs-command*
                  (c-pointer cs-int)))

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
  (= retcode cs-succeed))

;;; Should rather be called: `error-on-non-success'.
(define (error-on-failure thunk location message . arguments)
  (let ((retcode (thunk)))
    (if (not (success? retcode))
        (apply freetds-error location message retcode arguments))))

(define-external (cs_message_callback (cs-context* context)
                                      (cs-client-message* message))
    cs-retcode
  (freetds-error 'callback "holy shit!"))

(define-external (cs_client_message_callback (cs-context* context) 
                                             (cs-connection* connection)
                                             (cs-client-message* message))
    cs-retcode
  (freetds-error 'callback "holy shit!"))

(define-external (cs_server_message_callback (cs-context* context) 
                                             (cs-connection* connection)
                                             (cs-server-message* message))
  cs-retcode
  (freetds-error 'callback "holy shit!"))

(define-foreign-record-type
  (CS_DATAFMT CS_DATAFMT)
  ;; 132 == CS_MAX_NAME
  (char (name 132) data-format-name)
  (int namelen data-format-name-length data-format-name-length-set!)
  (int datatype data-format-datatype data-format-datatype-set!)
  (int format data-format-format data-format-format-set!)
  (int maxlength data-format-max-length data-format-max-length-set!)
  (int scale data-format-scale data-format-scale-set!)
  (int precision data-format-precision data-format-precision-set!)
  (int status data-format-status data-format-status-set!)
  (int count data-format-count data-format-count-set!)
  (int usertype data-format-usertype data-format-usertype-set!)
  ((c-pointer "CS_LOCALE") locale data-format-locale data-format-locale-set!))

(define-foreign-record-type
  coldata
  (char (value 1024) column-data-value column-data-value-set!)
  (int valuelen column-data-valuelen column-data-valuelen-set!)
  (int indicator column-data-indicator column-data-indicator-set!))

(foreign-declare "typedef struct coldata * coldata_t;")

(define (char-null? char)
  (char=? char #\nul))

(define char-vector->string
  (case-lambda
   ((char-vector char-ref)
    (char-vector->string char-ref +inf))
   ((char-vector char-ref max-length)
    ;; lazy to do the reversal; should we append instead of cons?
    (define (chars->string chars)
      (string-reverse (list->string chars)))
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

(let-location ((context cs-context*))
  (error-on-failure
   (lambda ()
     (cs-ctx-alloc cs-version-100 (location context)))
   'cs_ctx_alloc
   "failed to allocate context")

  (error-on-failure
   (lambda ()
     (ct-init context cs-version-100))
   'ct_init
   "failed to initialize context")

  #;(error-on-failure
   (lambda ()
     (cs-config context
                cs-set
                cs-message-callback
                cs_message_callback
                cs-unused
                (null-pointer)))
   'cs_config
   "failed to set client-server-library message-callback")

  #;(error-on-failure
   (lambda ()
     (ct-callback context
                  (null-pointer)
                  cs-set
                  cs-client-message-callback
                  cs_client_message_callback))
   'ct_callback
   "failed to set client-library message-callback")

  #;(error-on-failure
   (lambda ()
     (ct-callback context
                  (null-pointer)
                  cs-set
                  cs-server-message-callback
                  (location cs_server_message_callback)))
   'ct_callback
   "failed to set server message-callback")

  (let-location ((connection cs-connection*))
    (error-on-failure
     (lambda ()
       (ct-con-alloc context
                     (location connection)))
     'ct_con_alloc
     "failed to allocate a connection")

    (error-on-failure
     (lambda ()
       (ct-con-props connection
                     cs-set
                     cs-username
                     username
                     cs-nullterm
                     (null-pointer)))
     'ct_con_props
     "failed to set the username")

    (error-on-failure
     (lambda ()
       (ct-con-props connection
                     cs-set
                     cs-password
                     password
                     cs-nullterm
                     (null-pointer)))
     'ct_con_props
     "failed to set the password")

    (error-on-failure
     (lambda ()
       (ct-connect connection
                   server
                   (string-length server)))
     'ct_connect
     "failed to connect to server")

    (let-location ((command cs-command*))
      (error-on-failure
       (lambda ()
         (ct-cmd-alloc connection
                       (location command)))
       'ct_cmd_alloc
       "failed to allocate command")

      (error-on-failure
       (lambda ()
         (ct-command command
                     cs-language-command
                     (location "SELECT refdate FROM SYSOBJECTS WHERE XTYPE = 'U';")
                     cs-nullterm
                     cs-unused))
       'ct_command
       "failed to issue command")

      (error-on-failure
       (lambda ()
         (ct-send command))
       'ct_send
       "failed to send command")

      (let-location ((result-type int))
        (let more-results ((result-status
                            (ct-results command (location result-type))))
          (if (success? result-status)
              (begin
                #;(debug 'oh-wirklich)
                (select result-type
                  (((foreign-value "CS_ROW_RESULT" int))
                   (let-location ((column-count int))
                     (error-on-failure
                      (lambda ()
                        ((foreign-lambda cs-retcode
                                         "ct_res_info"
                                         cs-command*
                                         cs-int
                                         cs-void*
                                         cs-int
                                         (c-pointer cs-int))
                         command
                         (foreign-value "CS_NUMDATA" int)
                         (location column-count)
                         (foreign-value "CS_UNUSED" int)
                         (null-pointer)))
                      'ct_res_info
                      "failed to count columns")
                     (list-tabulate
                      column-count
                      (lambda (column)
                        (let-location ((format (c-pointer "CS_DATAFMT")))
                          (error-on-failure
                           (lambda ()
                             ((foreign-lambda cs-retcode
                                              "ct_describe"
                                              cs-command*
                                              cs-int
                                              (c-pointer "CS_DATAFMT"))
                              command
                              (+ column 1)
                              (location format)))
                           'ct_describe
                           "failed to describe column")
                          (data-format-datatype-set!
                           (location format)
                           (foreign-value "CS_CHAR_TYPE" int))
                          (data-format-format-set!
                           (location format)
                           (foreign-value "CS_FMT_NULLTERM" int))
                          (data-format-max-length-set!
                           (location format)
                           1024)
                          (let ((value (make-string 1024)))
                            (let-location (#;(value c-string)
                                           #;(value int)
                                           (valuelen int)
                                           (indicator int)
                                           #;(column-data (c-pointer "coldata_t")))
                              #;(set! value (allocate (* (foreign-value "sizeof(char)" int) 1024)))
                              ;; (debug (foreign-value "sizeof(coldata_t)" int))
                              ;; (set! column-data (allocate (foreign-value "sizeof(coldata_t)" int)))
                              ;; (column-data-valuelen-set! column-data 0)
                              ;; (column-data-indicator-set! column-data 0)
                              (error-on-failure
                               (lambda ()
                                 ((foreign-lambda cs-retcode
                                                  "ct_bind"
                                                  cs-command*
                                                  int
                                                  (c-pointer "CS_DATAFMT")
                                                  cs-void*
                                                  (c-pointer "CS_INT")
                                                  (c-pointer "CS_SMALLINT"))
                                  command
                                  (+ column 1)
                                  (location format)
                                  (location value)
                                  (location valuelen)
                                  (location indicator)))
                               'ct_bind
                               "failed to bind statement")
                              (let-location ((rows-read int))
                                (while (success?
                                        ((foreign-lambda cs-retcode
                                                         "ct_fetch"
                                                         cs-command*
                                                         int
                                                         int
                                                         int
                                                         (c-pointer int))
                                         command
                                         (foreign-value "CS_UNUSED" int)
                                         (foreign-value "CS_UNUSED" int)
                                         (foreign-value "CS_UNUSED" int)
                                         (location rows-read)))
                                       (debug (string-trim-right value))))
                              #;(debug 'oh-jes)
                              #;(free value)
                              #;(debug (char-vector->string
                              column-data
                              column-data-value
                              (column-data-valuelen column-data)))
                            #;(free column-data)))
                          #;(let ((name
                        (char-vector->string
                          (location format)
                          data-format-name
                          (data-format-name-length (location format))))
                      (type
                          (data-format-datatype (location format))))
                     (cons name type))))))))
                (more-results (ct-results command (location result-type))))
              (begin
                ((foreign-lambda cs-retcode
                                 "ct_cmd_drop"
                                 cs-command*)
                 command)
                ((foreign-lambda cs-retcode
                                 "ct_close"
                                 cs-connection*
                                 int)
                 connection
                 (foreign-value "CS_UNUSED" int))
                ((foreign-lambda cs-retcode
                                 "ct_con_drop"
                                 cs-connection*)
                 connection)
                ((foreign-lambda cs-retcode
                                 "ct_exit"
                                 cs-context*
                                 int)
                 context
                 (foreign-value "CS_UNUSED" int))
                ((foreign-lambda cs-retcode
                                 "cs_ctx_drop"
                                 cs-context*)
                 context))))))))
