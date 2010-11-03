(use format foreigners lolevel)
(include "test-freetds-secret.scm")

(foreign-declare "#include <ctpublic.h>")

(define-foreign-type cs-context* (c-pointer "CS_CONTEXT"))
(define-foreign-type cs-connection* (c-pointer "CS_CONNECTION"))
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

(define (error-on-failure thunk location message . arguments)
  (let ((retcode (thunk)))
    (if (not (= retcode cs-succeed))
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

(let-location ((context cs-context*))
  #;(define-external context cs-context* (null-pointer))
  (error-on-failure
   (lambda ()
     (cs-ctx-alloc cs-version-100 #$context))
   'context-allocation
   "failed to allocate context")

  (error-on-failure
   (lambda ()
     (ct-init context cs-version-100))
   'context-initialization
   "failed to initialize context")

  #;(error-on-failure
   (lambda ()
     (cs-config context
                cs-set
                cs-message-callback
                cs_message_callback
                cs-unused
                (null-pointer)))
   'set-cs-library-message-callback
   "failed to set client-server-library message-callback")

  #;(error-on-failure
   (lambda ()
     (ct-callback context
                  (null-pointer)
                  cs-set
                  cs-client-message-callback
                  cs_client_message_callback))
   'set-client-library-message-callback
   "failed to set client-library message-callback")

  #;(error-on-failure
   (lambda ()
     (ct-callback context
                  (null-pointer)
                  cs-set
                  cs-server-message-callback
                  cs_server_message_callback))
   'set-server-message-callback
   "failed to set server message-callback")

  (let-location ((connection cs-connection*))
    #;(define-external connection cs-context* (null-pointer))
    (error-on-failure
     (lambda ()
       (ct-con-alloc context
                     #$connection))
     'allocate-connection
     "failed to allocate a connection")

    (error-on-failure
     (lambda ()
       (ct-con-props connection
                     cs-set
                     cs-username
                     username
                     cs-nullterm
                     (null-pointer)))
     'set-username
     "failed to set the username")

    (error-on-failure
     (lambda ()
       (ct-con-props connection
                     cs-set
                     cs-password
                     password
                     cs-nullterm
                     (null-pointer)))
     'set-password
     "failed to set the password")

    (error-on-failure
     (lambda ()
       (ct-connect connection
                   server
                   (string-length server)))
     'create-connection
     "failed to connect to server")))
