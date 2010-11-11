#!/usr/bin/env chicken-scheme
(use format
     foreigners
     lolevel
     debug
     srfi-1
     srfi-13)

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

(let-location ((context cs-context*))
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
                  #$cs_server_message_callback))
   'set-server-message-callback
   "failed to set server message-callback")

  (let-location ((connection cs-connection*))
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
     "failed to connect to server")

    (let-location ((command cs-command*))
      (error-on-failure
       (lambda ()
         (ct-cmd-alloc connection
                       #$command))
       'command-allocation
       "failed to allocate command")

      (error-on-failure
       (lambda ()
         (ct-command command
                     cs-language-command
                     #$"SELECT * FROM SYSOBJECTS WHERE XTYPE = 'U'"
                     cs-nullterm
                     cs-unused))
       'command-issue
       "failed to issue command")

      (error-on-failure
       (lambda ()
         (ct-send command))
       'command-send
       "failed to send command")

      #|
      See "Program Structure for Processing Results:"

      while ct_results returns CS_SUCCEED
          case CS_ROW_RESULT
              ct_res_info to get the number of columns
              for each column:
                   ct_describe to get a description of the
                       column
                   ct_bind to bind the column to a program
                       variable
              end for
              while ct_fetch returns CS_SUCCEED or
                   CS_ROW_FAIL
                   if CS_SUCCEED
                       process the row
                   else if CS_ROW_FAIL
                       handle the row failure;
                   end if
              end while
              switch on ct_fetch’s final return code
                   case CS_END_DATA...
                   case CS_CANCELED...
                   case CS_FAIL...
              end switch
          end case
          case CS_CURSOR_RESULT
              ct_res_info to get the number of columns
              for each column:
                   ct_describe to get a description of the
                       column
                   ct_bind to bind the column to a program
                       variable
              end for
          while ct_fetch returns CS_SUCCEED or
               CS_ROW_FAIL
               if CS_SUCCEED
                   process the row
               else if CS_ROW_FAIL
                   handle the row failure
               end if
               /* For update or delete only: */
               if target row is not the row just fetched
                   ct_keydata to specify the target row
                       key
               end if
               /* End for update or delete only */
               /* To send another cursor command: */
               ct_cursor to initiate the cursor command
               ct_param if command is update of some
                   columns only
               ct_send to send the command
               while ct_results returns CS_SUCCEED
                   (...process results...)
               end while
               /* End to send another cursor command */
          end while
          switch on ct_fetch’s final return code
               case CS_END_DATA...
               case CS_CANCELED...
               case CS_FAIL...
          end switch
      end case
      case CS_PARAM_RESULT
          ct_res_info to get the number of parameters
          for each parameter:
               ct_describe to get a description of the
                   parameter
               ct_bind to bind the parameter to a
                   variable
          end for
          while ct_fetch returns CS_SUCCEED or
               CS_ROW_FAIL
               if CS_SUCCEED
                   process the row of parameters
               else if CS_ROW_FAIL
                   handle the failure
               end if
          end while
          switch on ct_fetch’s final return code
               case CS_END_DATA...
               case CS_CANCELED...
               case CS_FAIL...
          end switch
      end case
      case CS_STATUS_RESULT
          ct_bind to bind the status to a program
               variable
          while ct_fetch returns CS_SUCCEED or
               CS_ROW_FAIL
               if CS_SUCCEED
                   process the return status
               else if CS_ROW_FAIL
                   handle the failure
               end if
          end while
          switch on ct_fetch’s final return code
               case CS_END_DATA...
               case CS_CANCELED...
               case CS_FAIL...
          end switch
      end case
      case CS_COMPUTE_RESULT
          (optional: ct_compute_info to get bylist
               length, bylist, or compute row id)
          ct_res_info to get the number of columns
          for each column:
               ct_describe to get a description of the
                   column
               ct_bind to bind the column to a program
                   variable
               (optional: ct_compute_info to get the
                   compute column id or the aggregate
                   operator for the compute column)
          end for
          while ct_fetch returns CS_SUCCEED or
               CS_ROW_FAIL
               if CS_SUCCEED
                   process the compute row
               else if CS_ROW_FAIL
                   handle the failure
               end if
          end while
          switch on ct_fetch’s final return code
               case CS_END_DATA...
               case CS_CANCELED...
               case CS_FAIL...
          end switch
      end case
          case CS_MSG_RESULT
              ct_res_info to get the message id
              code to handle the message
          end case
          case CS_DESCRIBE_RESULT
              ct_res_info to get the number of columns
              for each column:
                   ct_describe or ct_dyndesc to get a
                       description
              end for
          end case
          case CS_ROWFMT_RESULT
              ct_res_info to get the number of columns
              for each column:
                   ct_describe to get a column description
                   send the information on to the gateway
                       client
              end for
          end case
          case CS_COMPUTEFMT_RESULT
              ct_res_info to get the number of columns
              for each column:
                   ct_describe to get a column description
                   (if required:
                       ct_compute_info for compute
                           information
                   end if required)
                   send the information on to the gateway
                       client
              end for
          end case
          case CS_CMD_DONE
              indicates a command’s results are completely
                   processed
          end case
          case CS_CMD_SUCCEED
              indicates the success of a command that
                   returns no results
          end case
          case CS_CMD_FAIL
              indicates a command failed
          end case
      end while
      switch on ct_results’ final return code
          case CS_END_RESULTS
              indicates no more results
          end case
          case CS_CANCELED
              indicates results were canceled
          end case
          case CS_FAIL
              indicates ct_results failed
          end case
      end switch

      |#
      (let-location ((result-type int))
        (let more-results ((result-status
                            (ct-results command #$result-type)))
          (select result-type
            (((foreign-value "CS_ROW_RESULT" int))
             (display 'omg-harro!))))))))
