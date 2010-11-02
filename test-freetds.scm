#;(use foreign)
(use format foreigners lolevel)

(foreign-declare "#include <ctpublic.h>")

(define-foreign-type cs-context* (c-pointer "CS_CONTEXT"))
(define-foreign-type cs-int integer32)
(define-foreign-variable cs-version-100 cs-int "CS_VERSION_100")
;; (define-foreign-variable cs-succeed cs-retcode "CS_SUCCEED")
;; (define-foreign-variable cs-fail cs-retcode "CS_FAIL")
(define-foreign-type cs-retcode (enum "CS_RETCODE"))
(define-foreign-variable cs-fail cs-retcode "CS_FAIL")
(define-foreign-variable cs-succeed cs-retcode "CS_SUCCEED")
(define-foreign-variable cs-force-exit cs-retcode "CS_FORCE_EXIT")
;;; CS_RETCODE cs_ctx_alloc(CS_INT version, CS_CONTEXT ** ctx);
;; (define-external context cs-context*)
;; (define-external context (c-pointer "CS_CONTEXT"))
;; (foreign-declare "CS_CONTEXT *context;")
;; (foreign-declare "CS_RETCODE ret;")
;; (foreign-declare "context = (CS_CONTEXT *) NULL;")
;; (foreign-declare "context = (CS_CONTEXT *) NULL;")
;; (define-foreign-variable context (c-pointer "CS_CONTEXT") "NULL")
;; (define-foreign-variable context cs-context* "NULL")
(define cs-ctx-alloc
  (foreign-lambda cs-retcode cs_ctx_alloc cs-int (c-pointer cs-context*)))
(define ct-init
  (foreign-lambda cs-retcode ct_init cs-context* cs-int))
(define ct-exit
  (foreign-lambda cs-retcode ct_exit cs-context* cs-int))
(define cs-ctx-drop
  (foreign-lambda cs-retcode cs_ctx_drop cs-context*))
;; (foreign-declare "ret = cs_ctx_alloc(CS_VERSION_100, &context);")
;; (cs-ctx-alloc cs-version-100 context)
;; (set! context (null-pointer))
(define (freetds-error location message . arguments)
  (signal (make-composite-condition
           (make-property-condition 'exn
                                    'location location
                                    'message message
                                    'arguments arguments)
           (make-property-condition 'freetds))))

(define (error-on-failure thunk location message . arguments)
  (if (not (= (thunk) cs-succeed))
      (apply freetds-error location message arguments)))

(let-location ((context cs-context*))
  (error-on-failure
   (lambda ()
     (cs-ctx-alloc cs-version-100 (location context)))
   'context-allocation
   "failed to allocate context")

  (error-on-failure
   (lambda ()
     (ct-init context cs-version-100))
   'context-initialization
   "failed to initialize context"))
