#;(use foreign)

(foreign-declare "#include <ctpublic.h>")

(define-foreign-type cs-context* (c-pointer "CS_CONTEXT"))
(define-foreign-type cs-int integer32)
(define-foreign-variable cs-version-100 cs-int "CS_VERSION_100")
;;; CS_RETCODE cs_ctx_alloc(CS_INT version, CS_CONTEXT ** ctx);
(define cs-ctx-alloc (foreign-lambda cs-int cs_ctx_alloc cs-int (c-pointer cs-context*)))
#;(define-external context cs-context*)
(foreign-declare "CS_CONTEXT context*;")
(foreign-declare "context = (CS_CONTEXT *) NULL;")
(cs-ctx-alloc cs-version-100 context)
(display 123)
