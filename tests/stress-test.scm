(use freetds sql-null)
(define debug print)
(include "test-freetds-secret.scm")
(define connection (make-connection server username password))

(send-query connection
            (conc "CREATE TABLE #harro "
                  "(a VARCHAR(30), b VARCHAR(30), c VARCHAR(30), "
                  " d VARCHAR(30), e VARCHAR(30), f VARCHAR(30))"))

(for-each
 (lambda (i)
   (send-query connection
               "INSERT INTO #harro VALUES(?, ?, ?, ?, ?, ?)"
               i (+ i 1) (exact->inexact (+ i 2)) (->string (+ i 3)) (+ i 4) (sql-null)))
 (iota (expt 2 10)))

(debug (result-values
        (send-query connection "SELECT a, b, c, d, e, f FROM #harro")))

(debug (result-values
        (send-query connection "SELECT a, b, c, d, e, f FROM #harro")))

(debug (result-values
        (send-query connection "SELECT a, b, c, d, e, f FROM #harro")))