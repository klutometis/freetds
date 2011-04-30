(use test freetds sql-null)

(include "test-freetds-secret.scm")

(test-begin "FreeTDS")

(test-group "connection management"
  (test-assert "connect returns an open connection"
               (let* ((conn (make-connection server username password))
                      (is-conn (connection? conn))
                      (conn-open (connection-open? conn)))
                 (connection-close conn)
                 (and is-conn conn-open)))
  (test-error "cannot connect with invalid credentials"
              (make-connection server "non-existing-user" "invalid-password"))
  (test-assert "connection-close closes the connection"
              (let ((conn (make-connection server username password)))
                (connection-close conn)
                (not (connection-open? conn)))))

;; From now on, just keep using the same connection
(define connection (make-connection server username password))

(test-group "type parsing"
  (call-with-result-set
   connection
   "SELECT 'one', 'testing', ''"
   (lambda (command)
     (test "String values are retrieved correctly"
           '(("one" "testing" ""))
           (result-values connection command))))
  (call-with-result-set
   connection
   "SELECT 0, -1, 110"
   (lambda (command)
     (test "Integer values are retrieved correctly"
           '((0 -1 110))
           (result-values connection command))))
  (call-with-result-set
   connection
   "SELECT 0.0, -1.5, 256.0, 257.0, 0.256, 110.12345"
   (lambda (command)
     (test "Numeric values are retrieved correctly"
           '((0.0 -1.5 256.0 257.0 0.256 110.12345))
           (result-values connection command))))
  (call-with-result-set
   connection
   (conc "SELECT CAST(0.0 AS FLOAT), CAST(-1.5 AS FLOAT), "
         "       CAST(256.0 AS FLOAT), CAST(257.0 AS FLOAT), "
         "       CAST(0.125 AS FLOAT), CAST(110.0625 AS FLOAT)")
   (lambda (command)
     (test "Float values are retrieved correctly"
           '((0.0 -1.5 256.0 257.0 0.125 110.0625))
           (result-values connection command))))
  (call-with-result-set
   connection
   (conc "SELECT CAST(0.0 AS REAL), CAST(-1.5 AS REAL), "
         "       CAST(256.0 AS REAL), CAST(257.0 AS REAL), "
         "       CAST(0.125 AS REAL), CAST(110.0625 AS REAL)")
   (lambda (command)
     (test "Real values are retrieved correctly"
           '((0.0 -1.5 256.0 257.0 0.125 110.0625))
           (result-values connection command))))
  (call-with-result-set
   connection
   "SELECT NULL, NULL"
   (lambda (command)
     (test "NULL values are retrieved correctly"
           (list (list (sql-null) (sql-null)))
           (result-values connection command)))))
(test-group "type unparsing"
  (call-with-result-set
   connection
   "SELECT ?, ?, ?"
   "one" "testing" ""
   (lambda (command)
     (test "String values are written correctly"
           '(("one" "testing" ""))
           (result-values connection command))))
  (call-with-result-set
   connection
   "SELECT ?, ?, ?"
   0 -1 110
   (lambda (command)
     (test "Integer values are written correctly"
           '((0 -1 110))
           (result-values connection command))))
  (call-with-result-set
   connection
   "SELECT ?, ?, ?"
   0.0 -1.5 110.12345
   (lambda (command)
     (test "Float values are written correctly"
           '((0.0 -1.5 110.12345))
           (result-values connection command))))
  (call-with-result-set
   connection
   "SELECT ?, ?"
   (sql-null) (sql-null)
   (lambda (command)
     (test "NULL values are written correctly"
           (list (list (sql-null) (sql-null)))
           (result-values connection command)))))
(test-group "misc"
  (test-error "Error for invalid SQL"
              (call-with-result-set
               connection "INVALID"
               ;; This should be just VOID, but there's a bug
               ;; in FreeTDS which causes the error to be
               ;; put in the queue but not returned as status.
               ;; There's nothing we can do about that.
               (lambda (command)
                 (result-values connection command)))))

(test-end)

(unless (zero? (test-failure-count)) (exit 1))