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
  (test "String values are retrieved correctly"
        '(("one" "testing" ""))
        (result-values (send-query connection "SELECT 'one', 'testing', ''")))
  (test "Integer values are retrieved correctly"
        '((0 -1 110))
        (result-values (send-query connection "SELECT 0, -1, 110")))
  (test "Numeric values are retrieved correctly"
           '((0.0 -1.5 256.0 257.0 0.256 110.12345))
           (result-values
            (send-query connection
                        "SELECT 0.0, -1.5, 256.0, 257.0, 0.256, 110.12345")))
  (test "Float values are retrieved correctly"
        '((0.0 -1.5 256.0 257.0 0.125 110.0625))
        (result-values
         (send-query connection
                     (conc "SELECT CAST(0.0 AS FLOAT), CAST(-1.5 AS FLOAT), "
                           "       CAST(256.0 AS FLOAT), CAST(257.0 AS FLOAT), "
                           "       CAST(0.125 AS FLOAT), CAST(110.0625 AS FLOAT)"))))
  (test "Real values are retrieved correctly"
        '((0.0 -1.5 256.0 257.0 0.125 110.0625))
        (result-values
         (send-query connection
                     (conc "SELECT CAST(0.0 AS REAL), CAST(-1.5 AS REAL), "
                           "       CAST(256.0 AS REAL), CAST(257.0 AS REAL), "
                           "       CAST(0.125 AS REAL), CAST(110.0625 AS REAL)"))))
  (test "NULL values are retrieved correctly"
        (list (list (sql-null) (sql-null)))
        (result-values (send-query connection "SELECT NULL, NULL"))))

(test-group "type unparsing"
  (test "String values are written correctly"
        '(("one" "testing" ""))
        (result-values
         (send-query connection "SELECT ?, ?, ?" "one" "testing" "")))
  (test "Integer values are written correctly"
        '((0 -1 110))
        (result-values (send-query connection "SELECT ?, ?, ?" 0 -1 110)))
  (test "Float values are written correctly"
        '((0.0 -1.5 110.12345))
        (result-values
         (send-query connection "SELECT ?, ?, ?" 0.0 -1.5 110.12345)))
  (test "NULL values are written correctly"
        (list (list (sql-null) (sql-null)))
        (result-values
         (send-query connection "SELECT ?, ?" (sql-null) (sql-null)))))

(test-group "misc"
  (test "Call-with-result-set works the way it should"
        ;; TODO: Shouldn't SELECT 1, 2, 3 UNION SELECT 4, 5, 6 return
        ;; ((1 2 3) (4 5 6)) instead of ((4 5 6) (1 2 3))?
        '((1 2 3))
        (call-with-result-set connection "SELECT 1, 2, 3" result-values))
  (test-error "Error for invalid SQL"
              (send-query connection "INVALID")))

(test-end)

(unless (zero? (test-failure-count)) (exit 1))