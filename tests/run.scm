(use test freetds sql-null srfi-19)

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

(test-group "low-level query & results interface"
  (let ((res (send-query connection
                         (conc "SELECT 1 AS one, 2 AS two, 3 AS three"
                               " UNION "
                               "SELECT 4, 5, 6"))))
    (test-assert "send-query returns result object"
                 (result? res))
    (test "Column name can be obtained"
          'one
          (column-name res 0))
    (test "All column names can be obtained"
          '(one two three)
          (column-names res))
    (test "First row-fetch returns one row"
          '(1 2 3)
          (row-fetch res))
    (test "Second row-fetch returns another row (alist)"
          '((one . 4) (two . 5) (three . 6))
          (row-fetch/alist res))
    (test-assert "Final row-fetch returns #f"
                 (not (row-fetch res)))
    (test-assert "Cleaning up the result gives no problems"
                 (result-cleanup! res)))
  (test "result-values retrieves all values"
        '((1 2 3) (4 5 6))
        (result-values
         (send-query connection (conc "SELECT 1 AS one, 2 AS two, 3 AS three"
                                      " UNION "
                                      "SELECT 4, 5, 6"))))
  (test "result-values/alist retrieves all values as alist"
        '(((one . 1) (two . 2) (three . 3))
          ((one . 4) (two . 5) (three . 6)))
        (result-values/alist
         (send-query connection (conc "SELECT 1 AS one, 2 AS two, 3 AS three"
                                      " UNION "
                                      "SELECT 4, 5, 6"))))
  ;; TODO: Maybe we should always fetch the entire result so this doesn't
  ;; have to be a problem
  (test-error "Querying before reading out the previous result gives error"
              (begin (send-query connection "SELECT 1, 2, 3")
                     (send-query connection "SELECT 4, 5, 6")))
  (test "After resetting the connection, we can send queries again"
        '((7 8 9))
        (begin (connection-reset! connection)
               (result-values (send-query connection "SELECT 7, 8, 9"))))
  (test "Cleaning up one result allows us to send another"
        '((13 14 15))
        (begin (result-cleanup! (send-query connection "SELECT 10, 11, 12"))
               (result-values (send-query connection "SELECT 13, 14, 15")))))

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
  (test "Datetime values are retrieved correctly"
        ;; TODO: Figure out how to make this thing use timezones
        `((,(make-date 0  0  0  0 1 1 2000 0)
           ,(make-date 0 56  1 17 9 5 2011 0)
           ,(make-date 0 58 14 17 9 5 2011 0)))
        (result-values
         (send-query connection
                     (conc "SELECT CAST('2000-01-01T00:00:00Z' AS DATETIME),"
                           "       CAST('2011-05-09T17:01:56Z' AS DATETIME),"
                           "       CAST('May 9 2011 17:14:58PM' AS DATETIME)"))))
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
  (test "Datetime values are written correctly"
        ;; TODO: Figure out how to make this thing use timezones
        `((,(make-date 0  0  0  0 1 1 2000 0)
           ,(make-date 0 56  1 17 9 5 2011 0)
           ,(make-date 0 58 14 17 9 5 2011 0)))
        (result-values (send-query connection
                                   "SELECT ?, ?, ?"
                                   (make-date 0  0  0  0 1 1 2000 0)
                                   (make-date 0 56  1 17 9 5 2011 0)
                                   (make-date 0 58 14 17 9 5 2011 0))))
  (test "NULL values are written correctly"
        (list (list (sql-null) (sql-null)))
        (result-values
         (send-query connection "SELECT ?, ?" (sql-null) (sql-null)))))

(test-group "misc"
  (test "Call-with-result-set works the way it should"
        '((1 2 3) (4 5 6))
        (call-with-result-set connection
                              "SELECT 1, 2, 3 UNION SELECT 4, 5, 6"
                              result-values))
  (test-error "Error for invalid SQL"
              (send-query connection "INVALID")))

(test-end)

(unless (zero? (test-failure-count)) (exit 1))