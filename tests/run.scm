(use test freetds sql-null)

(include "test-freetds-secret.scm")

(test-begin "FreeTDS")

(call-with-context
 (lambda (context)
   (call-with-connection
    context
    server
    username
    password
    (lambda (connection)
      (test-group "type parsing"
        (call-with-result-set
         connection
         "SELECT 'one', 'testing', ''"
         (lambda (command)
           (test "String values are retrieved correctly"
                 '(("one" "testing" ""))
                 (result-values context connection command))))
        (call-with-result-set
         connection
         "SELECT 0, -1, 110"
         (lambda (command)
           (test "Integer values are retrieved correctly"
                 '((0 -1 110))
                 (result-values context connection command))))
        (call-with-result-set
         connection
         "SELECT 0.0, -1.5, 256.0, 257.0, 0.256, 110.12345"
         (lambda (command)
           (test "Numeric values are retrieved correctly"
                 '((0.0 -1.5 256.0 257.0 0.256 110.12345))
                 (result-values context connection command))))
        (call-with-result-set
         connection
         (conc "SELECT CAST(0.0 AS FLOAT), CAST(-1.5 AS FLOAT), "
               "       CAST(256.0 AS FLOAT), CAST(257.0 AS FLOAT), "
               "       CAST(0.125 AS FLOAT), CAST(110.0625 AS FLOAT)")
         (lambda (command)
           (test "Float values are retrieved correctly"
                 '((0.0 -1.5 256.0 257.0 0.125 110.0625))
                 (result-values context connection command))))
        (call-with-result-set
         connection
         (conc "SELECT CAST(0.0 AS REAL), CAST(-1.5 AS REAL), "
               "       CAST(256.0 AS REAL), CAST(257.0 AS REAL), "
               "       CAST(0.125 AS REAL), CAST(110.0625 AS REAL)")
         (lambda (command)
           (test "Real values are retrieved correctly"
                 '((0.0 -1.5 256.0 257.0 0.125 110.0625))
                 (result-values context connection command))))
        (call-with-result-set
         connection
         "SELECT NULL, NULL"
         (lambda (command)
           (test "NULL values are retrieved correctly"
                 (list (list (sql-null) (sql-null)))
                 (result-values context connection command)))))
      (test-group "type unparsing"
        (call-with-result-set
         connection
         "SELECT ?, ?, ?"
         "one" "testing" ""
         (lambda (command)
           (test "String values are written correctly"
                 '(("one" "testing" ""))
                 (result-values context connection command))))
        (call-with-result-set
         connection
         "SELECT ?, ?, ?"
         0 -1 110
         (lambda (command)
           (test "Integer values are written correctly"
                 '((0 -1 110))
                 (result-values context connection command))))
        (call-with-result-set
         connection
         "SELECT ?, ?, ?"
         0.0 -1.5 110.12345
         (lambda (command)
           (test "Float values are written correctly"
                 '((0.0 -1.5 110.12345))
                 (result-values context connection command))))
        (call-with-result-set
         connection
         "SELECT ?, ?"
         (sql-null) (sql-null)
         (lambda (command)
           (test "NULL values are written correctly"
                 (list (list (sql-null) (sql-null)))
                 (result-values context connection command)))))))))

(test-end)

(unless (zero? (test-failure-count)) (exit 1))