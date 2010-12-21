.PHONY: test clean test-module clean-module

# NB: needs the autocompile egg
test:
	CHICKEN_SCHEME_OPTIONS=-lct ./test-freetds.scm

clean:
	chicken-scheme -purge

test-module:
	chicken-install -n && ./test-freetds-module.scm

clean-module:
	rm -vf freetds.{c,o,so} freetds.import.*

db:
	tsql -S $(HOST):1344 -U freetds -P freetds < test.sql
