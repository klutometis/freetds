.PHONY: test

# NB: needs the autocompile egg
test:
	CHICKEN_SCHEME_OPTIONS=-lct ./test-freetds.scm

purge:
	chicken-scheme -purge
