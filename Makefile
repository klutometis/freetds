.PHONY: test clean

# NB: needs the autocompile egg
test:
	CHICKEN_SCHEME_OPTIONS=-lct ./test-freetds.scm

clean:
	chicken-scheme -purge
