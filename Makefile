.PHONY: test

all: fn-tests db-tests

db-tests:
	clj -C:test test/clj_fdb/core_test.clj
	clj -C:test test/clj_fdb/db_test.clj
	clj -C:test test/clj_fdb/directory_test.clj
	clj -C:test test/clj_fdb/subspace_test.clj
	clj -C:test test/clj_fdb/simple_test.clj

fn-tests:
	clj -C:test test/clj_fdb/tuple_test.clj
	clj -C:test test/clj_fdb/value_test.clj
