.PHONY: test

test:
	clj -C:test test/clj_fdb/core_test.clj
	clj -C:test test/clj_fdb/db_test.clj
	clj -C:test test/clj_fdb/tuple_test.clj
	clj -C:test test/clj_fdb/subspace_test.clj
