# Extracted from test-get-functions.R:12

# setup ------------------------------------------------------------------------
library(testthat)
test_env <- simulate_test_env(package = "refbankr", path = "..")
attach(test_env, warn.conflicts = FALSE)

# test -------------------------------------------------------------------------
ensure_redivis_auth()
result <- get_messages(max_results = 5)
expect_true(is.data.frame(result))
expect_gt(nrow(result), 0)
