test_that("build_filter returns empty string for NULL vals", {
  expect_equal(refbankr:::build_filter("dataset_id", NULL), "")
})

test_that("build_filter returns WHERE clause for single value", {
  expect_equal(refbankr:::build_filter("dataset_id", "foo"), "WHERE dataset_id IN ('foo')")
})

test_that("build_filter returns WHERE clause for multiple values", {
  expect_equal(
    refbankr:::build_filter("dataset_id", c("foo", "bar")),
    "WHERE dataset_id IN ('foo', 'bar')"
  )
})

test_that("build_dataset_query produces clean SELECT with no filter or limit", {
  result <- refbankr:::build_dataset_query("messages", "", NULL, NULL)
  expected_table <- refbankr:::table_keys["messages"]
  expect_match(result, paste0("^SELECT \\* FROM ", expected_table))
  expect_false(grepl("WHERE", result))
  expect_false(grepl("LIMIT", result))
  expect_false(grepl("\\s+$", result))
})

test_that("build_dataset_query includes LIMIT when max_results is given", {
  result <- refbankr:::build_dataset_query("messages", "", NULL, 10)
  expect_match(result, "LIMIT 10$")
})

test_that("build_dataset_query includes join string", {
  join <- "LEFT JOIN foo USING (id)"
  result <- refbankr:::build_dataset_query("messages", join, NULL, NULL)
  expect_match(result, "LEFT JOIN foo USING \\(id\\)")
})

test_that("build_dataset_query includes WHERE clause for dataset filter", {
  result <- refbankr:::build_dataset_query("messages", "", c("d1", "d2"), NULL)
  expect_match(result, "WHERE dataset_id IN \\('d1', 'd2'\\)")
})
