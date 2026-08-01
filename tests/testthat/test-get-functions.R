test_that("get_datasets returns a non-empty tibble", {
  ensure_redivis_auth()
  result <- get_datasets(max_results = 5)
  expect_true(is.data.frame(result))
  expect_gt(nrow(result), 0)
})

test_that("get_messages returns a non-empty tibble", {
  ensure_redivis_auth()
  result <- get_messages(max_results = 5)
  expect_true(is.data.frame(result))
  expect_gt(nrow(result), 0)
})

test_that("get_trials returns a non-empty tibble", {
  ensure_redivis_auth()
  result <- get_trials(max_results = 5)
  expect_true(is.data.frame(result))
  expect_gt(nrow(result), 0)
})

test_that("get_selections returns a non-empty tibble", {
  ensure_redivis_auth()
  result <- get_selections(max_results = 5)
  expect_true(is.data.frame(result))
  expect_gt(nrow(result), 0)
})

test_that("get_conditions returns a non-empty tibble", {
  ensure_redivis_auth()
  result <- get_conditions(max_results = 5)
  expect_true(is.data.frame(result))
  expect_gt(nrow(result), 0)
})

test_that("get_players returns a non-empty tibble", {
  ensure_redivis_auth()
  result <- get_players(max_results = 5)
  expect_true(is.data.frame(result))
  expect_gt(nrow(result), 0)
})

test_that("get_images returns a non-empty tibble", {
  ensure_redivis_auth()
  result <- get_images(max_results = 5)
  expect_true(is.data.frame(result))
  expect_gt(nrow(result), 0)
})

test_that("get_sbert_embeddings returns a non-empty tibble", {
  ensure_redivis_auth()
  result <- get_sbert_embeddings(max_results = 5)
  expect_true(is.data.frame(result))
  expect_gt(nrow(result), 0)
})

test_that("get_cosine_similarities returns a non-empty tibble", {
  ensure_redivis_auth()
  result <- get_cosine_similarities(max_results = 5)
  expect_true(is.data.frame(result))
  expect_gt(nrow(result), 0)
})

test_that("get_per_game_summary returns a non-empty tibble", {
  ensure_redivis_auth()
  result <- get_per_game_summary(max_results = 5)
  expect_true(is.data.frame(result))
  expect_gt(nrow(result), 0)
})

test_that("get_dataset_summary returns a non-empty tibble", {
  ensure_redivis_auth()
  result <- get_dataset_summary(max_results = 5)
  expect_true(is.data.frame(result))
  expect_gt(nrow(result), 0)
})
