# Extracted from test-table-coverage.R:46

# setup ------------------------------------------------------------------------
library(testthat)
test_env <- simulate_test_env(package = "refbankr", path = "..")
attach(test_env, warn.conflicts = FALSE)

# prequel ----------------------------------------------------------------------
table_function_map <- c(
  "datasets"            = "get_datasets",
  "messages"            = "get_messages",
  "trials"              = "get_trials",
  "choices"             = "get_choices",
  "conditions"          = "get_conditions",
  "players"             = "get_players",
  "images"              = "get_images",
  "image_files"         = "download_image_files",
  "embeddings"          = "get_sbert_embeddings",
  "cosine_similarities" = "get_cosine_similarities",
  "summary"             = "get_dataset_summary",
  "per_game_summary"    = "get_per_game_summary"
)

# test -------------------------------------------------------------------------
ensure_redivis_auth()
dataset <- tryCatch(refbank("next"), error = function(e) NULL)
if (is.null(dataset)) skip("No 'next' version available")
live_tables <- tryCatch(dataset$list_tables(), error = function(e) NULL)
if (is.null(live_tables)) skip("Could not list tables in 'next' version")
live_names <- sapply(live_tables, function(t) t$name)
for (name in live_names) {
    expect_true(
      name %in% names(table_function_map),
      label = paste0("table '", name, "' has a fetch function in 'next' version")
    )
  }
