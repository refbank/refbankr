table_function_map <- c(
  "datasets"            = "get_datasets",
  "messages"            = "get_messages",
  "trials"              = "get_trials",
  "selections"          = "get_selections",
  "conditions"          = "get_conditions",
  "players"             = "get_players",
  "images"              = "get_images",
  "image_files"         = "download_image_files",
  "embeddings"          = "get_sbert_embeddings",
  "cosine_similarities" = "get_cosine_similarities",
  "summary"             = "get_dataset_summary",
  "per_game_summary"    = "get_per_game_summary",
  "annotations" = "get_annotated_messages",
  "parsed" = "get_parsed_messages"
)

test_that("all mapped functions are exported by the package", {
  exported <- getNamespaceExports("refbankr")
  for (fn in table_function_map) {
    expect_true(fn %in% exported, label = paste(fn, "is exported"))
  }
})

test_that("all tables in current version have a fetch function", {
  ensure_redivis_auth()
  live_tables <- refbank("current")$list_tables()
  live_names <- sapply(live_tables, function(t) t$name)
  for (name in live_names) {
    expect_true(
      name %in% names(table_function_map),
      label = paste0("table '", name, "' has a fetch function")
    )
  }
})

test_that("all tables in next version have a fetch function", {
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
})
