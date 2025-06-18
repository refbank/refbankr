#' Reference to Refbank Redivis dataset
#'
#' @param version (Optional) Version of the dataset to reference (defaults to
#'   "current"), can also be "next" for unreleased versions.
#'
#' @export
refbank <- function(version) {
  message(glue::glue("Using Refbank dataset version: {version}"))
  redivis::redivis$user("mcfrank")$dataset("refbank", version = version)
}

# run query on dataset and retrieve results
get_dataset_query <- function(dataset, query_str, max_results) {
  dataset$query(query_str)$to_tibble(max_results = max_results)
}

# reference a table in dataset and retrieve results
get_dataset_table <- function(dataset, table_name, max_results) {
  dataset$table(table_name)$to_tibble(max_results = max_results)
}

# build a SQL WHERE clause out of a variable and its allowed values
build_filter <- function(var, vals) {
  vals_str <- glue("'{vals}'") |> paste(collapse = ", ")
  if (is.null(vals)) "" else glue("WHERE {var} IN ({vals_str})")
}

# build a SQL query that subsets a dataset table by a vector of dataset IDs
build_dataset_query <- function(table_name, datasets, max_results) {
  dataset_filter <- build_filter(var = "dataset_id", vals = datasets)
  glue("SELECT * FROM {table_name} {dataset_filter} LIMIT {max_results}" ) |>
    stringr::str_trim()
}

#' Get datasets
#'
#' @inheritParams refbank
#' @param max_results (Optional) Max number of records to load for each table (defaults to
#'   entire table).
#'
#' @export
get_datasets <- function(version = "current", max_results = NULL) {
  get_dataset_table(refbank(version), "datasets:q7yy", max_results)
}

#' Get messages
#'
#' @inheritParams get_datasets
#' @param datasets (Optional) Character vector of dataset IDs to include
#'   (default to all datasets).
#'
#' @export
get_messages <- function(version = "current", datasets = NULL, max_results = NULL) {
  query_str <- build_dataset_query("messages:2q18", datasets, max_results)
  get_dataset_query(refbank(version), query_str, max_results)
}

#' Get trials
#'
#' @inheritParams get_messages
#'
#' @export
get_trials <- function(version = "current", datasets = NULL, max_results = NULL) {
  query_str <- build_dataset_query("trials:zkj2", datasets, max_results)
  get_dataset_query(refbank(version), query_str, max_results)
}

#' Get choices
#'
#' @inheritParams get_messages
#'
#' @export
get_choices <- function(version = "current", datasets = NULL, max_results = NULL) {
  query_str <- build_dataset_query("choices:s1zj", datasets, max_results)
  get_dataset_query(refbank(version), query_str, max_results)
}

#' Get conditions
#'
#' @inheritParams get_messages
#'
#' @export
get_conditions <- function(version = "current", datasets = NULL, max_results = NULL) {
  query_str <- build_dataset_query("conditions:kk1e", datasets, max_results)
  get_dataset_query(refbank(version), query_str, max_results)
}
