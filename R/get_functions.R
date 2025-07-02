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

table_keys <- c("datasets"="datasets:q7yy", "messages"="messages:2q18",
                "trials"="trials:zkj2", "choices"="choices:s1zj",
                "conditions"="conditions:kk1e", "players"="players:7xnd",
                "images"="images:jw0t"
)

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
build_dataset_query <- function(primary_table, aux_joins, datasets, max_results) {
  if (!is.null(max_results)) {max_results_str= glue("LIMIT {max_results}")}
  else {max_results_str=""}
  dataset_filter <- build_filter(var = "dataset_id", vals = datasets)
   glue("SELECT * FROM {table_keys[primary_table]} {aux_joins} {dataset_filter} {max_results_str}" ) |>
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

join_conditions_string <- "LEFT JOIN {table_keys['conditions']} USING (condition_id, dataset_id)"
#join_conditions_string <- "LEFT JOIN {table_keys['conditions']} ON {table_keys['trials']}.condition_id = {table_keys['conditions']}.condition_id"

join_images_string <- "LEFT JOIN {table_keys['images']} ON {table_keys['trials']}.target = {table_keys['images']}.image_id"

join_players_string <- "LEFT JOIN {table_keys['players']} USING (player_id, dataset_id)"

join_trials_string <- "LEFT JOIN {table_keys['trials']} USING (trial_id, dataset_id)"

#' Get messages
#'
#' @inheritParams get_datasets
#' @param datasets (Optional) Character vector of dataset IDs to include
#'   (default to all datasets).
#'
#' @export
get_messages <- function(version = "current", datasets = NULL, max_results = NULL,
                         include_trial_data=F,
                         include_player_data=F,
                         include_image_data=F,
                         include_condition_data=F) {
  primary_table="messages"
  join_string <- ""
  if (include_image_data || include_condition_data) {include_trial_data=T}
  if (include_trial_data) {join_string=stringr::str_c(join_string, glue(join_trials_string))}
  if (include_player_data) {join_string=stringr::str_c(join_string, glue(join_players_string))}
  if (include_image_data) {join_string=stringr::str_c(join_string, glue(join_images_string))}
  if (include_condition_data) {join_string=stringr::str_c(join_string, glue(join_conditions_string))}
  query_str <- build_dataset_query(primary_table, join_string, datasets, max_results)
  message(query_str)
  get_dataset_query(refbank(version), query_str, max_results)
}


#' Get trials
#'
#' @inheritParams get_messages
#'
#' @export
get_trials <- function(version = "current", datasets = NULL, max_results = NULL,
                       include_image_data=F,
                       include_condition_data=F) {

  join_string=""
  if (include_image_data) {join_string=stringr::str_c(join_string, glue(join_images_string))}
  if (include_condition_data) {join_string=stringr::str_c(join_string, glue(join_conditions_string))}
  query_str <- build_dataset_query("trials", join_string,  datasets, max_results)
  message(query_str)
  get_dataset_query(refbank(version), query_str, max_results)
}


#' Get choices
#'
#' @inheritParams get_messages
#'
#' @export
get_choices <- function(version = "current", datasets = NULL, max_results = NULL,
                        include_trial_data=F,
                        include_player_data=F,
                        include_image_data=F,
                        include_condition_data=F) {

  primary_table="choices"
  join_string=""
  if (include_image_data || include_condition_data) {include_trial_data=T}
  if (include_trial_data) {join_string=stringr::str_c(join_string, glue(join_trials_string))}
  if (include_player_data) {join_string=stringr::str_c(join_string, glue(join_players_string))}
  if (include_image_data) {join_string=stringr::str_c(join_string, glue(join_images_string))}
  if (include_condition_data) {join_string=stringr::str_c(join_string, glue(join_conditions_string))}
  query_str <- build_dataset_query(primary_table, join_string, datasets, max_results)
  get_dataset_query(refbank(version), query_str, max_results)
}


#' Get conditions
#'
#' @inheritParams get_messages
#'
#' @export
get_conditions <- function(version = "current", datasets = NULL, max_results = NULL) {
  query_str <- build_dataset_query("conditions", "", datasets, max_results)
  get_dataset_query(refbank(version), query_str, max_results)
}

#' Get players
#'
#' @inheritParams get_messages
#'
#' @export
get_players <- function(version = "current", datasets = NULL, max_results = NULL) {
  query_str <- build_dataset_query("players", "", datasets, max_results)
  get_dataset_query(refbank(version), query_str, max_results)
}

#' Get images
#'
#' @inheritParams get_messages
#'
#' @export
get_images <- function(version = "current", datasets = NULL, max_results = NULL) {
  query_str <- build_dataset_query("images","",  datasets, max_results)
  get_dataset_query(refbank(version), query_str, max_results)
}
