#' Reference to Refbank Redivis dataset
#'
#' @param version (Optional) Version of the dataset to reference (defaults to
#'   "current"), can also be "next" for unreleased versions.
#'
#' @export
refbank <- function(version) {
  message(glue::glue("Using Refbank dataset version: {version}"))
  redivis::redivis$user("mcfrank")$dataset("refbank:2zy7", version = version)
}

#' Reference to Refbank Redivis workflow for processed outputs
#'
#'
#' @export
refbank_derived <- function() {
  workflow <- redivis::redivis$user("alvinwmtan")$workflow("refbank-sbert:y7kz")
  source<- workflow$list_datasources()[[1]]$properties$sourceDataset$scopedReference
  version <- stringr::str_sub(source, 14,-1)
  message(glue::glue("The processed data corresponds to refbank version {version}."))
  workflow
}


#' Returns the version tag for the current version of refbank
#'
#'
#' @export
get_current_version <- function() {
  refbank("current")$get()$properties$currentVersion$tag
}


table_keys <- c("datasets"="datasets:q7yy", "messages"="messages:2q18",
                "trials"="trials:zkj2", "choices"="choices:s1zj",
                "conditions"="conditions:kk1e", "players"="players:7xnd",
                "images"="images:jw0t", "image_files"="image_files:zkvc",
                "embeddings"="embeddings_full:wn5k","sims"="embed_sims:har0"
)

join_conditions_string <- "LEFT JOIN {table_keys['conditions']} USING (condition_id, dataset_id)"
join_images_string <- "LEFT JOIN {table_keys['images']} ON `trials:zkj2`.target = `images:jw0t`.image_id"
join_players_string <- "LEFT JOIN {table_keys['players']} USING (player_id, dataset_id)"
join_trials_string <- "LEFT JOIN {table_keys['trials']} USING (trial_id, dataset_id)"

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
  vals_str <- glue::glue("'{vals}'") |> paste(collapse = ", ")
  if (is.null(vals)) "" else glue::glue("WHERE {var} IN ({vals_str})")
}

# build a SQL query that subsets a dataset table by a vector of dataset IDs
build_dataset_query <- function(primary_table, aux_joins, datasets, max_results) {
  if (!is.null(max_results)) {max_results_str= glue::glue("LIMIT {max_results}")}
  else {max_results_str=""}
  dataset_filter <- build_filter(var = "dataset_id", vals = datasets)
   glue::glue("SELECT * FROM {table_keys[primary_table]} {aux_joins} {dataset_filter} {max_results_str}" ) |>
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
get_messages <- function(version = "current", datasets = NULL, max_results = NULL,
                         include_trial_data=F,
                         include_player_data=F,
                         include_image_data=F,
                         include_condition_data=F) {
  primary_table="messages"
  join_string <- ""
  if (include_image_data || include_condition_data) {include_trial_data=T}
  if (include_trial_data) {join_string=stringr::str_c(join_string, glue::glue(join_trials_string))}
  if (include_player_data) {join_string=stringr::str_c(join_string, glue::glue(join_players_string))}
  if (include_image_data) {join_string=stringr::str_c(join_string, glue::glue(join_images_string))}
  if (include_condition_data) {join_string=stringr::str_c(join_string, glue::glue(join_conditions_string))}
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
  if (include_image_data) {join_string=stringr::str_c(join_string, glue::glue(join_images_string))}
  if (include_condition_data) {join_string=stringr::str_c(join_string, glue::glue(join_conditions_string))}
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
  if (include_trial_data) {join_string=stringr::str_c(join_string, glue::glue(join_trials_string))}
  if (include_player_data) {join_string=stringr::str_c(join_string, glue::glue(join_players_string))}
  if (include_image_data) {join_string=stringr::str_c(join_string, glue::glue(join_images_string))}
  if (include_condition_data) {join_string=stringr::str_c(join_string, glue::glue(join_conditions_string))}
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
  primary_table="images"
  if (!is.null(max_results)) {max_results_str= glue::glue("LIMIT {max_results}")}
  else {max_results_str=""}
  dataset_filter <- build_filter(var = "dataset_id", vals = datasets)
  cte_string <- "WITH unique_image_ids AS (
  SELECT DISTINCT
    dataset_id,
    TRIM(image_id) AS image_id
  FROM `trials:zkj2`
  CROSS JOIN UNNEST(SPLIT(option_set, ';')) AS image_id)"
  join_string="LEFT JOIN unique_image_ids USING (image_id)"
   query_str <- glue::glue("{cte_string} SELECT * FROM {table_keys[primary_table]} {join_string} {dataset_filter} {max_results_str}" ) |>
    stringr::str_trim()
   message(query_str)
   get_dataset_query(refbank(version), query_str, max_results)
}

#' Download image files
#'
#' @inheritParams get_messages
#' @param destination directory to write files to, defaults to working directory
#'
#' @export
download_image_files <- function(version = "current", destination=getwd(), datasets = NULL, max_results=NULL, overwrite=F) {
  primary_table="image_files"
  if (!is.null(max_results)) {max_results_str= glue::glue("LIMIT {max_results}")}
  else {max_results_str=""}
  dataset_filter <- build_filter(var = "dataset_id", vals = datasets)
  cte_string <- "WITH unique_image_ids AS (
  SELECT DISTINCT
    dataset_id,
    TRIM(image_id) AS image_id
  FROM `trials:zkj2`
  CROSS JOIN UNNEST(SPLIT(option_set, ';')) AS image_id
)"
  join_string_trials="LEFT JOIN unique_image_ids USING (image_id)"
  join_string_image_labels="LEFT JOIN `images:jw0t` ON `image_files:zkvc`.file_name = `images:jw0t`.image_path"
  query_str <- glue::glue("{cte_string} SELECT * FROM {table_keys[primary_table]} {join_string_image_labels}
                             {join_string_trials} {dataset_filter} {max_results_str}" ) |>
    stringr::str_trim()
  message(query_str)
  refbank(version)$query(query_str)$download_files(path=destination, overwrite, max_results=max_results)
}


#' Get sbert embeddings
#'
#' @inheritParams get_messages
#'
#' @export
get_sbert_embeddings <- function(datasets = NULL, max_results = NULL) {

  primary_table="embeddings"
  join_string=""
  query_str <- build_dataset_query(primary_table, join_string, datasets, max_results)
  get_dataset_query(refbank_derived(), query_str, max_results) |>
    dplyr::select(!starts_with("dim"), everything()) #put all the metadata first
}



#' Get cosine similarities of sbert embeddings
#'
#' @inheritParams get_messages
#' @param sim_type (Optional) Character vector of which similarity comparisons to return
#'    options are "to_last", "to_next", "to_first", "diverge", "diff", "idiosyncrasy"
#'   (default to all comparisons).
#' @export
get_cosine_similarities <- function(datasets = NULL, sim_type=NULL, max_results = NULL) {

  primary_table="sims"
  if (is.null(datasets) || is.null(sim_type)){ # easy mode, one WHERE clause
  type_filter <- build_filter(var = "sim_type", vals = sim_type)
  query_str <- build_dataset_query(primary_table, type_filter, datasets, max_results)
  }
  else { #hard mode, need a conjoint WHERE clause
    type_filter <- build_filter(var = "sim_type", vals = sim_type)
    vals_str <- glue::glue("'{datasets}'") |> paste(collapse = ", ")
    dataset_filter <- glue::glue("AND dataset_id IN ({vals_str})")
    both_filter <- stringr::str_c(type_filter, " ", dataset_filter)
    query_str <- build_dataset_query(primary_table, both_filter, NULL, max_results)
  }
  message(query_str)
  get_dataset_query(refbank_derived(), query_str, max_results) |>
    dplyr::select_if(function(x){!all(is.na(x))})
  #depending on what types of sim comparisons are returned, some columns don't apply!
}
