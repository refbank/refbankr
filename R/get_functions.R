#' Get datasets
#'
#' @export
get_datasets <- function() {
  redivis::redivis$user("mcfrank")$dataset("refbank")$table("datasets")$to_tibble()
}

#' Get messages
#'
#' @export
get_messages <- function() {
  redivis::redivis$user("mcfrank")$dataset("refbank")$table("messages")$to_tibble()
}

#' Get trials
#'
#' @export
get_trials <- function() {
  redivis::redivis$user("mcfrank")$dataset("refbank")$table("trials")$to_tibble()
}

#' Get choices
#'
#' @export
get_choices <- function() {
  redivis::redivis$user("mcfrank")$dataset("refbank")$table("choices")$to_tibble()
}


