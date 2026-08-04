#' Compute per-condition summary statistics and round-level plot data
#'
#' Applies refbank's standard exclusions (trial-level `exclude`,
#' message-level `message_irrelevant`) and summarizes accuracy and describer
#' word count by round, plus overall per-condition counts (games, players,
#' rounds, trials, images). This is the shared computation behind
#' refbank-import's interactive `check_dataset()` QA tool and the refbank
#' website's per-dataset validation page, so the numbers shown in both
#' places are guaranteed to match.
#'
#' @param trials A trials tibble (as returned by [get_trials()]) for a single dataset.
#' @param selections A selections tibble (as returned by [get_selections()]) for the same dataset.
#' @param messages A messages tibble (as returned by [get_messages()]) for the same dataset.
#' @param conditions A conditions tibble (as returned by [get_conditions()]) for the same dataset.
#'
#' @return A list with three tibbles:
#'   * `summary_stats` — one row per condition (games, players, rounds,
#'     trials, images, option set size, coverage of selections/messages).
#'   * `accuracy_plot_data` — one row per trial (`round_num`, `condition_label`,
#'     `stage_num`, `value`), `value` is 1 if the matcher selected the target image.
#'   * `words_plot_data` — one row per trial, `value` is the describer's word count.
#'
#' @export
summarize_dataset_checks <- function(trials, selections, messages, conditions) {
  # normalize join-key types: some datasets have empty selections/messages
  # tables, which makes readr guess a different type than the populated tables
  trials <- trials |> dplyr::mutate(trial_id = as.character(trial_id), condition_id = as.character(condition_id))
  selections <- selections |> dplyr::mutate(trial_id = as.character(trial_id))
  messages <- messages |> dplyr::mutate(trial_id = as.character(trial_id))
  conditions <- conditions |> dplyr::mutate(condition_id = as.character(condition_id))

  # apply all originally occurring exclusions
  trials <- trials |>
    dplyr::filter(is.na(exclude) | !exclude) |>
    dplyr::left_join(conditions |> dplyr::select(condition_id, condition_label), by = "condition_id")

  messages <- messages |>
    dplyr::filter(is.na(message_irrelevant) | !message_irrelevant)

  # summarize accuracy per round (in each condition)
  selections_by_trial <- trials |>
    dplyr::left_join(selections |> dplyr::select(trial_id, selected_image), by = "trial_id")

  # some conditions have multiple matchers per trial (one selection row each),
  # so count distinct trials with a selection rather than selection rows
  n_trials_with_selections <- selections_by_trial |>
    dplyr::group_by(condition_label) |>
    dplyr::summarize(n_trials_with_selections = dplyr::n_distinct(trial_id[!is.na(selected_image)]), .groups = "drop")

  accuracy_plot_data <- selections_by_trial |>
    dplyr::mutate(value = as.numeric(as.character(selected_image) == as.character(target_image))) |>
    dplyr::select(round_num, condition_label, stage_num, value)

  # summarize mean # words from describer per round (in each condition)
  words_per_trial <- messages |>
    dplyr::filter(role == "describer") |>
    dplyr::mutate(n_words = stringr::str_count(text, "\\S+")) |>
    dplyr::group_by(trial_id) |>
    dplyr::summarize(n_words = sum(n_words), .groups = "drop")

  words_by_trial <- trials |>
    dplyr::left_join(words_per_trial, by = "trial_id")

  n_trials_with_messages <- words_by_trial |>
    dplyr::group_by(condition_label) |>
    dplyr::summarize(n_trials_with_messages = sum(!is.na(n_words)), .groups = "drop")

  words_plot_data <- words_by_trial |>
    dplyr::mutate(value = tidyr::replace_na(n_words, 0)) |>
    dplyr::select(round_num, condition_label, stage_num, value)

  # number of images (total), option set size (range)
  images_total <- trials |>
    dplyr::mutate(image = stringr::str_split(image_options, ";")) |>
    tidyr::unnest(image) |>
    dplyr::distinct(condition_label, image) |>
    dplyr::count(condition_label, name = "n_images_total")

  option_set_size <- trials |>
    dplyr::mutate(option_set_size = image_options |> stringr::str_split(";") |> lengths()) |>
    dplyr::group_by(condition_label) |>
    dplyr::summarize(
      min_option_set_size = min(option_set_size),
      max_option_set_size = max(option_set_size),
      .groups = "drop"
    )

  trials_per_round <- trials |>
    dplyr::count(condition_label, game_id, room_num, round_num, name = "n_trials_in_round") |>
    dplyr::group_by(condition_label) |>
    dplyr::summarize(
      mean_trials_per_round = mean(n_trials_in_round),
      min_trials_per_round = min(n_trials_in_round),
      max_trials_per_round = max(n_trials_in_round),
      .groups = "drop"
    )

  # number of games
  # mean, min - max players / game
  # mean, min - max rounds / game
  # mean, min - max trials / game
  # total trials in condition
  players_per_game <- dplyr::bind_rows(
    trials |> dplyr::distinct(condition_label, game_id, player_id = as.character(describer)),
    trials |>
      dplyr::mutate(matcher = stringr::str_split(matchers, ";")) |>
      tidyr::unnest(matcher) |>
      dplyr::distinct(condition_label, game_id, player_id = as.character(matcher))
  ) |>
    # a trial with no resolved matcher (e.g. an unmatched role-inference join)
    # leaves matchers NA -- that's a missing player, not an extra one
    dplyr::filter(!is.na(player_id)) |>
    dplyr::distinct(condition_label, game_id, player_id) |>
    dplyr::count(condition_label, game_id, name = "n_players")

  rounds_per_game <- trials |>
    dplyr::distinct(condition_label, game_id, round_num) |>
    dplyr::count(condition_label, game_id, name = "n_rounds")

  trials_per_game <- trials |>
    dplyr::count(condition_label, game_id, name = "n_trials")

  per_game <- trials_per_game |>
    dplyr::left_join(rounds_per_game, by = c("condition_label", "game_id")) |>
    dplyr::left_join(players_per_game, by = c("condition_label", "game_id"))

  summary_stats <- per_game |>
    dplyr::group_by(condition_label) |>
    dplyr::summarize(
      n_games = dplyr::n(),
      total_trials = sum(n_trials),
      mean_players = mean(n_players), min_players = min(n_players), max_players = max(n_players),
      mean_rounds = mean(n_rounds), min_rounds = min(n_rounds), max_rounds = max(n_rounds),
      mean_trials = mean(n_trials), min_trials = min(n_trials), max_trials = max(n_trials),
      .groups = "drop"
    ) |>
    dplyr::left_join(images_total, by = "condition_label") |>
    dplyr::left_join(option_set_size, by = "condition_label") |>
    dplyr::left_join(trials_per_round, by = "condition_label") |>
    dplyr::left_join(n_trials_with_selections, by = "condition_label") |>
    dplyr::left_join(n_trials_with_messages, by = "condition_label") |>
    dplyr::left_join(
      conditions |> dplyr::select(
        condition_label, group_size, population, prior_relationship, partner_constancy,
        role_constancy, confederates, modality, feedback, backchannel, language
      ),
      by = "condition_label"
    )

  list(
    summary_stats = summary_stats,
    accuracy_plot_data = accuracy_plot_data,
    words_plot_data = words_plot_data
  )
}
