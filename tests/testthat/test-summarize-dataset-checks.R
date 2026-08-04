minimal_conditions <- function() {
  tibble::tibble(
    condition_id = "c1",
    condition_label = "cond_a",
    group_size = 2,
    population = "adult",
    prior_relationship = "no",
    partner_constancy = "yes",
    role_constancy = "no",
    confederates = "no",
    modality = "written",
    feedback = "none",
    backchannel = "none",
    language = "English"
  )
}

minimal_trials <- function(exclude = c(NA, NA, NA, NA)) {
  tibble::tibble(
    trial_id = c("t1", "t2", "t3", "t4"),
    condition_id = "c1",
    game_id = c("g1", "g1", "g2", "g2"),
    room_num = c("g1", "g1", "g2", "g2"),
    round_num = c(1, 2, 1, 2),
    stage_num = 1,
    describer = c("d1", "d1", "d2", "d2"),
    matchers = c("m1", "m1", "m2", "m2"),
    image_options = "img1;img2;img3",
    target_image = "img1",
    exclude = exclude
  )
}

minimal_selections <- function() {
  tibble::tibble(
    trial_id = c("t1", "t2", "t3", "t4"),
    selected_image = c("img1", "img1", "img1", "img2")
  )
}

minimal_messages <- function(message_irrelevant = c(NA, NA, NA, NA)) {
  tibble::tibble(
    trial_id = c("t1", "t2", "t3", "t4"),
    role = "describer",
    text = c("one two three", "four five", "six", "seven eight nine ten"),
    message_irrelevant = message_irrelevant
  )
}

test_that("summarize_dataset_checks computes per-condition summary stats", {
  result <- summarize_dataset_checks(
    trials = minimal_trials(),
    selections = minimal_selections(),
    messages = minimal_messages(),
    conditions = minimal_conditions()
  )

  stats <- result$summary_stats
  expect_equal(nrow(stats), 1)
  expect_equal(stats$condition_label, "cond_a")
  expect_equal(stats$n_games, 2)
  expect_equal(stats$total_trials, 4)
  expect_equal(stats$mean_players, 2)
  expect_equal(stats$mean_rounds, 2)
  expect_equal(stats$mean_trials, 2)
  expect_equal(stats$n_images_total, 3)
  expect_equal(stats$min_option_set_size, 3)
  expect_equal(stats$max_option_set_size, 3)
  expect_equal(stats$n_trials_with_selections, 4)
  expect_equal(stats$n_trials_with_messages, 4)
})

test_that("summarize_dataset_checks produces round-level plot data with accuracy and word count values", {
  result <- summarize_dataset_checks(
    trials = minimal_trials(),
    selections = minimal_selections(),
    messages = minimal_messages(),
    conditions = minimal_conditions()
  )

  expect_equal(nrow(result$accuracy_plot_data), 4)
  expect_setequal(result$accuracy_plot_data$value, c(1, 1, 1, 0))

  expect_equal(nrow(result$words_plot_data), 4)
  expect_setequal(result$words_plot_data$value, c(3, 2, 1, 4))
})

test_that("summarize_dataset_checks drops excluded trials but keeps NA-exclude trials", {
  trials <- minimal_trials(exclude = c(TRUE, NA, NA, NA))

  result <- summarize_dataset_checks(
    trials = trials,
    selections = minimal_selections(),
    messages = minimal_messages(),
    conditions = minimal_conditions()
  )

  expect_equal(result$summary_stats$total_trials, 3)
  expect_equal(nrow(result$accuracy_plot_data), 3)
})

test_that("summarize_dataset_checks drops message-irrelevant messages from word counts", {
  messages <- minimal_messages(message_irrelevant = c(TRUE, NA, NA, NA))

  result <- summarize_dataset_checks(
    trials = minimal_trials(),
    selections = minimal_selections(),
    messages = messages,
    conditions = minimal_conditions()
  )

  # t1's only message is now irrelevant, so it has zero words and doesn't
  # count as a trial with messages
  expect_equal(result$summary_stats$n_trials_with_messages, 3)
  expect_setequal(result$words_plot_data$value, c(0, 2, 1, 4))
})
