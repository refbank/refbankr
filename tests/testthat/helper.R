skip_if_no_integration <- function() {
  if (identical(Sys.getenv("REFBANKR_INTEGRATION"), "false")) {
    skip("Integration tests disabled (REFBANKR_INTEGRATION=false)")
  }
}

ensure_redivis_auth <- function() {
  skip_if_no_integration()

  has_api_token <- nchar(Sys.getenv("REDIVIS_API_TOKEN")) > 0
  has_cached_creds <- file.exists(path.expand("~/.redivis/r_credentials"))
  if (has_api_token || has_cached_creds) return(invisible(NULL))

  result <- tryCatch(
    R.utils::withTimeout(redivis:::perform_oauth_login(), timeout = 30, onTimeout = "silent"),
    error = function(e) NULL
  )

  if (is.null(result)) {
    skip("Redivis authentication unavailable or timed out")
  }
  invisible(NULL)
}
