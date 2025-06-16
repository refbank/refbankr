# refbankr

Refbank R package 

To install:

```{r}
remotes::install_github("refbank/refbankr")
```

## Usage

To get datasets, do:

```{r}
datasets <- refbankr::get_datasets()
```

To get messages, do:

```{r}
messages <- refbankr::get_messages()
```

To get choices, do:

```{r}
choices <- refbankr::get_choices()
```

To get trials, do:

```{r}
trials <- refbankr::get_trials()
```
