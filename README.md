# refbankr

Refbank R package 

To install:

```{r}
remotes::install_github("refbank/refbankr")
```

## Usage

Refbank has a variety of different tables storing the different aspects of referential communications datasets. 

These are:

* `datasets`: a list of datasets and citations 
* `messages`: a list of messages, which are the individual utterances in the datasets
* `choices`: a list of choices, which are the referential choices made by speakers
* `trials`: a list of trials, which are the individual trials in the tasks
* `conditions`: a list of conditions, which are the different experimental conditions in each dataset

These are retrieved by `get_X` functions, as follows:

```{r}
datasets <- refbankr::get_datasets()
```

