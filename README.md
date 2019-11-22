# URL Sanitization

For URLs with query strings, the code attempts to remove query parameters unrelated to content navigation by iteratively removing each query parameter and testing the resulting content for differences with original page content. The code also attempts to remove query parameters which typically related to user PII.

## Requirement
The URL Sanitization requires Python 3.

## How it works

The `process_urls()` takes loads data from `url_training_data_path` and trains a rule for each full domain that determines which query parameters need to be removed. Then, it applies the rule to the data in `url_full_data_path` and removes URLs' parameters accordingly. Finally, it loads the cleaned URLs to the `output_data_path`.

All input and output files are tab-separated values (TSV) file. We explicitly avoid comma delimited files, since URLs may contains commas too. The URLs to be processed are named `canonical_url` in both data sets `url_training_data_path` and `url_full_data_path`.

## Examples
The file `demo_run.py` illustrate an example. The file `demo_input.tsv` illustrates how the prepare the to-be-cleaned URLs data (both the training data and the full data). The file `demo_output.tsv` illustrates the expected processed results.

## License
Apache-2.0
