# URL Sanitization

For URLs with query strings, the code attempts to remove query parameters unrelated to content navigation by iteratively removing each query parameter and testing the resulting content for differences with original page content. The code also attempts to remove query parameters which typically related to user PII.

## Usage
The file `demo_run.py` illustrate an example. It loads data from `url_training_data_path` and trains a rule for each full domain that determines which query parameters need to be removed. Then, it applies the rule to the data in `url_full_data_path` and removes URLs' parameters accordingly. Finally, it loads the cleaned URLs to the `output_data_path`.

Data in `url_training_data_path` can be either the same as or a subset of that in `url_full_data_path`.

The file `demo_input.tsv` illustrates how the prepare the to-be-cleaned URLs data (both the training data and the full data). It includes three columns, canonical_url,	full_domain and url_id. The data file is tab delimited. We explicitly avoid comma delimited files, since URLs may contains commas too.

The file `demo_output.tsv` illustrates the expected results from running `demo_run.py`.

## License
Apache-2.0
