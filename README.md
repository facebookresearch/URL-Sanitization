# URL Sanitization

For URLs with query strings, the code aims to remove query parameters unrelated to content navigation. It does so by iteratively removing each query parameter and testing the resulting content for differences with original content. The code also remove query parameters often related to user PII by using simple string matching.

## Requirement
The URL Sanitization requires Python 3, along with the pandas, numpy, urllib, difflib, pebble, BeautifulSoup, and phonenumbers modules.

## How it works

The function `process_urls()` takes a list of URLs from `url_training_data_path` and for each domain, generates a rule that determines which query parameters will be removed. Then, it applies the rule to the data in `url_full_data_path` and removes URL parameters that do not meaningfully change page content. Then it saves cleaned URLs to the `output_data_path`.

The script requires tab-separated values (TSV) files and outputs the same (URLs may contains commas). The URLs to be processed must be called `canonical_url` in the input data `url_training_data_path` and output data `url_full_data_path`.

## Examples
The file `demo_run.py` illustrate an example. The file `demo_input.tsv` illustrates how the prepare the to-be-cleaned URLs data (both the training data and the full data). The file `demo_output.tsv` illustrates the expected processed results.

## License
Apache-2.0
