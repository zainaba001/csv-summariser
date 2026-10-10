# CSV Summariser: build files

Files referenced in the implementation run-sheet of the INFS3701 design document.

| File | Used in | Purpose |
|---|---|---|
| `lambda_function_v1.py` | Step 7 | First version of the Lambda code: web page, CSV parsing, statistics, S3 storage and history |
| `lambda_function_v2.py` | Step 12 | Final version: adds a per-browser user key so each user only sees their own files |
| `bucket_policy.json` | Step 10 | Denies every principal except LabRole and voclabs, and denies requests not made over HTTPS |
| `sample_claims.csv` | Step 9 | Sample data (60 insurance claims) for testing the service |

## Before using

- In `bucket_policy.json`, replace `<bucket-name>` with your full bucket name and `<account-id>` with your 12-digit AWS account ID.
- The code reads the bucket name from the `BUCKET_NAME` environment variable, so it needs no editing.
- The code uses only the Python standard library and boto3 (included in the Lambda runtime). Tested on Python 3.14, arm64.
