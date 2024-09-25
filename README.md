# AWS Document Analyzer

This Python application uploads PDF files to an AWS S3 bucket, triggers a Lambda function for processing, and downloads the resulting CSV files. The app filters and extracts relevant key-value pairs from the CSV file using fuzzy matching and stores the final results in a new CSV file.

# What I Learned

- Uploading and downloading files from AWS S3 using boto3
- Using fuzzy matching with fuzzywuzzy for string comparison
- Working with CSV files in Python using pandas
- Automating file processing with S3 buckets and Lambda functions
- Implementing time-based polling to check the status of asynchronous processes
