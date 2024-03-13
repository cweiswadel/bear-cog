# Pipeline Architecture

## api.py - collect data from APIs

## aws.py - create and load resources onto AWS

# Data Partitioning Strategy

## Pull data and create a 'raw' file --> human readable because dummy

## Mash the data together to create a single record with required fields (launch data, rocket data, payload(s) data)

## Sort data locally into sub directories based on file year (for human readable validation)

## Convert to parquet file for easy writing to AWS

# Operational Procedures

## Lacking knowledge with Prefect

## Translate it to how I would do it if it were with GitLab Runners AND TRY TO CORRELATE TO PREFECT STRUCTURE
