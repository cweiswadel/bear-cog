# Pipeline Architecture

## api.py - collect data from APIs

* This `api.py` file is to extract and transform the data pulled from the SpaceX APIs
  * It can be called simply with `python api.py`
  * If the `prefect` workflow was successfully set up, I think it would be best to be set up as a flow
    * Where we would define the API calls to the rocket and payload endpoints, the trimming of data, etc. as the tasks
    * In a production environment, it would be on a schedule, and the overall execution would be included as part of a bigger flow

## aws.py - create and load resources onto AWS

* This `aws.py` is to create and load resources into AWS based on the extracted and transformed data from `api.py`
  * The overall flow is to:
    * Create a S3 bucket if one does not exist
    * Load all the locally created parquet files (created via `api.py`) into the S3 bucket
      * With special care to preserve the data partitioning created locally
    * Then with all the files in S3, read the S3 objects (files) into a pandas dataframe(`df`)
    * Take this `df` and write it via SQL statement to RDS
      * **However this is where I could not get it to work, see [Lessons Learned](#lessons-learned) for more information**
  * It was set up as a command line argument based python file, because my experience is more with Containers and GitLab shared runners
    * To run the `aws.py` file successfully you would need to have the following:
    ```console
    $ python aws.py --awsKeyId=<AWS Key ID> --awsKeySecret=<AWS Access Secret> --rdsPW='<RDS/DB password>'
    ```
      * Assuming 
        1. You have access to the AWS instance with your Access Secret
        2. `database_name`, `table_name`, `rds_host`, `rds_port`, and `rds_user` all align with the configuration of your AWS RDS DB instance too
          * I am not too experienced with AWS to know all the ins and outs of how to handle these values effectively
    * This command line argument approach did lead to an issue with creating a prefect deployment because of the overlapping commands/options
      * See [Lessons Learned](#lessons-learned) for more details 

# Data Partitioning Strategy

* Very simply, the partitioning was set based on the year of the file
  * This was easily pulled from the `date_utc` field in the launch object/dictionary 
    * Parsing this field after converting it to a `date str` allowed saving the variable `fileYr` in further functions that manipulated the launches data
  * Using the `fileYr` directories were created locally and since the `launches` endpoint contained the files in DESC order of date_utc
    * This effectively sorted the data locally (and thus on the S3 Bucket)
* On the DB side, it would have been a loose preservation of the schema/partitioning that occurred in the Extract and Transform stages
  * I say loose because we would still have `date_utc` (and even `date_local`) to sort and group the results by
  * BUT we wouldn't store the year as a column value in the record 
    * In my opinion, we shouldn't because that would just be more spaced used up when it already exists in another column (but it may be a parquet thing to do this)
![S3Bucket](/MD/S3Bucket.png)
![S3Bucket2](/MD/S3Bucket2.png)


# Data Transformation Approach

* The approach was taken to pull all `launches` data, and then transform it via different functions
  * There is a huge [Lesson Learned](#lessons-learned) here because there was probably TOO much being done to create a human readable record in the Extract and Transform phase
  * Ultimately, it does create an effective human-readable, locally-based organization of data
    * And a good learning experience for me haha
  * The reasoning for this approach was because of the sampling of data I did when viewing the structure of the launch object
    * It seemed that there was always a rocket ID
      * Then based on the `rocket` endpoint, each `rocket` was an object with easy to decipher fields: `name`, `type`, `stages`, etc.
    * It seemed that there was always a list of payloads
      * Sometimes this list would be empty, so this needed to be accounted for
      * Then based on the `payload` endpoint, each `payload` was an object with easy to decipher fields: `name`, `type`, `orbit`, etc.
    * So the general thought was nest these paired down objects (or list of objects in the case of `payloads`) into the launch object
      * And I was aware that there is the ability (at least in SQL server and Oracle SQL) that you can store values as JSON/XML
        * This seemed like an easy extension of that column data type

## Conversion to parquet file for easy writing to AWS

* I had never worked with parquet files before, and it took some extensive googling to understand and/or manipulate data into/as them
  * The concept is clear now where you store data not traditionally but as chunks of data that are more similar to SQL/RDBMS columns for optimization
    * As I became more familiar with them, I realized that my approach was flawed
    * I needed to think less human readable, but more RDBMS oriented

# Operational Procedures

## Lacking knowledge with Prefect

* Similar to my level of familiarity with parquet files, I had never been exposed to prefect before
  * It is something that is more familiar to me know, as I can relate it to other `.yaml` based deployments, pipelines, build procedures, whatever, etc. etc.
  * My approach for creating a prefect like flow/automation would be as follows:
    * Within GitLab, there is the [GitLab Runner](https://docs.gitlab.com/runner/) 
    * You can trigger these runners to do any script execution based on a `.yml` file
      * This is traditionally and commonly used to automate devops / git procedures
      * And you can set a schedule to trigger a pipeline that will execute on a runner
    * A runner runs an executor that will pull from a given `image:` to run your script
    * Specifically for this task I would have chosen a Docker image that contained:
      * Python 3.11
      * All the pip installed dependencies found in `requirements.txt` 
      * The MySQL connector/driver for accessing the RDS MySQL DB
    * Then I would have set up the `script:` section to run the following (in order):
      1. `api.py`
      2. `aws.py`
      * There would NOT be as much granularity in the reporting of the job as anything in the same `script` execution gets lumped into one's job output
        * So in prefect world, instead of having one overall flow, with the two scripts each being a 'sub-flow' that each had their own tasks, we would only have one flow
* I was able to play around with prefect enough to create the localhost server
  * Within this server, I could see the tasks and flows and their various reported status(es)
  ![PrefectServer](/MD/PrefectServer.png)


# Lessons Learned

## What went wrong

1. Not enough thought/understanding of how to structure the end schema (to be written to RDS/DB)
2. When extracting/transforming launch data, I was doing too much manipulation of data in an effort to make it human readable
3. Command line argument approach for `aws.py`

## Possible Solutions

1. It is more clear now that the structure of the parquet file and of the over per-launch object data should have been more structured like a RDBMS row
    * More thought should have been put into converting fields from the launch object into single data/column type entities
    * In the case where an entire object sits as a value of a field, then this most likely needs to be converted into its own stand alone table
      * Ex. For each `launch['rocket']` id present, a call should be made to the `rocket` endpoint to extract that rocket object, and then this rocket object should be stored locally
        * This list of locally stored rocket objects would then be written to RDS as a 'supporting' table to allow joins against if there is a need for clarity on the rocket data
    * It is a tough scenario, because if it is being forced into a RDBMS storage of data, this should be done to move the complexity to the data layer and not the 'application/processing' layer
      * However if there was the option of NoSQL, then it would make perfect sense to store the launch data as a JSON/python-dict like object
2. There is most likely some more clever way (or a pre-built) library out there that allows for storing of an object to memory and then writing that to a parquet file
    * It lost time and overall processing power to have to constantly write extracted data to a file, when it could most likely be done directly to the destination (S3 bucket)
    * However, from my experience having sanity checks, and resources to validate different stages of development are crucial to creating a better understanding of the system/topic
3. This was set up to prevent the exposure and saving of sensitive credentials 
    * My experience is very much within GitLab where there is the ability to store credentials that can be passed in as variables during runtime of scripts
      * So using this experience, I developed the `aws.py` file to take in command line arguments 
      * However with the lack of a working prefect deployment, and seeing that prefect creates a polling service/connection to the target infrastructure
        * It would have made sense to develop the code in a manner where an environment variable or config file is read from as needed for the credentials/secrets
        * I would feel comfortable with doing this either via:
          * `os.environ[<VAR NAME>]` for reading an environment variable that would contain sensitive data OR 

            ```python
            import os
            from dotenv import load_dotenv

            load_dotenv()

            var = os.getenv('<VAR NAME>')
            ```

    * Based on some research it seems that AWS supports some sort of credential file that may have similar effects