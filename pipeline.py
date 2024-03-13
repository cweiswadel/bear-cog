import api, aws
from prefect import task, flow

@task
def etl_data():
    api.main()

@task 
def aws_actions():
    """
        aws.py used CLI arguments during testing/development to pass sensitive data
        There would need to be some way to pass (maybe through Prefect) this sensitive data at run time to prevent storage of the tokens/keys/PWDs in the code
        I simply didn't have the time / knowledge how to translate this to Prefect world (if even possible)
        With my attempt to use command line arguments in the py files to ensure hiding of sensitive data, something like the following would be needed to create the prefect deployment
            https://discourse.prefect.io/t/can-i-specify-deployments-using-python-code-rather-than-cli/1458
            Trying to run  prefect deployment build pipeline.py:pipeline -n TestPrefectDeploy was throwing errors because aws.py expects command arguments
    """
    aws.main()

@flow
def pipeline():
    etl_data()
    aws_actions()


"""
Attempted to create a Prefect workflow from the little understanding I built
Ideally we would want to track each scripts execution as a task to monitor and observe results
    If there was more time, each py file would include error handling to generate artifacts or lists showing what values for what records were missing, wrong, or of importance

"""
pipeline()