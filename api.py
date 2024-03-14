# https://coderbyte.com/question/software-engineer-data-engineer-assessment-h1lrnjpodk
import aiohttp, asyncio, json
from pyarrow import json as paj
import pyarrow.parquet as pq
from datetime import datetime
from pathlib import Path
from prefect import flow, task

#define some dirs to create files for validation/human readable
## Path execution is to create the dir if it doesn't exist
xFormOutput_dir = "./xFormOutputs/"
Path(xFormOutput_dir).mkdir(parents=True, exist_ok=True)
parqOutput_dir = "./parqOutputs/"
Path(parqOutput_dir).mkdir(parents=True, exist_ok=True)
errorOutput_dir = "./errorOutputs/"
Path(errorOutput_dir).mkdir(parents=True, exist_ok=True)

#Define the URLs for the endpoints of the SpaceX API
# launches url returns a list of all launches
# rocket url returns a single rocket based on a given ID
# payload url returns a single payload based on a given ID
launches_url = "https://api.spacexdata.com/v4/launches"
rocket_url = "https://api.spacexdata.com/v4/rockets/{rocket_id}"
payload_url = "https://api.spacexdata.com/v4/payloads/{payload_id}"

#last minute idea for error handling, at least document/show when a key field is missing
def append_error(launch_obj_id, errorField):
    if errorField == "payload":
        with open(errorOutput_dir+"payloadErrors.txt",'a') as file:
            file.write(f"Launch ID: {launch_obj_id} has no payload(s)!\n")
    elif errorField == "rocket":
        with open(errorOutput_dir+"rocketErrors.txt", 'a') as file:
            file.write(f"Launch ID: {launch_obj_id} has no rocket!\n")
    return

#trim launch object to reduce the amount of fields being sent to the parquet
## there are objects and lists of objects in the api response that maybe CANNOT be saved in the parquet
def trim_launch_details(launch_obj):
    #define the key values to capture from the launch object
    keys = ['id','name','date_utc', 'date_local', 'success','window','failures', 'details', 'flight_number', 'upcoming', 'launch_library_id']
    trimmed_launch_obj = {
        'rocket':launch_obj['rocket'],
        'payloads':launch_obj['payloads'] 
    }
    for key in keys:
        trimmed_launch_obj["launch_"+key] = launch_obj[key]

    return trimmed_launch_obj

#take the rocket id from the launch object and replace that key-value with the rocket obj
#the rocket obj is a trimmed version of the true rocket endpoint response (again thinking about space/storage)
async def add_rocket_to_launch(client: aiohttp.ClientSession, launch_obj, id):
    if id != None:
        async with client.request('GET', rocket_url.format(rocket_id = id)) as response:
            rocket_respBody = await response.json()
            #trim rocket object to just rocket_XYZ key-value pairs for storage in the parquet
            keys = ['id','name','type','stages']
            rocket_obj={}
            for key in keys:
                rocket_obj["rocket_"+key] = rocket_respBody[key]
            launch_obj['rocket'] = rocket_obj
    else:
        #expect an empty rocket id, generate an error 'report'
        append_error(launch_obj_id=launch_obj['id'], errorField="rocket")
    return launch_obj

#take the list of payload IDs in the launch object and replace those key-values in the list with a payload object
## the payload object is built from querying the payload endpoint for the values
async def add_payloads_to_launch(client: aiohttp.ClientSession, launch_obj, payload_id_list):
    if payload_id_list != []:
        print(launch_obj['id'], " contains payloads: ", payload_id_list )
        
        #create an abbreviated version of the payload object
        keys = ['id', 'name', 'type', 'orbit', 'reference_system' ]
        payloads_respBody_list = []
        for payload in payload_id_list:
            payload_obj ={}
            async with client.request('GET', payload_url.format(payload_id = payload)) as response:
                payload_respBody = await response.json()
                for key in keys:
                    payload_obj["payload_"+key] = payload_respBody[key]
                payloads_respBody_list.append(payload_obj)
        launch_obj['payloads'] = payloads_respBody_list
    else:
        #create an 'error' log
        append_error(launch_obj_id=launch_obj['id'], errorField="payload")
    return launch_obj

#run a one time pull from the endpoint that returns all launches
## store this locally as a 'raw' file for validation
async def get_launches(client: aiohttp.ClientSession):
    async with client.get(launches_url) as response:
        launches_respBody = await response.json()

        with open("api_resp_raw.json","w") as file:
            file.write(json.dumps(launches_respBody,indent=2)) #write as json because its easy to read and comfortable for me haha
        print("responseBody length", len(launches_respBody))
        return launches_respBody

#define a fn to write each to-be-parquet to json for easier conversion to parquet
@task
def write_json_file(output_dir, fileName, data_to_write):
    with open(f"{output_dir}{fileName}.json","w") as file:
        return file.write(json.dumps(data_to_write,indent=2))

#
async def write_to_parquet(inFile_dir, fileYear, fileName):
    #something like write_to_parquet() to actually write the data to the correct format
    # inspiration to write to parquet:
        # https://stackoverflow.com/questions/32940416/methods-for-writing-parquet-files-using-python
        # https://saturncloud.io/blog/how-to-write-data-to-parquet-with-python/   
        # have never used parquet before
    inFileName = inFile_dir+fileName+".json"
    outFile_dir = await organize_records(fileYear)
    outfileName = outFile_dir+fileName+".parquet"
    table = paj.read_json(inFileName)
    return pq.write_table(table, outfileName)

@task
async def organize_records(fileYear):
    # take the file generated from create_record() 
    # determine where it needs to go--> a directory per year
    ##  create the directories if they do not exist
    # add a file to the correct year and make sure the naming convention forces the files into a sort
    outputYr_dir = parqOutput_dir+"year="+str(fileYear)+"/"
    Path(outputYr_dir).mkdir(parents=True, exist_ok=True)
    return outputYr_dir

#the actual 'pipeline' for gathering the data from the api and transforming it prior to loading it to AWS
@flow
async def create_records():
    async with aiohttp.ClientSession() as session:
        #call to get the launches data
        launches_data = await get_launches(session)

        tasks = []
        for item in launches_data: #for each launch in the pulled response do the following transformation
            #store the launch date (from the utc field) for determination of where to organize it
            launchDt_str = item['date_utc']
            launchDt = datetime.strptime(launchDt_str, '%Y-%m-%dT%H:%M:%S.%fZ')
            fileYr = launchDt.year #capture the year from the datetime str 

            fileName = f"launch_parquet.{launchDt.strftime('%Y-%m-%dT%H.%M.%S')}"
            print("Processing fileName: ", fileName+".json with fileYr: ", fileYr)

            #create an asyncio task 'loop' to run the async calls to the rocket and payloads endpoint
            ## in each add_x_to_launch it will replace the respective key-value pair with the appropriate object 
            tasks.append(asyncio.create_task( add_rocket_to_launch(session, launch_obj=item, id=item['rocket']))) #replaces the rocket_id in the launch obj with the actual data from the rocket endpoint
            tasks.append(asyncio.create_task( add_payloads_to_launch(session, launch_obj=item, payload_id_list=item['payloads']))) #replaces the payload_ids in the launch ob with the actual data from the payload endpoint
            await asyncio.gather(*tasks, return_exceptions=True)
            
            #remove any 'un-needed' fields from the launch object
            item = trim_launch_details(item) 

            #write the file to json (to easily write to parquet for loading)
            write_json_file(output_dir=xFormOutput_dir, fileName=fileName, data_to_write=item)            
            
            #write the file to parquet FROM json, and organize it based on the fileYr
            await write_to_parquet(inFile_dir=xFormOutput_dir, fileYear=fileYr, fileName=fileName)
    return

# def main():
if __name__=="__main__":
    asyncio.get_event_loop().run_until_complete(create_records())

