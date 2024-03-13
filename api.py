# https://coderbyte.com/question/software-engineer-data-engineer-assessment-h1lrnjpodk
import aiohttp, asyncio, json
from pyarrow import json as paj
import pyarrow.parquet as pq
from datetime import datetime
from pathlib import Path
from prefect import flow, task

xFormOutput_dir = "./xFormOutputs/"
Path(xFormOutput_dir).mkdir(parents=True, exist_ok=True)
parqOutput_dir = "./parqOutputs/"
Path(parqOutput_dir).mkdir(parents=True, exist_ok=True)

launches_url = "https://api.spacexdata.com/v4/launches"
rocket_url = "https://api.spacexdata.com/v4/rockets/{rocket_id}"
payload_url = "https://api.spacexdata.com/v4/payloads/{payload_id}"


# #capture only 'necessary data above'
# def capture_launch_details(launch_obj):


#     trimmed_launch_obj = {
#         "launch_id" : launch_obj['id'],
#         "launch_name" : launch_obj['name'],
#         "launch_dt_utc" : launch_obj['date_utc'],
#         "launch_dt_local" : launch_obj['date_utc'],
#         "launch_rocket_id" : launch_obj['rocket'],
#         "launch_payloads_list" : launch_obj['payloads'], #List of Ids of payloads
#         "launch_success" : launch_obj['success'], #T/F
#         "launch_failures" : launch_obj['success'], #List of objects with keys: time, altitude, reason 
#     }

#     return trimmed_launch_obj


async def add_rocket_to_launch(client: aiohttp.ClientSession, launch_obj, id):
    async with client.request('GET', rocket_url.format(rocket_id = id)) as response:
        # print(rocket_url.format(rocket_id=id))
        # print("get_rocket(id) status code: ", response.status)

        rocket_respBody = await response.json()
        print(launch_obj['id'], " contains rocket: ", id )

        #trim rocket object to just rocket_XYZ key-value pairs for storage in the parquet
        keys = ['id','name','type','stages']
        rocket_obj={}
        for key in keys:
            # launch_obj["rocket_"+key] = rocket_respBody[key]
            rocket_obj["rocket_"+key] = rocket_respBody[key]
        # del launch_obj['rocket']
        launch_obj['rocket'] = rocket_obj
    return launch_obj

async def add_payloads_to_launch(client: aiohttp.ClientSession, launch_obj, payload_id_list):

    if payload_id_list != []:
        print(launch_obj['id'], " contains payloads: ", payload_id_list )
        payloads_respBody_list = []
        keys = ['id', 'name', 'type', 'orbit', 'reference_system' ]

        for payload in payload_id_list:
            payload_obj ={}
            async with client.request('GET', payload_url.format(payload_id = payload)) as response:            
                # print("get_payload(id) status code: ", response.status)

                payload_respBody = await response.json()
                # print("payload_respBody['id']: ", payload_respBody['id'])
                # payloads_respBody_list.append(payload_respBody)
                for key in keys:
                    payload_obj["payload_"+key] = payload_respBody[key]
                payloads_respBody_list.append(payload_obj)

        launch_obj['payloads'] = payloads_respBody_list
    else:
        print(launch_obj['id'], " contains: NO PAYLOADS" )
    return launch_obj

async def get_launches(client: aiohttp.ClientSession):
    async with client.get(launches_url) as response:
        # print("get_launches() status code: ", response.status)
        # print("Headers: ", response.headers)

        # launches_respBody = await response.text()
        launches_respBody = await response.json()

        with open("api_resp_raw.json","w") as file:
            file.write(json.dumps(launches_respBody,indent=2))
        # print(json.dumps(responseBody,indent=2))  
        print("responseBody length", len(launches_respBody))
        return launches_respBody

@task
def write_json_file(output_dir, fileName, data_to_write):
    with open(f"{output_dir}{fileName}.json","w") as file:
        return file.write(json.dumps(data_to_write,indent=2))

async def write_to_parquet(inFile_dir, fileYear, fileName):
    #something like write_to_parquet() to actually write the data to the correct format
    # https://saturncloud.io/blog/how-to-write-data-to-parquet-with-python/
    # inspiration to write to parquet: https://stackoverflow.com/questions/32940416/methods-for-writing-parquet-files-using-python
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
    # add a file to the correct year and make sure the naming convetion forces the files into a sort
    outputYr_dir = parqOutput_dir+"year="+str(fileYear)+"/"
    Path(outputYr_dir).mkdir(parents=True, exist_ok=True)
    return outputYr_dir

@flow
async def create_records():
    #pass in a launch object
    # build the object, calling for rocket and payload info as needed
    # compose the rocket and payload info into the output_obj
    # write the output_obj to a file 
    async with aiohttp.ClientSession() as session:
        launches_data = await get_launches(session)

        tasks = []
        for item in launches_data:
            #"date_utc": "2006-03-24T22:30:00.000Z",
            launchDt_str = item['date_utc']
            launchDt = datetime.strptime(launchDt_str, '%Y-%m-%dT%H:%M:%S.%fZ')
            
            fileName = f"launch_parquet.{launchDt.strftime('%Y-%m-%dT%H.%M.%S')}"
            fileYr = launchDt.year
            print("fileName: ", fileName+".json with fileYr: ", fileYr)
            tasks.append(asyncio.create_task( add_rocket_to_launch(session, launch_obj=item, id=item['rocket']))) #replaces the rocket_id in the launch obj with the actual data from the rocket endpoint
            tasks.append(asyncio.create_task( add_payloads_to_launch(session, launch_obj=item, payload_id_list=item['payloads']))) #replaces the payload_ids in the launch ob with the actual data from the payload endpoint
            await asyncio.gather(*tasks, return_exceptions=True)
            
            write_json_file(output_dir=xFormOutput_dir, fileName=fileName, data_to_write=item)            
            # await write_to_parquet(inFile_dir=xFormOutput_dir, outFile_dir=organize_records(fileYear=fileYr), fileName=fileName)
            await write_to_parquet(inFile_dir=xFormOutput_dir, fileYear=fileYr, fileName=fileName)
    return


#pseudo code
##  create a async fn to get all launches from endpoint
##  create a secondary async fn to get the rocket info from rocket endpoint
##  create a tertiary async fn to get the payloads from the payload endpoint
### for any endpoint call, fill in the missing data with None, and create a seperate 'error' file
### In this 'error' file, call out which entries have what fields missing
##  'wrap' all of these API calls into a single write function to create the 'result' file 
## convert this 'result' file into the format for Parquet --> partition the entries for Parquet structure
## Send/write the structured files to S3
## 'Wrap' all of this in a Prefect workflow
            
##overall structure inspiration: https://stackoverflow.com/questions/53199248/get-json-using-python-and-asyncio
def main():
    asyncio.get_event_loop().run_until_complete(create_records())

