import requests as req
import json
from  os import path
import neo_utility as util
import traceback as tb



def data_load(neo_url,params,data_load_path,job_log_name,job_status_log_path):

    res=req.get(neo_url, params=params)
    if res.status_code!=200:
        util.job_status_logger(str(res.content)+'::Exception=>'+tb.format_exc(),job_status_log_path)
        return -201,'Error while calling the data pull API.\n The API status code:'+str(res.status_code)+'::Exception=>'+tb.format_exc()

    ## The API call is success. Parse the JSON for each day and create JSON file per extract date.
    raw_data=res.json()
    etl_run_log=[]
    load_file_list=[]
    for key in raw_data['near_earth_objects']:
        file_name=util.create_neo_file_name(key)
        if file_name==None:
            #the Key is not a date key
            continue 
        complete_file_name=path.join(data_load_path, key+".json")  
        try:   
            with open(complete_file_name, 'w') as fout:
                json.dump(raw_data['near_earth_objects'][key], fout)
    
            load_file_list.append(complete_file_name)
        
            etl_run_log.append((str(params['start_date']),str(params['end_date'])))
        except:
            return -202,'Error while writing the data.'+'::Exception=>'+tb.format_exc()

    # The load is completed. Record the extract dates into job run log
    status_log="Total file count="+str(len(load_file_list))+" loaded sucessfully. File details => "+",".join(load_file_list)
    util.job_status_logger(status_log,job_status_log_path)
    util.job_run_loger(etl_run_log,job_log_name)
    return 0,'Success'

