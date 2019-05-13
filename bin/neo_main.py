import json 
from datetime import datetime,timedelta          
import traceback
from  os import path
import sys as sys

# Custom modules for helper functions
import neo_utility as util
import neo_data_load as data_load
import neo_data_process as data_proc

#****************************


def main():
        
    #Job run log file full name
    job_log_name=path.join(job_run_log_path, load_run_log_name)
   
    # Using start date same as last run date to catch any fallout in the last run. 
    # If the sysdate was used as start date for the previous run
    start_date=util.last_run_date(job_log_name)
    if start_date==None:
        #The last run log not exist. 
        #Set the data load default start date as start date
        start_date=default_start_date
   
    
    #Incremental load will be last run date + 1.
    # Set end date to start date +1
    end_date=start_date + timedelta(days=1)
    
    # End setup
    
    '''
    Start data extraction, load, and transformation jobs.
    
    Job Run options :
    
        1) Default run type is Extract, Load, and Transformation =>ELT
        2) Extract and Load only =>LOAD
        3) Transformation only  =>TRAN
    
    '''
    # set default run type
    run_type='ELT'
   
    
    if len(sys.argv)>1:
        run_type=sys.argv[1].upper()
    
        
        
    if run_type in ('ELT', 'LOAD'):
        # Run NEO data load job
        # Setup NEO API's parametes
        neo_param = {'api_key': neo_key, 'start_date': start_date,'end_date':end_date}
        
        status_code,status_message=data_load.data_load(neo_url=neo_url,\
                                                       params=neo_param,\
                                                       data_load_path=data_load_path,\
                                                       job_log_name=job_log_name,\
                                                       job_status_log_path=job_status_log_path)
        if status_code!=0:
            return status_code,status_message
        print('New Data load completed successfully.Start data:'+str(start_date)+' End data:'+str(end_date))
    # end NEO data load
    
    if run_type in ('ELT', 'TRAN'):
        # Run NEO data trasformation job
        status_code,status_message=data_proc.transform(load_source_path=data_load_path,\
                                                       fact_path=neo_fact_path,\
                                                       archive_path=neo_archive_path)
        if status_code!=0:
            return status_code,status_message
        print('Data transformation completed successfully.')
        
    if run_type not in ('LOAD','ELT', 'TRAN'):
        status_code= -101 
        status_message='Job type is not defined'
    return (status_code,status_message)



## NEO project load and transform driver program script

if __name__ == "__main__":
    # Load setup file and initialise all setup variables
    setup_file="../setup/neo_setup.json"
    # read system parameters from the setup file
    
    try:
        with open(setup_file, 'r') as setup_file:
            setup_data=json.load(setup_file)
    except:
        print ('Error Code:',-102)
        print('Error is opening the setup file.\
                        Please check the setup file location: neo/setup/neo_setup.json')
        
    
    # Initialize setup properties
    
    neo_key=setup_data['neo_API']['key']
    neo_url=setup_data['neo_API']['url']
    data_load_path=setup_data['data_load']['data_path']
    load_run_log_name=setup_data['data_load']['run_log_name']
    default_start_date=datetime.strptime(setup_data['data_load']['default_start_date'], '%Y-%m-%d').date()
    job_run_log_path=setup_data['process']['run_log_path']
    job_status_log_path=setup_data['process']['job_status_log_path']
    neo_fact_path=setup_data['data_tran']['fact_path']
    neo_archive_path=setup_data['data_tran']['archive_path']
   
    # end setup initialization
   
    #call main mudule
    status_code,status_message=main()
 
    if status_code !=0:
        sys_error=traceback.format_exc()
        status_message+='::Exception message=>'+sys_error
        util.job_status_logger(status_message,job_status_log_path)
        print(status_message)
    else:
        print('Success')
 
#end of file#