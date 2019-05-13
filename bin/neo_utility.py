from datetime import datetime,timedelta 
import csv
import traceback as tb

def create_neo_file_name(key):
     try:
            #chek if the key is a date in 'yyyy-mm-dd' format
        return str(datetime.strptime(key, '%Y-%m-%d').date())     
     except:
        return none

def last_run_date(job_log_name):
    last_line_date=None
    try:
        with open(job_log_name,'r') as f:
            for line in csv.reader(f):
                last_line_date=line[1] 
    except:
        return None
    if last_line_date!=None:
        try:
            last_run_date=datetime.strptime(last_line_date, '%Y-%m-%d').date()
        except:
            pass
    return last_run_date

def job_run_loger(etl_run_log,job_log_name):
    with open(job_log_name,'a') as fd:
        writer = csv.writer(fd)
        writer.writerows(sorted(etl_run_log,key=lambda x:datetime.strptime(x[0], '%Y-%m-%d')))

def job_status_logger(log_str,log_path):
    file_name=log_path+'/log:'+str(datetime.today()).replace(" ",":")+'.txt'
    error_log = open(file_name,"w") 
    error_log.writelines(log_str)
    error_log.close() 
