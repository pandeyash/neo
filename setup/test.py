def transform(load_source_path,fact_path,archive_path):
    
    # read all file name from the raw folder pointed by load_source_path
    # and transform each file and save in parquet format for futher analysys.
    status_message=''
    status_code=0
    for file_name in glob.glob(load_source_path+'/*.json'):
        print('file to load'+file_name)
    
    for file_name in glob.glob(load_source_path+'/*.json'):
        # open the source file and process
        print('Processed file name:'+file_name)
        try:
            with open(file_name, 'r') as load_file:
                load_data=json.load(load_file)
        
                i=0
                for data in load_data:
                    df1=pd.io.json.json_normalize(data,sep='_') 
                    df1=df1.drop(columns='close_approach_data')
                    df2=pd.io.json.json_normalize(data['close_approach_data'],sep='_')
                    df2["neo_reference_id"]=df1["neo_reference_id"]

                    df3= pd.merge(df1, df2, how='inner', on = "neo_reference_id")
                    if i==0:
                        df_all=df3
                        i+=1
                    else:
                        df_all= df_all.append(df3)
            
                    table = pa.Table.from_pandas(df_all)
                    try:
                        pq.write_to_dataset(table, root_path=fact_path,partition_cols=['close_approach_date'])
                    except:
                        status_code=-1
                        status_message='An error occured while writing data frame into Parquet root path=>'+fact_path+'::Exception=>'+tb.format_exc()
                        return status_code,status_message
          
                try:
               
                    # compress raw file and move to the archive folder 
                    print('archive folder:',archive_path)
                    arc_file=archive_path+'/'+file_name[-15:]+'.gz'
                    print(arc_file)
                    with gzip.open(arc_file, 'wb') as f_out:
                        try:
                            shutil.copyfileobj(load_file, f_out)
                            #remove raw file from the raw folder
                        except:
                            status_code=-1
                            status_message+='Error while compressing the source file=>'+file_name+'.::Exception=>'+tb.format_exc()
                            return status_code,status_message
                        try:
                            print('remove file',file_name)
                            os.remove(file_name)
                        except:
                            status_code=-1
                            status_message+='Error while removing the source file=>'+file_name+' after archiving sccessfully.::Exception=>'+tb.format_exc()
                            print('remove file error',file_name)
                            return status_code,status_message
           
                except:
                    status_code=-1
                    status_message+='Error while archiving the processed file =>'+file_name+'::Exception=>'+tb.format_exc()
                    return status_code,status_message
        except:
            status_code=-1
            status_message='An error occured while reading the file=>'+file_name+'::Exception=>'+tb.format_exc()
            return status_code,status_message
        
    status_code,status_message=0,'Success'
    return status_code,status_message

