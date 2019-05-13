# NEO Challenge solution
# Technology stack: Python, JSON, Parquet,pyarrow, numpy,pandas,and matplotlib.

# Installation and run instructions:

1. create a directory name neo in your system.
2. clone github into the neo
3. cd to the neo directory  (where the requirements.txt is located).
4. run: pip install -r requirements.txt in your shell.
5. Run -> python3 neo_main.py ELT 
	or python3 neo_main.py LOAD
		and python3 neo_main.py TRAN
		
	"ELT" option is for ingest and transform NEO data
	"LOAD" option is for ingest only
	"TRAN" option is for transformation only
	
6. open jupyter notebook NEO_Visualization.ipynb to run data visualization
6.1. Change the Start_date and End_date parameters as needed.
