from modules.finnishModules import Finland
import os

# make a subdirectory called finland within a directory called data
path = os.path.join('data', 
                    'finland')
os.makedirs(path,
            exist_ok = True)

finland_instance = Finland()
finland_instance.make_request()
finland_instance.data_to_BQ()