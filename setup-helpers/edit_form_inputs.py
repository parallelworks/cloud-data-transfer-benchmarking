import json
import os
import copy

# Open `inputs.json`
cwd = os.getcwd()
with open(f'{cwd}/inputs.json') as infile:
    inputs = json.loads(infile.read())



# Set resource input form
resource_input = inputs['RESOURCES']
user = resource_input['Resource']['username']
publicIP = resource_input['Resource']['publicIP']

resources = [{"Name" : resource_input['Resource']['name'],
            "SSH" : f'{user}@{publicIP}',
            "MinicondaDir" : resource_input['MinicondaDir'],
            "CSP" : resource_input['CSP'],
            "Dask" : {"Scheduler" : "SLURM",
                        "Partition" : resource_input['Partition'],
                        "CPUs" : int(resource_input['CPUs']),
                        "Memory" : float(resource_input['Memory']) 
                    }
            }]


# Set storage input form
storage_input = inputs['STORAGE']

# If the credentials are not from google, they must be from
# AWS (will need to be changed if more clouds are supported)
try:
    input_creds = copy.copy(storage_input['gcp_credentials'])
    credentials = {'token' : input_creds}
except:
    input_creds = copy.copy(storage_input['aws_credentials'])
    split_creds = input_creds.split(',')
    credentials = {'key' : split_creds[0], 'secret' : split_creds[1]}

storage = [{"Path" : storage_input["Path"],
            "Type" : storage_input['Type'],
            "CSP" : storage_input['CSP'],
            "Credentials" : credentials
            }]



# Set user files
userfile_input = inputs['USERFILES']
data_vars = userfile_input['nc_data_vars'].split(',')
userfiles = []
if userfile_input["nc_file"]:
    userfiles.append({"Format": "NetCDF4",
                    "SourcePath": storage_input["Path"] + "/" + userfile_input['nc_file'],
                    "DataVars": data_vars,
                    "Type": storage_input["Type"],
                    "CSP": storage_input['CSP'],
                    "Credentials": credentials
                    })

if userfile_input['csv_file']:
    userfiles.append({"Format": "CSV",
                    "SourcePath": storage_input["Path"] + "/" + userfile_input['csv_file'],
                    "DataVars": ['*'],
                    "Type": storage_input["Type"],
                    "CSP": storage_input['CSP'],
                    "Credentials": credentials
                    })



# Set randomly-generated files
randfile_input = inputs['RANDFILES']

# If the user left the file size as 0, do set the generate boolean to
# false for the respective file format
generate_bools = []
for size in [randfile_input['csv_size'], randfile_input['netcdf_size']]:
    if float(size):
        generate_bools.append(True)
    else:
        generate_bools.append(False)

randfiles = [{"Format": "CSV",
              "Generate": generate_bools[0],
              "SizeGB": float(randfile_input['csv_size'])
            },
            {"Format": "NetCDF4",
            "Generate": generate_bools[1],
            "SizeGB": float(randfile_input['netcdf_size']),
            "Data Variables": int(randfile_input['netcdf_dvars']),
            "Float Coords": int(randfile_input['netcdf_dims']),
            },
            {"Resource": resource_input['Resource']['name']}
            ]


# Set file conversion options
convertopts = []
for file in randfiles[:-1]:

    if file['Generate']:
        size = file['SizeGB']
        fformat = file['Format']
        if fformat == 'CSV':
            convertopts.append({"Algorithms": randfile_input['csv_comp_algs'],
                                "Level": 5,
                                "Chunksize": float(randfile_input['csv_chunksize']),
                                "Datasets" : [f'random_{float(size)}GB_CSV']
                                })
        elif fformat == 'NetCDF4':
            convertopts.append({"Algorithms": randfile_input['nc_comp_algs'],
                                "Level": 5,
                                "Chunksize": float(randfile_input['nc_chunksize'])
                                "Datasets": [f'random_{float(size)}GB_NetCDF4.nc']
                                })

for file in userfiles:
    fformat = file['Format']

    sourcepath = file['SourcePath']
    split_path = [ele for ele in sourcepath.split('/') if len(ele)!=0]
    path = split_path[-1]
    if path == '*':
        path = split_path[-2]

    if fformat == 'CSV':
        convertopts.append({"Algorithms": userfile_input['csv_comp_algs'],
                            "Level": 5,
                            "Chunksize": float(userfile_input['csv_chunksize'])
                            "Datasets": [path]
                            })
    elif fformat == 'NetCDF4':
        convertopts.append({"Algorithms": userfile_input['nc_comp_algs'],
                            "Level": 5,
                            "Chunksize": float(userfile_input['nc_chunksize'])
                            "Datasets": [path]
                            })


# Set global options
global_input = inputs['GLOBALOPTS']
globalopts = {"worker_step": global_input['worker_step'],
            "tests": global_input['tests'],
            "Dask": {"Workers": global_input['workers_per_node']},
            "local_conda_sh": global_input['local_conda_sh']
            }


# Write new file
new_inputs = json.dumps({"RESOURCES": resources,
                        "STORAGE": storage,
                        "USERFILES": userfiles,
                        "RANDFILES": randfiles,
                        "CONVERTOPTS": convertopts,
                        "GLOBALOPTS": globalopts
                        })
with open('form_inputs.json', 'w') as outfile:
    outfile.write(new_inputs)