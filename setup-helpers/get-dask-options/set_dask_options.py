import json
import os

with open('inputs.json', 'r') as infile:
    user_input = json.loads(infile.read())

resource_dask_opts = [resource['Dask'] for resource in user_input['RESOURCES']]
global_dask_opts = user_input['GLOBALOPTS']['Dask']

global_dask_opts['CPUs'] = min([opt_set['CPUs'] for opt_set in resource_dask_opts])
global_dask_opts['Memory'] = min([opt_set['Memory'] for opt_set in resource_dask_opts])
global_dask_opts['MaxWorkers'] = min([global_dask_opts['Workers']*opt_set['MaxNodes'] for opt_set in resource_dask_opts])

updated_json = json.dumps(user_input)
with open('inputs.json', 'w') as outfile:
    outfile.write(updated_json)