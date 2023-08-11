import json
import os

with open('inputs.json', 'r') as infile:
    user_input = json.loads(infile.read())

resource_dask_opts = [resource['Dask'] for resource in user_input['RESOURCES']]
global_dask_opts = user_input['GLOBALOPTS']['Dask']

global_dask_opts['CPUs'] = min([cpus for cpus in resource_dask_opts['CPUs']])
global_dask_opts['Memory'] = min([mem for mem in resource_dask_opts['Memory']])
global_dask_opts['MaxWorkers'] = min([global_dask_opts['Workers']*max_nodes for max_nodes in resource_dask_opts['MaxNodes']])

updated_json = json.dumps(user_input)
with open('inputs.json', 'w') as outfile:
    outfile.write(updated_json)