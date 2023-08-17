import json
import os

# Open user input file
input_file = str(os.environ['input_file'])


with open(input_file, 'r') as infile:
    user_input = json.loads(infile.read())

# Set index and max node count from environment variables
index = int(os.environ['index'])
max_nodes = int(os.environ['max_nodes'])

# Append `MaxNodes` field to the resource referenced in
# the current loop iteration of `getmax.sh`
current_dask = user_input['RESOURCES'][index]['Dask']
current_dask['MaxNodes'] = max_nodes

# Update `inputs.json` with new fields
updated_json = json.dumps(user_input)
with open(input_file, 'w') as outfile:
    outfile.write(updated_json)