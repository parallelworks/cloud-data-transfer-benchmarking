# Imports
import rand_helper as rh
import subprocess
import os
import json

# Shell Inputs
file_types = tuple(json.loads(os.environ['RANDGEN_FILES']))
file_sizes = tuple(json.loads(os.environ['RANDGEN_SIZES']))
locations = tuple(json.loads(os.environ['RANDGEN_STORES']))

## For now, tokens are generated as empty until an option to add
## cloud storage token location is introduced to the workflow
tokens = []
for i in range(len(locations)):
    tokens.append('None')
tokens = tuple(tokens)


# Preprocess user inputs and determine cloud service
# provide for each URI
preprocessor = rh.preprocessor(locations)
location_info = preprocessor.process()
locations = location_info['Locations']
csps = location_info['CSPs']

# Initialize file generator and list for 
# final locations of written files
generator = rh.randfileGenerator()
rand_filenames = []

# Main file generation loop. Writes requested
# file formats and sizes to all resources used 
# for benchmarking workflow.
for i in range(len(file_types)):
    # Set file format and size for
    # current loop index
    current_file = file_types[i]
    current_size = file_sizes[i]

    # Loop through all cloud storage locations
    for n in range(len(locations)):

        # Set cloud storage location, credential token (can be 'None'
        # if the location doesn't require a token), and cloud service provider
        # of the object store for the current nested loop index
        current_location = locations[n]
        current_token = tokens[n]
        current_csp = csps[n]

        # Pass current variables to the file generator and generate the file based on
        # current file format defined by the main loop
        generator.getInput(current_size, current_location, current_token, current_csp)
        match current_file:
            case "CSV":
                written_filename = generator.csv()
            case "NetCDF4":
                written_filename = generator.netcdf()
            case "Binary":
                written_filename = generator.binary()

        # Record the location of the file written on the current
        # loop iteration
        rand_filenames.append(written_filename)
    # End of nested loop
# End of main loop

# Pass file locations back to main workflow body
os.environ['RAND_FILENAMES'] = json.dumps(tuple(rand_filenames))
subprocess.run(["export RAND_FILENAMES"], shell=True)
# END OF SCRIPT