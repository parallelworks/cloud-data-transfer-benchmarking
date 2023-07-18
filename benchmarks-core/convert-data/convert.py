import json
import os
from dask_jobqueue import SLURMCluster
from dask.distributed import Client
import sys
sys.path.insert(0, '~/benchmarks-core')
import core_helpers as core



# Index that indicates which resource to pull cluster options from
resource_index = os.environ['resource_index']

# Open benchmark information file
with open('benchmark_info.json') as infile:
    inputs = json.loads(infile.read())

# Populate variables from input file
resource = inputs['RESOURCES'][resource_index]
resource_csp = resource['CSP']
dask_options = resource['Dask']
max_nodes = dask_options['MaxNodes']

# Get conversion data
files2convert = []
files2.convert.append([userfile for userfile in inputs['USERFILES']])
for randfile in inputs['RANDFILES']:
    if randfile['Generate']:
        files2convert.append(randfile)

# Instantiate Dask cluser & client
if __name__ == '__main__':
    cores = dask_options['CPUs']
    memory = dask_options['Memory']
    memory = f'{int(memory)} GB'
    dask_dir = '/mnt/shared/dask-worker-logs'

    match dask_options['Scheduler']:
        case 'SLURM':
            cluster = SLURMCluster(account='convert',
                                queue=dask_options['Partition'],
                                job_cpu=cores,
                                cores=cores,
                                memory=memory,
                                processes=1,
                                job_directives_skip=['--mem'],
                                walltime='01:00:00',
                                log_directory=dask_dir
                                    )
    client = Client(cluster)


diag_timer = core.DiagnosticTimer(time_desc='Conversion Time')

for location in 


with diag_timer.time(conversion_type = 'csv2parquet'):
    df = dd.read_csv(root + 'ETOPO1_Ice_g_gmt4.csv', assume_missing=True, header=None, names=names)
    dd.to_parquet(df, root + 'parquetpartitions', name_function=name_function, storage_options={'token':token})
del df







client.close()