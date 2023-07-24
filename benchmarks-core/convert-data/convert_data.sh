#!/bin/bash


resource_names=$( jq -r '.RESOURCES[] | .Name' benchmark_info.json )

f_convert() {
    source .miniconda3/etc/profile.d/conda.sh
    conda activate cloud-data
    cd benchmarks-core

    export resource_index=$1
    python convert-data/convert.py
}

resource_index=0
for resource in ${resource_names}
do
    ssh -q ${resource}.clusters.pw "$(typeset -f f_convert); \
                                    f_convert ${resource_index}"
    
    let resource_index++