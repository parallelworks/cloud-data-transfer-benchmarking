# cloud-data-transfer-benchmarking
## Overview
This repository is the start of a comprehensive benchmarking workflow designed to measure speeds of writing legacy file formats (e.g., NetCDF & CSV) to cloud storage, converting them to cloud-native formats (e.g., Zarr, Parquet, TileDB Embedded), reading both file classes, and performing operations using cloud-cluster computing and cloud storage. With data sizes only getting larger, pairing cloud storage and computing together is a promising method for speeding up access and analysis of larger-than-memory datasets. This approach is referred to as "data-proximate computing": in other words, by keeping cloud data storage and computing close to each other, theoretically, computation power can be scaled near-infinitely from any location. Critically, this bypasses the need for building new and improved on-premise supercomputers and copying data to new disks for distribution between users. However, despite all the advantages of data-proximate computing, the data transfer speeds observed during operation are heavily impacted by the format of the data and how the formats are configured in storage. "Legacy file formats" such as NetCDF and CSV are slow in the cloud, while novel formats (dubbed "cloud-native") like Zarr and Parquet offer lightning-fast speeds when configured correctly. Thus, the objective of this benchmarking is to offer an automated approach for:

- Measuing data read and computation throughput for legacy data formats
- Comparing the legacy transfer speeds with those of cloud-native formats
- Analyzing any gains or drops in performance when configuring cloud-native formats to store with different chunk sizes and compression algorithms
- Testing different clouds to see how performance is altered

This benchmarking offers both normal workflow execution using the PW platform **(not ready yet)** and an interactive Jupyter notebook to visualize each step. For users new to data-proximate computing it is recommended that the interactive version be explored first, as it offers expository prose designed to help further one's understanding of the topic.

## Before Running the Workflow

NOTE: If you are supplying a single-file dataset that needs to be transferred across clouds (e.g., Google to AWS), and you desire for the workflow to handle this transfer for you, **its size must not exceed 5.4 GB (5 GiB)**. The tool the workflow uses for the transfer, `gsutil cp`, does not support single-file cross-cloud uploads larger than the posted size. In this event, you must manually transfer the file over to the buckets you wish to use in this benchmarking. Additionally, since accessing multiple buckets using the `export AWS_ACCESS_KEY_ID` and `export AWS_SECRET_ACCESS_KEY` method isn't supported, you will need to pre-transfer files between S3 buckets.

### For Google Buckets
This workflow currently only supports authentication into Google buckets through [service accounts](https://cloud.google.com/iam/docs/service-account-overview). Before executing the workflow, you must make sure that you:
 - Have a service account with permissions to read, write, list, and put objects to the desired bucket(s)
 - Authenticate the service account in your user container with [`gcloud auth activate-service-account <service-account>@<domain>.com --key-file=</path/to/key.json>`](https://cloud.google.com/sdk/gcloud/reference/auth/activate-service-account).

### For AWS S3 Buckets
For S3 buckets, the workflow handles authentication using the [method described in the Parallel Works docs](https://docs.parallel.works/storage/transferring-data-aws)  For this benchmarking, you must:
- Have a bucket policy with `PutObject`, `GetObject`, and `ListBucket`
- Have your AWS access key ID and AWS secret access key available to enter into the workflow as in input.

## Jupyter Notebook Version Setup Guide
Before interacting with any benchmark code, you must first start an interactive session in your user container:
1. Navigate to the `WORKFLOWS` tab in the PW platform and click on Jupyter (if you do not have this workflow added, click on your profile in the upper right-hand corner of the screen, and then go to Marketplace. Find the Jupyter workflow, and click "Add Parallel Workflow")
2. Click `Run Workflow`, and enter the fields as shown below (use the default conda path & environment, or choose your own):
![image](image.png)
3. Wait for the 