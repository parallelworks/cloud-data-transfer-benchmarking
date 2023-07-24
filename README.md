# cloud-data-transfer-benchmarking
## Overview
This repository is the start of a comprehensive benchmarking workflow designed to measure speeds of writing legacy file formats (e.g., NetCDF & CSV) to cloud storage, converting them to cloud-native formats (e.g., Zarr, Parquet, TileDB Embedded), reading both file classes, and performing operations using cloud-cluster computing and cloud storage. With data sizes only getting larger, pairing cloud storage and computing together is a promising method for speeding up access and analysis of larger-than-memory datasets. This approach is referred to as "data-proximate computing": in other words, by keeping cloud data storage and computing close to each other, theoretically, computation power can be scaled near-infinitely from any location. Critically, this bypasses the need for building new and improved on-premise supercomputers and copying data to new disks for distribution between users. However, despite all the advantages of data-proximate computing, the data transfer speeds observed during operation are heavily impacted by the format of the data and how the formats are configured in storage. "Legacy file formats" such as NetCDF and CSV are slow in the cloud, while novel formats (dubbed "cloud-native") like Zarr and Parquet offer lightning-fast speeds when configured correctly. Thus, the objective of this benchmarking is to offer an automated approach for:

- Measuing data read and computation throughput for legacy data formats
- Comparing the legacy transfer speeds with those of cloud-native formats
- Analyzing any gains or drops in performance when configuring cloud-native formats to store with different chunk sizes and compression algorithms
- Testing different clouds to see how performance is altered

This benchmarking offers both normal workflow execution using the PW platform and an interactive Jupyter notebook to visualize each step. For users new to data-proximate computing it is recommended that the interactive version be explored first, as it offers expository prose designed to help further one's understanding of the topic.

## Before Running the Workflow

NOTE: If you are supplying a single-file dataset that needs to be transferred across clouds (e.g., Google to AWS), and you desire for the workflow to handle this transfer for you, **its size must not exceed 5.4 GB (5 GiB)**. The tool the workflow uses for the transfer, `gsutil cp`, does not support single-file cross-cloud uploads larger than the posted size. In this event, you must manually transfer the file over to the buckets you wish to use in this benchmarking.

### For Google Buckets
This workflow currently only supports authentication into Google buckets through [service accounts](https://cloud.google.com/iam/docs/service-account-overview). Before executing the workflow, you must make sure that you:
 - Have a service account with permissions to read, write, list, and put objects to the desired bucket(s)
 - Authenticate the service account in your user container with [`gcloud auth activate-service-account <service-account>@<domain>.com --key-file=</path/to/key.json>`](https://cloud.google.com/sdk/gcloud/reference/auth/activate-service-account).

### For AWS S3 Buckets
For S3 buckets, the workflow handles authentication with [configuration files](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html) located in the `~/.aws` directory of your user container filesystem. For this benchmarking, you must:
- Have a bucket policy with `PutObject`, `GetObject`, and `ListBucket` (See [cross-account access](https://docs.aws.amazon.com/AmazonS3/latest/userguide/example-walkthroughs-managing-access-example4.html) if you aren't the owner of the bucket)
- Create configuration and credentials files where either the default profile or a specific profile has access to the bucket

**WARNING:** In a specific use case, a user may wish to supply a dataset from one S3 bucket (Bucket A) to use in the benchmarking with another S3 bucket (Bucket B). For this case, the user does not wish to include Bucket A in the benchmarking but desires to move the data to Bucket B and use that storage location instead. The workflow will automate this process, but **only** if there is a *single* profile that has the correct access permissions for *both* Bucket A and B. That is, the underlying transfer mechanism, `aws s3 sync --recursive <BUCKET A> <BUCKET B>`, does not support using two different profiles' permissions for two different buckets.