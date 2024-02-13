# STEDI Human Balance Analytics

**This document describes the ELT process and data lake built for the STEDI Human Balance Analytics project, utilizing AWS S3, Athena, and Glue Studio.**

## Table of Contents

- [Considerations](#considerations)
- [Prerequisites](#prerequisites)
- [Thought Process](#thought-process)
- [Alternatives](#Alternatives)

## Considerations

The codes in this repository are parts of the whole ecossystem builton AWS and also used on AWS cli. 
To create a datalake you can follow the items inside the numbered folders, and executing the programs according to possible dependenciees that they have (for example accelerometer_trusted needs customer_trusted)

## Prerequisites

Access to AWS, including S3, CloudShell, Glue Studio, and Athena.
Optional: AWS CLI installed and configured on your local machine.

## Thought process

1. Data ingestion
    I've used the AWS cli on my local machine to upload the data into an s3 path. The data was in the github repo provided in the instructions. Alternatively, a Glue job can be used with a s3 path to update the _landing tables. Since the path provided didn't have read permissions, this alternative wasn't explored.
    
2. Lannding table definitions
    The landing tables were defined according to data in the json files. Athena was used to define a table considering a json input and column with strings, bigints and float data types.

3. Trusted table creations
    The trusted tables were created with Glue Studio, using s3 and Data Catalog sources and SQL transformations. First, there was a treatment in customer_landing to exclude users that didn't opt to share reserach data. Then, customer_trusted is used in joins with accelerometer_landing and step_trainer_landing to create, respectively, accelerometer_trusted and step_trainer_trusted. The Glue job also created the new paths and table definitions, with the setting of updating this defintion if the input data changes.

4. Curated table creations
    With the trusted layer ready, customer_curated was created to consider only users with accelerometer data, and also masking PII data. All of those steps happened in Glue Studio. With step_trainer_trusted and accelerometer_trusted, machine_learning_curated was created to provide all of the data needed by the team that request the data. There was also a treatment for PII information.

## Alternatives

During the process there were also included some alternative tables, with possible corrections to the results that were expected in the rubric of the project. The oringinal data was left as it is in compliance with the expected output. 
The first potential issued addressed is in accelerometer_trusted, as in the instructions there was a mention of the accelerometer data being valid only if its timestamp was logged after the user consented to share research data. This was ambiguous because we don't know the terms of the agreement, if the customer agreed only to share data from this moment, or all of its data collected for the product.
Another opportunity was in the machine_learning_curated table, as the original join only consindered timestamps, there could be two or more different users that had the same timestamp. An alternative join uses customer, accelerometer, and step_trainer, bringing the user email and serial number to the join with step_trainer, this showed that there were some duplicated data on the requested output.