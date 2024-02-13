#!/bin/bash

aws iam create-role --region us-west-2 --role-name glue-role --assume-role-policy-document '{
   "Version": "2012-10-17",
   "Statement": [
       {
           "Effect": "Allow",
           "Principal": {
               "Service": "glue.amazonaws.com"
           },
           "Action": "sts:AssumeRole"
       }
   ]
}'

# No console
#     "Role": {
#         "Path": "/",
#         "RoleName": "glue-role",
#         "RoleId": "AROA3FLD4LJMSIBDL6AGE",
#         "Arn": "arn:aws:iam::767398009433:role/glue-role",
#         "CreateDate": "2024-02-12T02:30:09+00:00",
#         "AssumeRolePolicyDocument": {
#             "Version": "2012-10-17",
#             "Statement": [
#                 {
#                     "Effect": "Allow",
#                     "Principal": {
#                         "Service": "glue.amazonaws.com"
#                     },
#                     "Action": "sts:AssumeRole"
#                 }
#             ]
#         }
#     }
# }