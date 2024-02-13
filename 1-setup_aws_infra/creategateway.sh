#!/bin/bash

aws ec2 describe-vpcs --region us-west-2
# vpc: 0cadd61f2a5845f38
# rtb: 0ad325d2d11cd9557

aws ec2 create-vpc-endpoint --region us-west-2 --vpc-id vpc-0cadd61f2a5845f38 --service-name com.amazonaws.us-west-2.s3  --route-table-ids rtb-0ad325d2d11cd9557

# {
#     "VpcEndpoint": {
#         "VpcEndpointId": "vpce-0301757f719ef98aa",
#         "VpcEndpointType": "Gateway",
#         "VpcId": "vpc-0cadd61f2a5845f38",
#         "ServiceName": "com.amazonaws.us-west-2.s3",
#         "State": "available",
#         "PolicyDocument": "{\"Version\":\"2008-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":\"*\",\"Action\":\"*\",\"Resource\":\"*\"}]}",
#         "RouteTableIds": [
#             "rtb-0ad325d2d11cd9557"
#         ],
#         "SubnetIds": [],
#         "Groups": [],
#         "PrivateDnsEnabled": false,
#         "RequesterManaged": false,
#         "NetworkInterfaceIds": [],
#         "DnsEntries": [],
#         "CreationTimestamp": "2024-02-12T02:28:55+00:00",
#         "OwnerId": "767398009433"
#     }
# }