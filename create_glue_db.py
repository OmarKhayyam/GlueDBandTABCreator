#!/usr/bin/env python

import argparse
import boto3

# When you try to create a database in Glue, it tries to create a table
# 'Default', in case this is not permitted for the user, in my case it wasn't
# it may be a better idea to create a table instead of merely a database.
# Here is the error I got: botocore.errorfactory.InvalidInputException: An error occurred (InvalidInputException) when calling the CreateDatabase operation: Create Table Default not supported for principal
# For my user, I have already granted permission to create a database in the Lakeformation console.
# Seems like Lake Formation manages users differently as compared to IAM.

def _create_glue_table(name,desc):
    client = boto3.client('glue')
    response = client.create_table(DatabaseName='salesdb',
                    TableInput={
                        'Name': name,
                        'Description': desc,
                        'StorageDescriptor': {
                            'Columns': [
                                {
                                    'Name': 'itemid',
                                    'Type': 'string'
                                },
                                {
                                    'Name': 'quantity',
                                    'Type': 'tinyint'
                                },
                                {
                                    'Name': 'userid',
                                    'Type': 'string'
                                },
                                {
                                    'Name': 'Meta_Geo',
                                    'Type': 'string'
                                },
                                {
                                    'Name': 'Meta_Loc',
                                    'Type': 'string'
                                },
                                {
                                    'Name': 'Meta_HOD',
                                    'Type': 'tinyint'
                                }
                            ],
                            'InputFormat': 'TextInputFormat',
                            'OutputFormat': 'IgnoreKeyTextOutputFormat',
                            'Compressed': True
                            'Location': 's3://rns-kdf-demo/streamed-data/' ## This is the S3 data location
                         }
                    }
               ) 

def create_glue_table(name,des):
    client = boto3.client('glue')
    ## Do not create the database if it already exists ##
    try:
        response = client.get_database(Name='salesdb')
    except client.exceptions.EntityNotFoundException:
        response = client.create_database(
            DatabaseInput={
                'Name': 'salesdb',
                'Description': 'Database for maintaining sales order data.'
            }
        )
    finally:
        _create_glue_table(name,des)
        print("Created table {}.".format(name))

def main():
    parser = argparse.ArgumentParser(description='Create an AWS Glue Catalog Table.')
    parser.add_argument('--tab_name',required=True,type=str,help="Provide a valid table name.")
    parser.add_argument('--tab_desc',required=True,type=str,help="Provide a description string in a few words.")

    tbname = vars(parser.parse_args())['tab_name']
    desc = vars(parser.parse_args())['tab_desc']
    
    create_glue_table(tbname,desc)

main()
