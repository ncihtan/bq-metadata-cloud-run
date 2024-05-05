#!/usr/bin/env python3

import json
import os
import re
import pandas as pd
import numpy as np
import requests
import glob
import synapseclient

from google.cloud import bigquery
from google.api_core.exceptions import Conflict


def load_bq(client, project, dataset, table, data, schema):
    '''
    Load table and schema to BigQuery
    '''

    print( 'Loading %s.%s.%s to BigQuery' 
        % (project, dataset, table))

    table_bq = '%s.%s.%s' % (project, dataset, table)
    job_config = bigquery.LoadJobConfig(
        schema=schema, 
        write_disposition="WRITE_TRUNCATE",
        autodetect=False,
        allow_jagged_rows=True,
        allow_quoted_newlines=True,
        source_format=bigquery.SourceFormat.CSV
    )

    job = client.load_table_from_dataframe(
        data, table_bq, job_config=job_config
    )



def get_description(attribute, schema, add_descriptions):
    
    try:
        dsc = schema[schema['Attribute'] == attribute]['Description'].values[0]
        description = (dsc[:1024]) if len(dsc) > 1024 else dsc
    
    except:
        try:
            dsc = add_descriptions[
                add_descriptions['Attribute'] == attribute
            ]['Description'].values[0]
            description = (dsc[:1024]) if len(dsc) > 1024 else dsc
        except:
            description = 'Description unavailable. Contact DCC for more information'
            print(
                '{} attribute not found in HTAN schema'.format(
                    attribute)
            )
    
    return description

# Function to send Slack notification
def send_slack_notification(message):
    webhook_url = os.environ.get('SLACK_WEBHOOK_URL')
    if webhook_url:
        data = {'text': message}
        response = requests.post(webhook_url, json=data)
        if response.status_code != 200:
            print('Failed to send Slack notification.')


def main():

    # map: HTAN center names to HTAN IDs 
    htan_centers = {
        "HTAN HTAPP": {"center_id": "hta1"},
        "PCAPP Pilot Project": {"center_id": "hta2"},
        "HTAN BU": {"center_id": "hta3"},
        "HTAN CHOP": {"center_id": "hta4"},
        "HTAN DFCI": {"center_id": "hta5"},
        "HTAN Duke": {"center_id": "hta6"},
        "HTAN HMS": {"center_id": "hta7"},
        "HTAN MSK": {"center_id": "hta8"},
        "HTAN OHSU": {"center_id": "hta9"},
        "HTAN SRRS": {"center_id": "hta15"},
        "HTAN Stanford": {"center_id": "hta10"},
        "HTAN Vanderbilt": {"center_id": "hta11"},
        "HTAN WUSTL": {"center_id": "hta12"},
        "HTAN TNP SARDANA": {"center_id": "hta13"},
        "HTAN TNP - TMA": {"center_id": "hta14"},
        "HTAN SRRS": {"center_id": "hta15"},
    }
    
    SYN_PAT = os.environ.get('SYNAPSE_AUTH_TOKEN')

    # instantiate synapse client
    syn = synapseclient.Synapse()
    syn.login(authToken=SYN_PAT)

    # instantiate google bigquery client
    client = bigquery.Client()

    # read in 'additional' attribute descriptions
    SHEET_ID = '1RpwQqY7xi-arWJMOMpF0EOhbXPCcQudv8RZ_fp0o_es'
    SHEET_NAME = 'Sheet1'
    url = f'https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet={SHEET_NAME}'
    
    add_descriptions = pd.read_csv(url)

   	# Query HTAN Fileview excluding test center projects
    all_files = syn.tableQuery(
        "SELECT * FROM syn20446927 WHERE type = 'file' AND projectId NOT IN \
        ('syn21989705','syn20977135','syn20687304','syn32596076','syn52929270')"
    ).asDataFrame()

    # get all manifests grouped by project (i.e. research center/atlas)
    metadata_manifests = all_files[all_files["name"].str.contains(
        "synapse_storage_manifest")][
        ["id", "projectId", "parentId", "modifiedOn"]
        ].groupby(["parentId"]).max()
    
    metadata_manifests = metadata_manifests.groupby(["projectId"])

    data_model = client.query("""
        SELECT * FROM `htan-dcc.metadata.data-model`
    """).result().to_dataframe()

    data_model['label'] = [x.replace(' ','').lower() for x in list(data_model['Attribute'])]

    released_entities = client.query("""
        SELECT * FROM `htan-dcc.released.entities`
    """).result().to_dataframe()

    combined_tables = {}

    for project_id, dataset_group in metadata_manifests:

        center = syn.get(project_id[0], downloadFile = False).name
        if not center in htan_centers:
            #do not include project outside official HTAN centers (e.g. test ones)
            continue

        center_id = htan_centers[center]['center_id']
        print("ATLAS: " + center)

        datasets = dataset_group.to_dict("records")

        for dataset in datasets:

            manifest_location = "./tmp/" + center_id + "/" + dataset["id"] + "/"
            manifest_path = manifest_location + "synapse_storage_manifest.csv"

            manifest = syn.get(
                dataset["id"], 
                downloadLocation=manifest_location, 
                ifcollision='overwrite.local'
            )
            
            os.rename(glob.glob(manifest_location + "*.csv")[0], manifest_path)

            manifest_data = pd.read_csv(manifest_path)
            manifest_id = manifest.id
            manifest_version = manifest.versionNumber

            # data type/schema component
            try:
                component = manifest_data['Component'][0]
            except IndexError:
                print("Manifest data unexpected: " + 
                    manifest_path + " " + str(manifest_data)
                )
                continue

            # skip components that are NA
            if pd.isna(component):
                print("Component is N/A: " + 
                    manifest_path + " " + str(manifest_data)
                )
                continue
            
            print('Template: '+center +' '+component)

            # ------------------------------------------------------------

            # reindex table to remove non-standard or user-defined columns
            attr = ['entityId','Uuid','Id']

            try:
                attr = attr + [x.strip(' ') for x in list(
                    data_model[data_model['label'] == 
                    component.lower()]['DependsOn'])[0].split(',')
                    ]
            except:
                print(component + ' not found in data model')
                continue

            for a in attr:
                try:
                    vv = [x.strip(' ') for x in list(
                        data_model[data_model['label'] == 
                        a.replace(' ','').lower()]['Valid_Values'])[0].split(',')
                    ]
                    
                    for v in vv:                        
                        try:
                            attr = attr + [x.strip(' ') for x in list(
                                data_model[data_model['label'] == 
                                v.replace(' ','').lower()]['DependsOn'])[0].split(',')
                            ]
                        except:
                            continue
                except:
                    continue


            if component in ['BulkRNA-seqLevel2','BulkWESLevel2']:
                attr.append('HTAN Parent Biospecimen ID')

            if component == 'ImagingLevel2':
                attr.append('HTAN Parent Data File ID')

            attr = list(set(attr))

            manifest_data = (manifest_data.reindex(columns=attr)).astype("string")

            # Add center name, manifest ID, and manifest version columns to table
            manifest_data['HTAN_Center'] = center
            manifest_data['Manifest_Id'] = manifest_id
            manifest_data['Manifest_Version'] = manifest_version

            # merge tables by component
            if component in combined_tables:
                combined_tables[component]['data'] = pd.concat(
                    [combined_tables[component]['data'],manifest_data]
                ).reset_index(drop=True)
            else:
                combined_tables[component] = {"data": manifest_data}


    for key,value in combined_tables.items():
        print(key)
        bq_table = value['data']

        bq_table['Id'] = bq_table['Id'].fillna(bq_table['Uuid'])
        bq_table.drop(columns=['Uuid'],inplace=True)

        # add cloud url, file size and md5 
        # columns to non-biospecimen/clinical tables
        if any(a in key for a in [
            'Level','Auxiliary','Other','ExSeq','CosMx','Xenium']):

            bq_table = bq_table.merge(
                all_files[['id',
                    'dataFileSizeBytes',
                    'dataFileMD5Hex',
                    'dataFileConcreteType',
                    'dataFileBucket',
                    'dataFileKey']], 
                how='left', left_on='entityId', right_on='id'
            )

            bq_table = bq_table.rename(columns={
                'dataFileSizeBytes':'File_Size', 
                'dataFileMD5Hex':'md5'}
            )

            cloud_url = []
            for i,r in bq_table.iterrows():
                try:
                    cloud_url.append(('s3://' if 'S3' in 
                        r['dataFileConcreteType'] else 
                        'gs://') + r['dataFileBucket'] + '/' + r['dataFileKey'])
                except:
                    cloud_url.append(None)

            bq_table['Cloud_Storage_Path'] = cloud_url

            bq_table = bq_table.drop(columns=['dataFileConcreteType',
                'dataFileBucket','dataFileKey','id'])

            # Add data release and cds release indicator columns
            bq_table = bq_table.merge(
                released_entities[['entityId','Data_Release','CDS_Release']],
                how='left', on='entityId'
            )

        # create bigquery table schema: 
        # without column type validation, sometimes values that should be 
        # ints or floats are not; as it only takes one bad value to cause 
        # the entire BQ upload to fail, we set all columns as 'string'

        bq_schema = []
        default_type='STRING'

        for column_name, dtype in bq_table.dtypes.items():
            bq_schema.append(
                {
                    'name': re.sub('[^0-9a-zA-Z]+', '_', column_name),
                    'type': default_type if column_name not in 
                        ['File_Size','Manifest_Version'] else 'integer',
                    'description': get_description(column_name, data_model, add_descriptions)
                }
            )

        # make column names bigquery-friendly
        bq_table.columns = bq_table.columns.str.replace(
            '[^0-9a-zA-Z]+','_', regex=True
        )

        # load table to BigQuery
        load_bq(client, 
            'htan-dcc', 'combined_assays', 
            key, bq_table.drop_duplicates(), bq_schema
        )

    print('Sending Slack notification')

    try:
        # Calculate summary stats
        total_files_processed = sum(len(value['data']) for value in combined_tables.values())
        total_projects_processed = len(combined_tables)
        total_entities_released = len(released_entities)
        data_release_percentage = (total_entities_released / total_files_processed) * 100 if total_files_processed > 0 else 0
        cds_release_percentage = (released_entities['CDS_Release'].sum() / total_files_processed) * 100 if total_files_processed > 0 else 0
    
        # Prepare Slack notification message
        slack_message = f'''
        Daily Summary Stats:
        - Total Files Processed: {total_files_processed}
        - Total Projects Processed: {total_projects_processed}
        - Total Entities Released: {total_entities_released}
        - Data Release Percentage: {data_release_percentage:.2f}%
        - CDS Release Percentage: {cds_release_percentage:.2f}%
        '''
    
        # Send Slack notification
        send_slack_notification(slack_message)
    except:
        print('Failed to send Slack notification!'

    print( '' )
    print( ' Done! ' )
    print( '' )


if __name__ == '__main__':

    main()
