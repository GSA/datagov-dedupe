import json
import requests
import sys
import datetime
import pdb
import psycopg2

def get_org_list(url):
    organizations_list = []

    org_list = requests.get(url + "/api/action/package_search?q=source_type:datajson&rows=1000")
    org_list = org_list.json()['result']['results']

    for organization in org_list:
        if organization['organization']['name'] not in organizations_list:
            organizations_list.append(organization['organization']['name'])
        # print(organization['organization']['name'])
    with open('org_out.txt', 'w') as f:
        print >> f, 'Filename:', organizations_list

    return organizations_list

def get_dataset_list(datagov_url, org_name):
    '''
        Collect the Datasets from catalog.data.gov
    '''
    org_datasets = []
    dataset_keep = []
    duplicate_datasets = []
    org_harvest = []
    dataset_harvest_list = []

    # get list of harvesters for the organization
    org_harvest_tmp = requests.get(
        datagov_url + "/api/action/package_search?q=organization:" + org_name +
        "&fq=source_type:datajson&rows=100")
    org_harvest_tmp = org_harvest_tmp.json()['result']['results']
    # print(org_harvest_tmp)

    for harvest in org_harvest_tmp:
        org_harvest.append(harvest['id'])

    for harvest_id in org_harvest:
        dataset_list = requests.get(datagov_url + '/api/action/package_search?q=harvest_source_id:' + harvest_id)
        harvest_data_count = dataset_list.json()['result']['count']
        start = 0
        while start <= harvest_data_count:
            dataset_list = requests.get(
                datagov_url + '/api/action/package_search?q=harvest_source_id:' + harvest_id + '&start=' + str(
                    start) + '&rows=1000')
            dataset_harvest_list += dataset_list.json()['result']['results']
            start += 1000
           # print(dataset_harvest_list)
        for dataset_harvest in dataset_harvest_list:
            org_datasets.append(dataset_harvest['id'])
           # print(org_datasets)

        dataset_keep_tmp = harvest_datasets(dataset_harvest_list)
        dataset_keep += dataset_keep_tmp
        #print(dataset_keep)

        duplicate_datasets = list(set(org_datasets) - set(dataset_keep))
       # print(duplicate_datasets)

        remove_duplicate_datasets(duplicate_datasets)

        with open('organization_datasets_' + harvest_id + '.txt', 'w') as f:
            print >> f, 'Filename:', org_datasets
        with open('duplicates_datasets_' + harvest_id + '.txt', 'w') as f:
            print >> f, 'Filename:', duplicate_datasets
        with open('keep_datasets_' + harvest_id + '.txt', 'w') as f:
            print >> f, 'Filename:', dataset_keep
    return duplicate_datasets

def harvest_datasets(dataset_harvest_list):
    dataset_keep = []

    for dataset in dataset_harvest_list:
        try:
            for extra in dataset['extras']:
                # get the harvest_id
                oldest_id = dataset['id']

                if extra['key'] == 'identifier':
                    identifier = extra['value']
                    dataset_list = requests.get(
                        datagov_url + '/api/action/package_search?q=identifier:"' + identifier + '"' +
                        '&fq=type:dataset&sort=metadata_modified+asc&rows=100')
                    if dataset_list.status_code == 200:
                        try:
                            dataset_count = dataset_list.json()['result']['count']
                            data = dataset_list.json()['result']['results']
                            # print(dataset_count)

                            if dataset_count > 1:
                                if data[dataset_count - 1]['id'] not in dataset_keep:
                                    dataset_keep.append(data[dataset_count - 1]['id'])
                            else:
                                dataset_keep.append(dataset['id'])
                        except IndexError:
                            continue

                if extra['key'] == 'extras_rollup':
                    extras_rollup = extra['value']
                    extras_rollup = json.loads(str(extras_rollup))
                    identifier = extras_rollup['identifier']

                    dataset_list = requests.get(
                        datagov_url + '/api/action/package_search?q=identifier:"' + identifier + '"' +
                        '&fq=type:dataset&sort=metadata_modified+asc&rows=1000')
                    if dataset_list.status_code == 200:
                        dataset_count = dataset_list.json()['result']['count']
                        data = dataset_list.json()['result']['results']

                        if dataset_count > 1:
                            if data[dataset_count - 1]['id'] not in dataset_keep:
                                dataset_keep.append(data[dataset_count - 1]['id'])
                        else:
                            dataset_keep.append(dataset['id'])
        except KeyError:
            continue

    return dataset_keep

def remove_duplicate_datasets(duplicate_datasets):

    conn_string = "host='terraform-20180618152009351000000001.clsowbdquocd.us-east-1.rds.amazonaws.com' dbname='datagov_catalog_db' user='datagovrds' password='ckanckan'"
    # print "Connecting to database\n ->%s" % (conn_string)

    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    with open('duplicate_datasets_out.txt', 'a') as f:
        for data in duplicate_datasets:
           # print(data)
            cursor.execute("update package set state='deleted' where name='" + data + "';")
            print >> f, "update package set state='to_delete' where name='" + data + "';"


    #  cursor.execute("update package set state='deleted' where id='" + data + "';")
            conn.commit()

    conn.close()


if __name__ == "__main__":

    #datagov_url = 'http://catalog.data.gov'
    datagov_url = 'http://catalog-web-test.datagov.us'
    sysadmin_api_key = '<my_admin_api_key>'
    duplicate_datasets = []
    datagov_datasets = []
    # get organizations that have datajson harvester
    org_list = get_org_list(datagov_url)
    print(org_list)

    # get list of duplicate_datasets
    for organization in org_list:
        print(organization)
        #try:
            duplicate_datasets = get_dataset_list(datagov_url, organization)

            # remove duplicate datasets
            # remove_duplicate_datasets(duplicate_datasets, sysadmin_api_key)
            remove_duplicate_datasets(duplicate_datasets)
       # except KeyError:
        #    continue
