import json
import requests
import sys
import datetime
import pdb
import csv
import psycopg2

def get_org_list(url):
    organizations_list = []
    org_list = requests.get(url + "/api/action/package_search?q=source_type:datajson&rows=1000")
    org_list = org_list.json()['result']['results']

    for organization in org_list:
        if organization['organization']['name'] not in organizations_list:
            organizations_list.append(organization['organization']['name'])
    organizations_list.sort()

    # with open('org_out.txt', 'w') as f:
    #    print >> f, 'Filename:', organizations_list

    return organizations_list

def get_dataset_list(datagov_url, org_name):
    '''
        Get the datasets on data.gov that we have for the organization
    '''
    dataset_keep = []
    org_harvest = []
    dataset_harvest_list = []
    totla_dup_data = []
    duplicates = []
    dup_log = []
    dup_json_log = []

    # get list of harvesters for the organization
    org_harvest_tmp = requests.get(
        datagov_url + "/api/3/action/package_search?q=organization:" + org_name +
        "&facet.field=[%22identifier%22]&facet.limit=-1&facet.mincount=2")
    org_harvest_tmp = org_harvest_tmp.json()['result']['search_facets']['identifier']['items']

    for harvest in org_harvest_tmp:
        org_harvest.append(harvest['name'])

    for identifier in org_harvest:
        dataset_list = requests.get(
            datagov_url + '/api/action/package_search?q=identifier:"' + identifier + '"' +
            '&fq=type:dataset&sort=metadata_modified+asc&rows=1000')
        harvest_data_count = dataset_list.json()['result']['count']
        start = 0
        while start <= harvest_data_count:
            dataset_list = requests.get(
                datagov_url + '/api/action/package_search?q=identifier:"' + identifier + '"&start=' + str(
                    start) + '&rows=1000')
            dataset_harvest_list += dataset_list.json()['result']['results']
            start += 1000

        if dataset_list.status_code == 200:
            try:
                dataset_count = dataset_list.json()['result']['count']
                data = dataset_list.json()['result']['results']

                if dataset_count > 1:
                    if data[dataset_count - 1]['id'] not in dataset_keep and \
                            data[dataset_count - 1]['organization']['name'] == org_name:
                        dataset_keep.append(data[dataset_count - 1]['id'])
                else:
                    dataset_keep.append(dataset['id'])

            except IndexError:
                continue

        for dataset_harvest in dataset_harvest_list:
            if dataset_harvest['id'] not in totla_dup_data and dataset_harvest['organization']['name'] == org_name:
                totla_dup_data.append(dataset_harvest['id'])

        duplicates = list(set(totla_dup_data) - set(dataset_keep))


    for dataset in duplicates:
        for dataset_h in dataset_harvest_list:
            if dataset_h['id'] == dataset:
                for extra in dataset_h['extras']:
                    # get the harvest_id
                    if extra['key'] == 'identifier':
                        identifier = extra['value']
                    if extra['key'] == 'source_hash':
                        source_hash = str(extra['value'])
                        dup_json_log.append(dataset_h)
                        dup_log.append(dataset_h['id']+ "','" + dataset_h['title'] + "','" +
                                       dataset_h['metadata_created'] + "','" + identifier  + "','" + source_hash +
                                       "','duplicate-removed'")

    with open('duplicates_datasets_json_' + org_name + '.txt', 'w') as f:
        print >> f, 'Filename:', dup_json_log

    with open('duplicates_datasets_' + org_name + '.csv', "w") as csv_f:
        writer = csv.writer(csv_f, lineterminator='\n')
        for dslist in dup_log:
            writer.writerow([dslist])

    return duplicates

def remove_duplicate_datasets(duplicate_datasets):
    # conn_string = "' dbname='' user='' password=''"
    # print "Connecting to database\n ->%s" % (conn_string)

    # conn = psycopg2.connect(conn_string)
    #  cursor = conn.cursor()
    with open('duplicate_datasets_out.txt', 'a') as f:
        for data in duplicate_datasets:
            print(data)
            #cursor.execute("update package set state='duplicate-removed' where name='" + data + "';")
            print >> f, "update package set state='duplicate-removed' where name='" + data + "';"
    #        conn.commit()

    # conn.close()

if __name__ == "__main__":
    '''
        This code for getting the list of organizations and duplicate duplicate data sets
    '''
    datagov_url = 'http://catalog.data.gov'
    #datagov_url = 'http://catalog-web-test.datagov.us'
    sysadmin_api_key = '<my_admin_api_key>'
    duplicate_datasets = []
    datagov_datasets = []
    # get organizations that have datajson harvester
    org_list = get_org_list(datagov_url)

    # get list of duplicate_datasets
    for organization in org_list:
        print(organization)
        dataset_dup_tmp = get_dataset_list(datagov_url, organization)
        duplicate_datasets += dataset_dup_tmp
        print(dataset_dup_tmp)
        #remove_duplicate_datasets(duplicate_datasets, sysadmin_api_key)
        remove_duplicate_datasets(dataset_dup_tmp)