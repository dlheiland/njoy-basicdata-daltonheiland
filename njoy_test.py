import json
import pandas as panda
from cassandra.cluster import Cluster
from datetime import datetime
import logging
import zipfile


def blast_data_to_cassandra(blast_dataframe,session):
    query = "INSERT INTO campaigns.blast_campaigns(name,day,id) VALUES (?,?,?)"
    prepared = session.prepare(query)

    for row in blast_dataframe.itertuples(index=True, name='blast_dataframe'):
        session.execute(prepared, (row.name,row.day,row.id))

def message_blast_data_to_cassandra(message_blast_dataframe,session):
    query = "INSERT INTO campaigns.message_blast(profile_id,clicks,delivery_status,device,message_id,message_revenue,opens,blast_id,send_time) VALUES (?,?,?,?,?,?,?,?,?)"
    prepared = session.prepare(query)

    for row in message_blast_dataframe.itertuples(index=True, name='message_blast_dataframe'):
        opens_list = opens_field_parse(row)
        clicks_dictionary = clicks_field_parse(row)
        session.execute(prepared, (row.profile_id,clicks_dictionary,row.delivery_status,row.device,row.message_id,row.message_revenue,opens_list,row.blast_id,datetime.strptime(str(row.send_time).replace("T", " "), '%Y-%m-%d %H:%M:%S.%f')))

def message_transactional_data_to_cassandra(message_transactional_dataframe,session):
    query = "INSERT INTO campaigns.message_transactional(id,clicks,delivery_status,device,message_revenue,opens,profile_id,send_time,template) VALUES (?,?,?,?,?,?,?,?,?)"
    prepared = session.prepare(query)

    for row in message_transactional_dataframe.itertuples(index=True, name='message_transactional_dataframe'):
        opens_list = opens_field_parse(row)
        clicks_dictionary = clicks_field_parse(row)
        session.execute(prepared, (row.id,clicks_dictionary,row.delivery_status,row.device,row.message_revenue,opens_list,row.profile_id,datetime.strptime(str(row.send_time).replace("T", " "), '%Y-%m-%d %H:%M:%S.%f'),row.template))

def profiles_data_to_cassandra(profiles_dataframe,session):
    query = "INSERT INTO campaigns.profiles(id,browser ,create_date,email,count,geo_city ,geo_country ,geo_state ,geo_zip ,last_click ,last_open ,last_pageview ,lists_signup ,optout_reason,optout_time,signup_time,total_clicks,total_messages,total_opens,total_pageviews,total_unique_clicks,total_unique_opens,variables) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    prepared = session.prepare(query)
    for row in profiles_dataframe.itertuples(index=True, name='profiles_dataframe'):
        session.execute(prepared, (row.id, \
                                   row.browser,\
                                   datetime.fromisoformat(str(row.create_date).replace("T", " ")),\
                                   row.email, \
                                   dictionary_extract_cassandra_sub(row.geo,'count'),\
                                   dictionary_extract_cassandra_sub(row.geo,'city') ,\
                                   dictionary_extract_cassandra_sub(row.geo,'country') ,\
                                   dictionary_extract_cassandra_sub(row.geo,'state') ,\
                                   dictionary_extract_cassandra_sub(row.geo,'zip') ,\
                                   datetime.fromisoformat(str(row.last_click).replace("T", " ")),\
                                   datetime.fromisoformat(str(row.last_open).replace("T", " ")),\
                                   datetime.fromisoformat(str(row.last_pageview).replace("T", " ")),\
                                   dictionary_date_time_cassandra_cleanup(row.lists_signup) ,\
                                   str(row.optout_reason), \
                                   date_time_cleanup(row.optout_time),\
                                   date_time_cleanup(row.signup_time),\
                                   row.total_clicks,\
                                   row.total_messages,\
                                   row.total_opens, \
                                   row.total_pageviews, \
                                   row.total_unique_clicks, \
                                   row.total_unique_opens,\
                                   dictionary_stringify_cassandra(row.vars)))

#parses opens fields to useable list with proper datatypes for cassandra
def opens_field_parse(row):
    logging.debug('Passed variables: %s', row)
    opens_list = []
    if (str(row.opens) != 'nan'):
        for item in row.opens:
            json_item = json.loads(str(item).replace("\'", "\""))
            opens_list.append(json_item['ts']['$date'])
    logging.debug('opens_list: %s', opens_list)
    return opens_list

#parses clicks fields to useable dictionary with proper datatypes for cassandra
def clicks_field_parse(row):
    logging.debug('Passed variables: %s', row)
    clicks_dictionary = {}
    if (str(row.clicks) != 'nan'):
        for item in row.clicks:
            json_item = json.loads(str(item).replace("\'", "\""))
            clicks_dictionary[json_item['ts']['$date']] = json_item['url']
    logging.debug('clicks_dictionary: %s', clicks_dictionary)
    return clicks_dictionary

#parses simple dates with proper datatypes for cassandra
def date_time_cleanup(row):
    logging.debug('Passed variables: %s', row)
    if (str(row) != 'nan'):
        logging.debug('Returning formated date time')
        return datetime.strptime(str(row).replace("T", " "), '%Y-%m-%d %H:%M:%S.%f')

#parses dates list with proper datatypes for cassandra
def dictionary_date_time_cassandra_cleanup(row):
    logging.debug('Passed variables: %s', row)
    extract_dictionary = {}
    if (str(row) != 'nan'):
        for item in row:
            json_item = json.loads(str(row).replace("\'", "\""))
            logging.info('Attempting to parse datetime in two possible formats.')
            try:
                extract_dictionary[item] = datetime.strptime(str(json_item.get(item)).replace("T", " "), '%Y-%m-%d %H:%M:%S.%f')
                logging.debug('Parsed with nanoseconds')
            except:
                extract_dictionary[item] = datetime.fromisoformat(str(json_item.get(item)).replace("T", " "))
                logging.debug('Parsed in iso format')
    logging.debug('extract_dictionary: %s', extract_dictionary)
    return extract_dictionary

#changes the profiles vars column to all string to allow it to be stored without datatype issues
def dictionary_stringify_cassandra(row):
    logging.debug('Passed variables: %s', row)
    extract_dictionary = {}
    if (str(row) != 'nan'):
        for item in row:
            json_item = json.loads(str(row).replace("\'", "\"").replace("None", "\"\"").replace("True", "\"True\"").replace("False", "\"False\""))
            extract_dictionary[item] = str(json_item.get(item))
    logging.debug('extract_dictionary: %s', extract_dictionary)
    return extract_dictionary

#parses a dictionary with proper datatypes for cassandra
def dictionary_extract_cassandra_sub(row,sub_search_string):

    logging.debug('Passed variables: %s,%s', row,sub_search_string)
    extract_dictionary = {}
    if (str(row) != 'nan' and sub_search_string in str(row)):
        json_item = json.loads(str(row).replace("\'", "\""))
        extract_dictionary = json_item[sub_search_string]

    logging.debug('extract_dictionary: %s', extract_dictionary)
    return extract_dictionary

#loads the unzipped files and loads them into a panda's dataframe
def load_json_file_to_list(file_location):

    logging.info('Processing file into pandas: %s',file_location)

    json_list=[]

    with open(file_location, "r") as json_file:
        for line in json_file:
            try:
                json_list.append(json.loads(line))
                logging.debug('Processing line successful: %s', line)
            except:
                logging.warn('Json line invalid: %s',line)

    json_representation_dataframe = panda.json_normalize(json_list,max_level=0)

    logging.debug('Returning Json Panda Dataframe: %s', json_representation_dataframe)
    return json_representation_dataframe

#loads the unzipped files and loads them into a panda's dataframe
def unzip_file(file_location,target_location):

    logging.info('Unzipping File: %s',file_location)

    with zipfile.ZipFile(file_location, "r") as zip_ref:
        zip_ref.extractall(target_location)

    logging.debug('Returning unzipped location: %s', target_location)
    return True

# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    #set logging level for the program
    logging.basicConfig(level=logging.DEBUG)

    cluster = Cluster(['localhost'], port=9042)
    session = cluster.connect('campaigns', wait_for_all_pools=True)
    logging.debug('Connection to Cassandra is complete.')

    file_location='raw-data-dump/sailthru_data-exporter_samples.zip'
    target_location='unzip-data-dump/'
    date_format='20160401'

    if (unzip_file(file_location,target_location)):
        blast_data_to_cassandra(load_json_file_to_list(target_location+'blast.'+date_format+'.json'),session)
        message_blast_data_to_cassandra(load_json_file_to_list(target_location+'message_blast.'+date_format+'.json'),session)
        message_transactional_data_to_cassandra(load_json_file_to_list(target_location+'message_transactional.'+date_format+'.json'),session)
        profiles_data_to_cassandra(load_json_file_to_list(target_location+'profile.'+date_format+'.json'),session)

