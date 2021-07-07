from datetime import date, datetime, timedelta
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q
import json
import os
import sys
import psycopg2
import datetime


# Assign credentials and collector information
endpoint = os.environ['ES_ENDPOINT']
index = os.environ['ES_INDEX']
REDSHIFT_DATABASE = os.environ['REDSHIFT_DATABASE']
REDSHIFT_USER = os.environ['REDSHIFT_USER']
REDSHIFT_PASSWD = os.environ['REDSHIFT_PASSWD']
REDSHIFT_PORT = os.environ['REDSHIFT_PORT']
REDSHIFT_ENDPOINT = os.environ['REDSHIFT_ENDPOINT']

with open('./serviceBCOfficeList.json') as json_file:
    service_centers = json.load(json_file)


def lambda_handler(event, context):
    # access query string parameters: event['queryStringParameters']['param']
    # access path parameters: event['pathParameters']['param']
    
    
    office_ids = []
    # Retrieve any office id's from query string parameters
    if 'queryStringParameters' in event:
        if 'id' in event['queryStringParameters']:
            office_ids = event['queryStringParameters']['id'].split(",")
    else:
        for service_center in service_centers:
            office_ids.append(service_center['cfms_poc.office_id'])
    
    
    rs_query = "SELECT office_id, time_per AS time FROM servicebc.servetime;"
    rs_result = query_redshift(rs_query)
    es_result = query_elasticsearch_realtime(office_ids)

    times = {}
    for row in rs_result:
        times[row[0]] = row[1] # row[0] is the office_id and row[1] is the time calculation

    api_response_data = []
    for office in es_result:
        office_id = int(office['office_id'])
        current_line_length = max(0,office['current_line_length']) # ensure that we don't report fewer than 0 people in line
        estimated_wait = ''
        num_agents = int(office['num_agents'])
        if office_id in times.keys():
            estimated_wait = round( (times[office_id] / max(1,num_agents)) * current_line_length / 60) #  (time_per / #agents) * length
            #this is rounded to the nearest minute, for now, as the app doesn't display fractional minutes well
            
        api_response_data.append({"office_id": office_id, "current_line_length": current_line_length, "estimated_wait": estimated_wait })

    return {
        "statusCode": 200,
        "headers": {"content-type": "application/json"},
        "body": json.dumps({
            "api_name": "sbc-wt",
            "api_env": "test",
            "api_version": "0.1",
            "response_tstamp": datetime.datetime.now(),
            "results_count": len(office_ids),
            "data": api_response_data
        },default=str)
    }


# Executes a query on the Redhshift database 
# and returns the results
def query_redshift(query_string):
    
    try:
        conn = psycopg2.connect(
            dbname=REDSHIFT_DATABASE,
            user=REDSHIFT_USER,
            password=REDSHIFT_PASSWD,
            port=REDSHIFT_PORT,
            host=REDSHIFT_ENDPOINT
        )
    except Exception as ERROR:
        print(f"Connection Issue: {ERROR}")

    try:
        cursor = conn.cursor()
        cursor.execute(query_string)
        result = cursor.fetchall()
        cursor.close()
        conn.commit()
        conn.close()
    except Exception as ERROR:
        print(f"Execution Issue: {ERROR}")

    return result


# Queries ElasticSearch for the number of citizens in line
# for a given office, and returns the result
def query_elasticsearch_realtime(office_ids):
    anchordate = date.today().strftime("%Y-%m-%d")
    client = Elasticsearch(endpoint)

    results_list = []
    for id in office_ids:
        #for serviceCenter in serviceCenters:
        #    if serviceCenter["cfms_poc.office_name"] == office:
        #        office_id = serviceCenter['cfms_poc.office_id']
    
        # Query for number of addcitizen events.
        # Note: Uses IANA time zone code 'America/Vancouver'
        # to account for PDT and UTC offset.
        params = Q('term', app_id='TheQ') & \
            Q('term', event_name='addcitizen') & \
            Q('term', **{'contexts_ca_bc_gov_cfmspoc_office_1.office_id':
                         id}) & \
            Q('range', derived_tstamp={'gte': anchordate}) & \
            Q('range', derived_tstamp={'lt': "now"}) & \
            Q('range', derived_tstamp={'time_zone': "America/Vancouver"})
        try:
            add_citizen_search_result = Search(using=client, index=index).filter(params)
        except Exception as e:
            print(e)
    
        add_citizen_count = add_citizen_search_result.count()
    
        # Query for number of customerleft events.
        params = Q('term', app_id='TheQ') & \
            Q('term', event_name='customerleft') & \
            Q('term', **{'contexts_ca_bc_gov_cfmspoc_office_1.office_id':
                         id}) & \
            Q('range', derived_tstamp={'gte': anchordate}) & \
            Q('range', derived_tstamp={'lt': "now"}) & \
            Q('range', derived_tstamp={'time_zone': "America/Vancouver"})
        try:
            customer_left_search_result = Search(using=client, index=index).filter(params)
        except Exception as e:
            print(e)
        customer_left_count = customer_left_search_result.count()
    
        # Query for number of finish events.
        params = Q('term', app_id='TheQ') & \
            Q('term', event_name='finish') & \
            Q('term', **{'contexts_ca_bc_gov_cfmspoc_office_1.office_id':
                         id}) & \
            Q('range', derived_tstamp={'gte': anchordate}) & \
            Q('range', derived_tstamp={'lt': "now"}) & \
            Q('range', derived_tstamp={'time_zone': "America/Vancouver"})
        try:
            finish_events_search_result = Search(using=client, index=index).filter(params)
        except Exception as e:
            print(e)
        finish_events_count = finish_events_search_result.count()

        now_minus_1_hour = datetime.datetime.now() - timedelta(hours=1)
        
        # Query for all front-office events in the last hour
        params = Q('term', app_id='TheQ') & \
            Q('term', **{'contexts_ca_bc_gov_cfmspoc_office_1.office_id':
                         id}) & \
            ~Q('term', **{'unstruct_event_ca_bc_gov_cfmspoc_chooseservice_3.program_name':'back-office'}) & \
            ~Q('term', **{'unstruct_event_ca_bc_gov_cfmspoc_chooseservice_3.channel':'Back Office'}) & \
            Q('range', derived_tstamp={'gte': now_minus_1_hour}) & \
            Q('range', derived_tstamp={'lt': "now"}) & \
            Q('range', derived_tstamp={'time_zone': "America/Vancouver"})
        try:
            events_search_result = Search(using=client, index=index).filter(params)
        except Exception as e:
            print(e)
    
        events = events_search_result.execute()
        agent_list = []
        for event in events:
            # pull out any agent id that appears in the result set from the last hour
            agent_list.append(event['contexts_ca_bc_gov_cfmspoc_agent_3'][0]['agent_id'])
        num_agents = len(list(set(agent_list)))
        
        
         # Calculate the current number of citizens in this office line
        line_size = add_citizen_count - (customer_left_count + finish_events_count)

        office_result = {
            "office_id": id,
            "current_line_length": line_size,
            "num_agents": num_agents
        }

        results_list.append(office_result)
        
    return results_list
