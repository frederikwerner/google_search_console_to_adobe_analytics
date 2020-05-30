import datetime
import requests
import sys
import jwt
import re
import httplib2

from apiclient.discovery import build
from apiclient.errors import HttpError
from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client.tools import argparser, run_flow

config = {
"apiKey":"3ec159485be87ed8fk6f9g37j79d67153b31e6",
"technicalAccountId":"6JCD048F50A6495F35C8D9D4D2@techacct.adobe.com",
"orgId":"25DB24210614E744C980A8A7@AdobeOrg",
"secret":"d033109-fd7a71ba2-489-9cf455-f2f87f4298ab",
"google_property":"https://www.website.com/",
"data_source_name": "Google Search Console Import",
"report_suite_id": 'orgreportsuiteid',
"job_prefix": "GSC-Import",
"lookback_days": 100,
"type_evar":"197",
"url_evar":"198",
"keyword_evar":"199",
"ctr_event":"997",
"clicks_event":"998",
"impressions_event":"999",
"position_event":"1000",
"key":b'-----BEGIN PRIVATE KEY-----\nMIIEvAIBADAN7wGu1P3aNA3yjqGA==\n-----END PRIVATE KEY-----',

"metascopes":"ent_analytics_bulk_ingest_sdk",
"imsHost":"ims-na1.adobelogin.com",
"imsExchange":"https://ims-na1.adobelogin.com/ims/exchange/jwt",
"discoveryUrl":"https://analytics.adobe.io/discovery/me"
}

base = datetime.datetime.today()
date_list = [(base - datetime.timedelta(days=x)).strftime("%Y-%m-%d") for x in range(config["lookback_days"])]

if config["clicks_event"] or config["impressions_event"] or config["position_event"] or config["ctr_event"]:
    if config["url_evar"] and config["keyword_evar"]:
        print("Both URL and Keyword eVar given. Importing Keywords per URL...")
        operating_mode = "URL and Keyword"
        query_dimensions = ['date','page','query']
        datasource_columns = ['Date', 'Evar '+config["type_evar"], 'Evar '+config["url_evar"], 'Evar '+config["keyword_evar"]]
    elif config["keyword_evar"]:
        print("Only Keyword eVar given. Importing only Keywords...")
        operating_mode = "Keyword Only"
        query_dimensions = ['date','query']
        datasource_columns = ['Date', 'Evar '+config["type_evar"], 'Evar '+config["keyword_evar"]]
    elif config["url_evar"]:
        print("Only URL eVar given. Importing only URLs...")
        operating_mode = "URL Only"
        query_dimensions = ['date','page']
        datasource_columns = ['Date', 'Evar '+config["type_evar"], 'Evar '+config["url_evar"]]
    else:
        print("No eVars given. Importing metrics only")
        operating_mode = "Metrics Only"
        query_dimensions = ['date']
        datasource_columns = ['Date', 'Evar '+config["type_evar"]]
    if config["clicks_event"]:
        datasource_columns.append("Event "+config["clicks_event"])
    if config["impressions_event"]:
        datasource_columns.append("Event "+config["impressions_event"])
    if config["position_event"]:
        datasource_columns.append("Event "+config["position_event"])
    if config["ctr_event"]:
        datasource_columns.append("Event "+config["ctr_event"])
else:
    print("No events given. Aborting...")
    sys.exit()

if not config["type_evar"]:
    print("No Type Evar given. Aborting...")
    sys.exit()

def get_authenticated_google_service():
    flow = flow_from_clientsecrets("client_secrets.json", scope="https://www.googleapis.com/auth/webmasters.readonly",
    message="MISSING_CLIENT_SECRETS_MESSAGE")
    storage = Storage("oauth2.json")
    credentials = storage.get()
    if credentials is None or credentials.invalid:
        credentials = run_flow(flow, storage)
    return build("webmasters", "v3", http=credentials.authorize(httplib2.Http()))

search_console = get_authenticated_google_service()

def get_jwt_token(config):
    return jwt.encode({
        "exp": datetime.datetime.utcnow() + datetime.timedelta(seconds=30),
        "iss": config["orgId"],
        "sub": config["technicalAccountId"],
        "https://{}/s/{}".format(config["imsHost"], config["metascopes"]): True,
        "aud": "https://{}/c/{}".format(config["imsHost"], config["apiKey"])
    }, config["key"], algorithm='RS256')

def get_access_token(config, jwt_token):
    post_body = {
        "client_id": config["apiKey"],
        "client_secret": config["secret"],
        "jwt_token": jwt_token
    }

    response = requests.post(config["imsExchange"], data=post_body)
    return response.json()["access_token"]

def get_first_global_company_id(config, access_token):
    response = requests.get(
        config["discoveryUrl"],
        headers={
            "Authorization": "Bearer {}".format(access_token),
            "x-api-key": config["apiKey"]
        }
    )
    return response.json().get("imsOrgs")[0].get("companies")[0].get("globalCompanyId")

jwt_token = get_jwt_token(config)
access_token = get_access_token(config, jwt_token)
global_company_id = get_first_global_company_id(config, access_token)

dataSources = requests.post(
        "https://api.omniture.com/admin/1.4/rest/?method=DataSources.Get",
        headers={
            "Authorization": "Bearer {}".format(access_token),
            "x-api-key": config["apiKey"],
            "x-proxy-global-company-id": global_company_id
        }, 
        data={'reportSuiteID':config["report_suite_id"]}
    ).json()

for dataSource in dataSources:
    if dataSource["name"] == config["data_source_name"]:
        dataSourceID = dataSource["id"]
        print("Found Data Source ID")
        break

if dataSourceID:
    jobs = requests.post(
        "https://api.omniture.com/admin/1.4/rest/?method=DataSources.GetJobs",
        headers={
            "Authorization": "Bearer {}".format(access_token),
            "x-api-key": config["apiKey"],
            "x-proxy-global-company-id": global_company_id
        }, 
        data={'reportSuiteID':config["report_suite_id"],'dataSourceID':dataSourceID}
    ).json()
    for job in jobs:
        jobname = job["fileName"]
        if config["job_prefix"].lower() in jobname:
            matchstring = '^'+re.escape(config["job_prefix"].lower())+"_"+operating_mode.lower()+'_([0-9]{4}-[0-9]{2}-[0-9]{2})_'+config["report_suite_id"]+'_'+dataSourceID+'_[0-9]*\.tab$'
            p = re.compile(matchstring)
            regex_match = p.match(job["fileName"])
            if regex_match and job["status"] != "failed":
                jobdate = regex_match.group(1)
                if jobdate in date_list:
                    date_list.remove(jobdate)
else:
    print("Data Source not found. Please check your configured Data Source name.")
    sys.exit()

print("Number of days to fetch: "+str(len(date_list)))
i = 1
for query_date in date_list:
    print("Fetching Google Search Console Data for "+query_date+". Query "+str(i)+"/"+str(len(date_list)))
    request = {
        'startDate': query_date,
        'endDate': query_date,
        'dimensions': query_dimensions,
        'rowLimit': 10000
    }
    result_rows = []
    result = search_console.searchanalytics().query(siteUrl=config["google_property"], body=request).execute()
    if "rows" in result:
        print("Received "+str(len(result["rows"]))+" rows of data. Uploading to Adobe...")
        for row in result["rows"]:
            row_to_append = []
            row_to_append.append(row["keys"][0][5:7]+"/"+row["keys"][0][8:10]+"/"+row["keys"][0][0:4]+"/00/00/00")
            row_to_append.append("Import Type: "+operating_mode)

            if operating_mode != "Metrics Only":
                row_to_append.append(row["keys"][1])
            if operating_mode == "URL and Keyword":
                row_to_append.append(row["keys"][2])

            if config["clicks_event"]:
                row_to_append.append(str(row["clicks"]))
            if config["impressions_event"]:
                row_to_append.append(str(row["impressions"]))
            if config["position_event"]:
                row_to_append.append(str(row["position"]))
            if config["ctr_event"]:
                row_to_append.append(str(row["ctr"]))
            result_rows.append(row_to_append)

        if len(result_rows)  > 0:
            jobresponse = requests.post(
                "https://api.omniture.com/admin/1.4/rest/?method=DataSources.UploadData",
                headers={
                    "Authorization": "Bearer {}".format(access_token),
                    "x-api-key": config["apiKey"],
                    "x-proxy-global-company-id": global_company_id
                }, 
                json={
                    "columns": datasource_columns,
                    'reportSuiteID': config["report_suite_id"],
                    'dataSourceID':dataSourceID,
                    "finished": True,
                    "jobName": config["job_prefix"]+"_"+operating_mode+"_"+query_date,
                    "rows": result_rows
                }
            )
    else:
        print("No Data for "+query_date)
    i+=1
