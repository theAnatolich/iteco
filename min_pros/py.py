import getopt
import json
import sys
import time

import psycopg2
import requests

connection = psycopg2.connect("host=localhost dbname=pdidb user=pdi password=PDIpass")



#%%
def get_headers():
    authorization_url = "http://172.16.250.122/idsrv/connect/token"
    authorization_data = "grant_type=password&scope=openid profile email roles viqubeadmin_api viqube_api&response_type=id_token token&username=admin&password=123456"
    authorization_headers = {
        'content-type': "application/x-www-form-urlencoded",
        'authorization': "Basic dmlxdWJlYWRtaW5fcm9fY2xpZW50OjcmZEo1UldwVVMkLUVVQE1reHU="
        }
    authorization_response = requests.request("POST", authorization_url, data=authorization_data, headers=authorization_headers)
    r_headers = {
        'X-API-VERSION':'1.0',# x_api_versions[api_serv],
        'Content-Type': 'application/json',
        'Authorization': ""+authorization_response.json()['token_type']+" "+authorization_response.json()['access_token']
    }
    return r_headers

def start_loadplans():
    
    l_url = "http://"+parametrs['domain']+"/vqadmin/api/databases/"+parametrs['databaseID']+"/loadplans/"+parametrs['loadPlanID']+"/start"
    l_payload = ""
    l_headers = get_headers(1)
    l_response = requests.request("POST", l_url, data=l_payload, headers=l_headers)
    
def stop_loadplans():
    
    l_url = "http://"+parametrs['domain']+"/vqadmin/api/databases/"+parametrs['databaseID']+"/loadplans/"+parametrs['loadPlanID']+"/stop"
    l_payload = ""
    l_headers = get_headers(1)
    l_response = requests.request("POST", l_url, data=l_payload, headers=l_headers)

def get_loaders():
    l_url = "http://"+parametrs['domain']+"/viqube/loaders"
    l_payload = ""
    l_headers = get_headers(2)
    l_response = requests.request("POST", l_url, data=l_payrload, headers=l_headers)
    #myChart.values=[[l_response]]
    while True:
        l_response = requests.request("GET", l_url, data="", headers=l_headers)
        
        if l_response.json()['status']=='Running':
            #print(json.dumps(response.json(), sort_keys=True, indent=4))
            time.sleep(1)
            continue
        else:
            
            break
			
    var=l_response.json()['error']
    if not l_response.json()['error']:
            
        
        myChart.values=[["good"]]
    else:
	    
        myChart.values=[[json.dumps(l_response.json(), sort_keys=True, indent=4)]]

def load_to_postgres():
    	
    l_url = "http://control.edu.gov.ru/api/get.php?auth=cvyhiux6dlndy8&status=in_progress"
    l_response = requests.request("GET", l_url, data="", headers="")
    #print(json.dumps(l_response.json(), sort_keys=True, indent=4))
    cursor = connection.cursor()
    data = []

    for line in l_response.json():
        data.append(json.loads(json.dumps(line)))

    fields = [
            'ID',
            'TITLE',
            'DEADLINE',
            'CLOSED_DATE',
            'RESPONSIBLE_ID',
            'RESPONSIBLE_LAST_NAME',
            'RESPONSIBLE_NAME',
            'RESPONSIBLE_SECOND_NAME',
            'FULL_NAME'
    ]

    cursor.execute("truncate table edu_gov_in_progress")
    for item in data:
        my_data = [item[field] for field in fields]
        insert_query = "INSERT INTO edu_gov_in_progress (ID,TITLE,DEADLINE,CLOSED_DATE,RESPONSIBLE_ID,RESPONSIBLE_LAST_NAME,RESPONSIBLE_NAME,RESPONSIBLE_SECOND_NAME,FULL_NAME,status) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'В работе')"
        cursor.execute(insert_query, tuple(my_data))

    cursor.execute("Commit")

    l_url = "http://control.edu.gov.ru/api/get.php?auth=cvyhiux6dlndy8&status=done"
    l_response = requests.request("GET", l_url, data="", headers="")
    #print(json.dumps(l_response.json(), sort_keys=True, indent=4))
    cursor = connection.cursor()
    data = []

    for line in l_response.json():
        data.append(json.loads(json.dumps(line)))

    cursor.execute("truncate table edu_gov_done")
    for item in data:
        my_data = [item[field] for field in fields]
        insert_query = "INSERT INTO edu_gov_done (ID,TITLE,DEADLINE,CLOSED_DATE,RESPONSIBLE_ID,RESPONSIBLE_LAST_NAME,RESPONSIBLE_NAME,RESPONSIBLE_SECOND_NAME,FULL_NAME,status) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'Исполнено')"
        cursor.execute(insert_query, tuple(my_data))

    cursor.execute("Commit")

    l_url = "http://control.edu.gov.ru/api/get.php?auth=cvyhiux6dlndy8&status=expired"
    l_response = requests.request("GET", l_url, data="", headers="")
    #print(json.dumps(l_response.json(), sort_keys=True, indent=4))
    cursor = connection.cursor()
    data = []

    for line in l_response.json():
        data.append(json.loads(json.dumps(line)))

    cursor.execute("truncate table edu_gov_expired")
    for item in data:
        my_data = [item[field] for field in fields]
        insert_query = "INSERT INTO edu_gov_expired (ID,TITLE,DEADLINE,CLOSED_DATE,RESPONSIBLE_ID,RESPONSIBLE_LAST_NAME,RESPONSIBLE_NAME,RESPONSIBLE_SECOND_NAME,FULL_NAME,status) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'Просрочено')"
        cursor.execute(insert_query, tuple(my_data))

    cursor.execute("Commit")

def main(argv=None):
    if argv is None:
        argv = sys.argv
    try:        
        argv = sys.argv
        #load_to_postgres()
        
    except Exception as e:
	    print(str(e))	    
        


if __name__ == "__main__":
    main()
		
