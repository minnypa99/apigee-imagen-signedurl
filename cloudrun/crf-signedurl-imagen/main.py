
#example curl
# curl  -H "Authorization: Bearer $(gcloud auth print-identity-token)"   "[cloud run url]" -X POST --data '{"bucket":"[BUCKET_NAME]", "expiry": "[EXPIRY, e.g. 30s, 5m]", objeci":[ {"id": "[IMAGE_ID]", "gcsUri": "[STORAGE_FILE_PATH]", "filename": "[FILE_PATH]"}] }' -H "content-type: application/json"

import json
import os
import urllib.parse
from datetime import datetime, timedelta


import google.auth
from google.auth.transport import requests    
from google.cloud import storage
import functions_framework 

def get_and_sign_object(bucket_name, object_name, expires, credentials):
    # Create storage object to sign
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.get_blob(object_name)

   
    # specify service account only for local development, deployment will use
    #   the assigned service account
    service_account_email = os.environ.get('SVC_ACCT', None)
    if hasattr(credentials, 'service_account_email'):
        service_account_email = credentials.service_account_email
    url = blob.generate_signed_url(
        version="v4",
        expiration=expires,
        service_account_email=service_account_email, 
        access_token=credentials.token,
        method='GET'
        )
    return url

@functions_framework.http
def get_url(request):
    # read POST json, expected format { "bucket": "x", "object": []}
    request_json = request.get_json(silent=True)
    bucket_name = request_json["bucket"]
    predictions = request_json["predictions"]
    expiry = request_json["expiry"]

    print("OK")
    print(bucket_name)
    print(predictions)

    expiry_unit = expiry[-1]
    expiry_val = expiry[0:-1]
    
    delta_unit = {"s" : "seconds", "m": "minutes", "h" : "hours", "d" : "days"}.get(expiry_unit, "seconds")

    print(f'expiry  {expiry_val} {delta_unit}')

    if expiry_unit == "s":
        expires = datetime.utcnow() + timedelta( seconds = int(expiry_val))
    elif expiry_unit == "m":
        expires = datetime.utcnow() + timedelta( minutes = int(expiry_val))
    elif expiry_unit == "h":
        expires = datetime.utcnow() + timedelta( hours = int(expiry_val))
    elif expiry_unit == "d":
        expires = datetime.utcnow() + timedelta( days = int(expiry_val))
    else:
        expires = datetime.utcnow() + timedelta( seconds = 60)

    # Get the default credential on the current environment
    credentials, project_id = google.auth.default()
    # Refresh request to get the access token 
    req = requests.Request()
    credentials.refresh(req)

    #for object_name in qs['object']:
    for object in predictions:
        url = get_and_sign_object(bucket_name, object["filename"], expires, credentials)
        object["signedurl"] = url;
        del object["filename"];


    return json.dumps(request_json, indent=1)

