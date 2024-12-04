
#example curl
# curl  -H "Authorization: Bearer $(gcloud auth print-identity-token)"   "[cloud run url]" -X POST 
# --data '{"verb": "[GET/PUT]", "expiry":"[30s/5m]", "bucket":"[BUCKET_NAME]","objects":[ {"id": "[IMAGE_ID]", "filename": "[STORAGE_FILE_PATH]"}] }' -H "content-type: application/json"

# curl -X PUT -H 'Content-Type: image/png' --upload-file  [IMAGE_FILE_PATH]   "[SIGNED_URL]"


import json
import os
import urllib.parse
from datetime import datetime, timedelta


import google.auth
from google.auth.transport import requests    
from google.cloud import storage
import functions_framework


def signedurl_object(verb, bucket_name, object_name, expires, credentials):
    # Create storage object to sign
    client = storage.Client()

    if verb == "GET":
        bucket = client.get_bucket(bucket_name)
        blob = bucket.get_blob(object_name)
    else:
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(object_name)

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
        method=verb
        )
    return url    

@functions_framework.http
def get_url(request):
    # read POST json, expected format { "bucket": "x", "object": []}
    request_json = request.get_json(silent=True)
    verb = (request_json["verb"]).upper()
    bucket_name = request_json["bucket"]
    objects = request_json["objects"]
    expiry = request_json["expiry"]

    print("OK")
    print(verb)
    print(bucket_name)
    print(objects)

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


    for object in objects:
        url = signedurl_object(verb, bucket_name, object["filename"], expires, credentials)
        object["signedurl"] = url;

    return json.dumps(request_json, indent=2)