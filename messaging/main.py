
from message_create import sms_create
from message_delete import sms_delete
from message_list import sms_list
from message_update import sms_update

def get_request_header_token(method):
    headers = {}

    if method == 'OPTIONS':
        headers['Access-Control-Allow-Origin'] = '*'
        headers['Access-Control-Allow-Methods'] = ["GET", "POST", "OPTIONS", "PUT", "DELETE", "PATCH"]
        headers['Access-Control-Allow-Headers'] = ['Content-Type', 'Authorization']
        headers['Access-Control-Max-Age'] = '3600'  # Cache for max 1 hour = 3600 seconds...
    else:
        headers['Access-Control-Allow-Origin'] = '*'

    return headers
#main function
def admin_message(request):
  try:
    headers = get_request_header_token(request.method)
    if request.method == "OPTIONS":
      return '', '204', headers
    if request.method == "GET":
        print('request.args - {}'.format(request.args))
        print('request.args.to_dict()::::', request.args.to_dict())
        request_json = request.args.to_dict()
    else:
        request_json = request.get_json()
    if request_json["operation"] == "create":
      return sms_create(request_json, headers)
    elif request_json["operation"] == "delete":
      return sms_delete(request_json, headers)
    elif request_json["operation"] == "update":
      return sms_update(request_json, headers)
    elif request_json["operation"] == "read":
      return sms_list(request_json, headers)
    else:
      return {"statusCode": 404, "errorMessage": "Client Error","errorMoreInfo":"Bad Request"}, 404, headers
  except Exception as e:
    print('in exception block::::', str(e))
    return {"statusCode": 500, "errorMessage": "Internal Server Error","errorMoreInfo":str(e)}, 500, headers

