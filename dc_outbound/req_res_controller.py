"""
This file does operation of reading request and setting response
"""
import os
import simplejson as json
from datetime import date,datetime,timedelta
import pandas as pd
import constant as constant



def get_request_details(request):
    req_data = {}
    req_data["user_name"] = request.args.get(constant.USER_NAME, default=constant.DEFAULT_USER_NAME, type=str)
    req_data["vendor_no"] = request.args.get(constant.VENDOR_NUMBER, default=constant.DEFAULT_VENDOR_NUMBER, type=str)
    req_data["supplier_name"] = request.args.get(constant.SUPPLIER_NAME, default=constant.DEFAULT_SUPPLIER_NAME, type=str)
    req_data["dc"] = request.args.get(constant.DC_FILTER, default=constant.DEFAULT_DC_FILTER, type=str)
    req_data["value"] = request.args.get(constant.VALUE_FILTER, default=constant.DEFAULT_VALUE_FILTER, type=str)
    req_data["period"] = request.args.get(constant.PERIOD_FILTER, default=constant.DEFAULT_PERIOD_FILTER, type=str)
    req_data["year"] = request.args.get(constant.YEAR_FILTER, default=constant.DEFAULT_YEAR_FILTER, type=str)
    req_data["week_commencing"] = request.args.get(constant.WEEK_COMMENCE_FILTER,
                                                   default=constant.DEFAULT_WEEK_COMMENCE_FILTER, type=str)
    req_data[constant.PATH] = request.path
    req_data["offset"] = request.args.get(constant.OFFSET_FILTER, default="0", type=str)
    req_data["max_results"] = request.args.get(constant.MAX_RESULTS_FILTER, default=constant.DEFAULT_MAX_RESULTS_FILTER,
                                               type=str)
    req_data["period"] = get_days_count(req_data["period"])
    req_data["prev_week_commencing"] = date_calculate(req_data["week_commencing"], 0, -req_data["period"] + 7)
    req_data["week_commencing"] = get_week_commence(req_data["period"], req_data["week_commencing"],
                                                    req_data["prev_week_commencing"])
    #print(req_data)
    return req_data


def set_response_details(results, req_data):
    if req_data[constant.PATH] == "/distributioncenteroutbound/download":
        res_data = {"cloudStorage": results["records"]}
        res_data = json.dumps(res_data)
        return res_data
    res_data = {"summary": "", "details": [], "metadata": ""}
    if len(results["records"]) == 0:
        return {constant.MESSAGE: constant.ERROR_NO_CONTENT}
    df = pd.DataFrame(results["records"])
    total_records = results["total_records"]
    cols2 = [col for col in df.columns if
             col not in ['R_DATE', 'DCIssues', 'prev_week_index', 'period_totaldcissues', 'Period_total_pre_week_inx']]
    cols3 = [col for col in df.columns if col in ['avrg_totaldcoutbound', 'avrg_total_prev_dcoutbound']]
    df = df.drop_duplicates()
    df = df.fillna("")
    df1 = df[cols2]
    df1 = df1.drop_duplicates('product_name', keep='last').head(int(req_data["max_results"])).reset_index(drop=True)
    temp_dict = df[cols3].to_dict('records')[0]
    temp_dict['avgOutbound'] = temp_dict.pop('avrg_totaldcoutbound')
    temp_dict['avgPreviousWeekIndex'] = temp_dict.pop('avrg_total_prev_dcoutbound')
    res_data["summary"] = temp_dict
    i_list = []
    #print(res_data)
    for i in range(0, len(df1.index)):
        x = {"productNumber": df1['product_name'][i], "productDescription": df1['item_description'][i],
             "tuc": df1['tuc'][i],
             "distributionCenter": df1['R_DEPOTCODE'][i], "week": []}
        for ind in range(0, len(df.index)):
            if df1['product_name'][i] == df['product_name'][ind]:
                y = {"date": df['R_DATE'][ind], "dcOutbound": df['DCIssues'][ind], "prev_DCOutbound": df['prev_DCIssues'][ind],
                     "prevWeekIndex": df['prev_week_index'][ind]}
                x["week"].append(y)
                i_list.append({"date": df['R_DATE'][ind], "currentDC": df['totaldcissues'][ind],"previousDC": df['total_Prev_dcissues'][ind], "previousWeekIndex": df['total_pre_week_inx'][ind]})
        res_data["details"].append(x)
    res_list = [i for n, i in enumerate(i_list) if i not in i_list[n + 1:]]
    res_data["total"] = res_list
    res_data = set_metadata(res_data, req_data, total_records)
    return res_data


def get_environment_variable(var):
    return os.environ.get(var)


def convert_to_dict(results):
    records = [dict(row) for row in results]
    return records


def date_calculate(date_string, week_value, day_value):
    str_to_date = datetime.strptime(date_string, "%Y-%m-%d")
    new_date = str_to_date + timedelta(weeks=week_value, days=day_value)
    date_to_str = new_date.strftime("%Y-%m-%d")
    return date_to_str


def get_days_count(period):
    days = {
        "1 day": 1,
        "1 week": 7,
        "4 week": 28,
        "12 week": 84
    }
    return days[period] if period in days else 1


def get_week_commence(period, wc, pwc):
    days = {
        1: wc,
        7: wc,
        28: pwc,
        84: pwc
    }
    return days[period] if period in days else wc


def json_serial(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()


def set_metadata(res_data, req_data, total_records):
    final_response = {"limit": int(req_data["max_results"]), "offset": int(req_data["offset"]),
                      "totalCount": total_records}
    res_data["metadata"] = final_response
    res_data = json.dumps(res_data, default=json_serial)
    return res_data
