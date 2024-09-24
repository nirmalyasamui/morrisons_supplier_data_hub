'''
***********************************************************************************************************************
Purpose: Common utility file for implementing all non BigQuerfunctionalities.
Developer: Somnath De (somnath.de@morrisonsplc.co.uk)
***********************************************************************************************************************
'''

import os
import uuid
from urllib.parse import unquote
from datetime import date
from datetime import datetime
from datetime import timedelta
import constant as constant
from dateutil import tz
from dateutil.relativedelta import relativedelta
import google.cloud.logging

client = google.cloud.logging.Client()
client.setup_logging()
import logging
import fyutil as fy


bst_tz = tz.gettz('Europe/London')



linkedColumn = {'region': 'region_name',
                'storeFormat': 'storeformat_name',
                'store': 'store',
                'locationLongName': 'location_long_name',
                'commercialCategory': 'commercial_category',
                'commercialClass': 'commercial_class',               
                'category': 'commercial_category',
                'subCategory': 'commercial_class',
                'subcategory': 'commercial_class',
                'brand': 'brand_name',
                'brandName': 'brand_name',
                'sku': 'sku_item_number',
                'tuc' : 'tuc',
                'min': 'sku_item_number',
                'itemNumber': 'sku_item_number',
                'locationId': 'location_id',
                'storeId': 'location_id',
                'subChannel': 'sub_channel',
                'purchaseOrderStatus': 'pchse_ordr_status',
                'depot': 'location_id',
                'storeno' : 'BSNS_UNIT_CD',
                'distributingDC' : 'FROM_BSNS_UNIT_CD',
                'commercial_category':'commercial_category',                
                'commercial_class': 'commercial_class', 
                'brand_name':'brand_name',
                'sku_item_number':'sku_item_number',
                'stores':'location',
                'division':'commercial_division'
                }


DATE_FORMAT_YYYY_MM_DD = '%Y-%m-%d'
DATE_FORMAT_DD_MM_YYYY = '%d-%m-%Y'

class CommonUtil:

    @staticmethod
    def get_reporting_period(path, request):
        reporting_period = request.args.get(constant.REPORTING_PERIOD, default=constant.DEFAULT_REPORTING_PERIOD,
                                            type=str)

        if path in constant.WM_PATH:                       
            reporting_period = request.args.get(constant.REPORTING_PERIOD, default=constant.DEFAULT_REPORTING_PERIOD_WM,
                                            type=str)
        elif path.__contains__('available'):
            reporting_period = request.args.get(constant.REPORTING_PERIOD, default=constant.DEFAULT_REPORTING_PERIOD_AP,
                                            type=str)
        return reporting_period

    @staticmethod
    def get_comparison_period(path, request):
        #changes for wastage & Markdown 
        comparison_period = request.args.get(constant.COMPARISON_PERIOD, default=constant.DEFAULT_COMPARISON_PERIOD,
                                            type=str)    
        if path.__contains__('available'):
            comparison_period = request.args.get(constant.COMPARISON_PERIOD, default=constant.DEFAULT_COMPARISON_PERIOD_AP,
                                            type=str)                       
        return comparison_period

    @staticmethod
    def get_sort_by(path, request):
        sort_by = request.args.get(constant.SORT_BY, default=constant.DEFAULT_SORT_BY, type=str)
        if(path in constant.PR_PATH):   
            sort_by = request.args.get(constant.SORT_BY, default=constant.DEFAULT_SORT_BY_PR, type=str)

        elif(path in constant.RP_PATH): 
            sort_by = request.args.get(constant.SORT_BY, default=constant.DEFAULT_SORT_DIRECTION_RP, type=str) 
            
        elif(path in constant.WM_PATH):
            sort_by = request.args.get(constant.SORT_BY, default=constant.DEFAULT_SORT_BY_WM, type=str)

        elif path.__contains__('available'):   
            sort_by = request.args.get(constant.SORT_BY, default=constant.DEFAULT_SORT_BY_AP, type=str)
        elif(path in constant.SS_PATH):
            sort_by = request.args.get(constant.SORT_BY, default=None, type=str)
        elif(path in constant.DS_PATH):
            sort_by = request.args.get(constant.SORT_BY, default=None, type=str)
        
        return sort_by

    @staticmethod

    def get_filters(request, path, full_path, temp_array):
        filters = {}
        if len(full_path) > len(path):
            parameters = temp_array[1].split('&')
            filters = {}

            for parameter in parameters:
                key_val = parameter.split('=')

                params = request.args.get(key_val[0]).split('Ç')
                decoded_params = []
                for param in params:
                    decoded_params.append(unquote(param))

                filters[key_val[0]] = decoded_params

        return filters
    
    @staticmethod
    def add_pop_filters(pop_params, filter):
        pop_params.append(filter)
        return pop_params


    @staticmethod
    def pop_filters(filters):
        pop_params = []
        pop_params = CommonUtil.add_pop_filters(pop_params, constant.LIMIT)
        pop_params = CommonUtil.add_pop_filters(pop_params, constant.PAGE)
        pop_params = CommonUtil.add_pop_filters(pop_params, constant.OFFSET)
        pop_params = CommonUtil.add_pop_filters(pop_params, constant.SAVE)
        pop_params = CommonUtil.add_pop_filters(pop_params, constant.STARTS_WITH)
        pop_params = CommonUtil.add_pop_filters(pop_params, constant.VENDOR_NUMBER)
        pop_params = CommonUtil.add_pop_filters(pop_params, constant.QUERY_TITLE)
        pop_params = CommonUtil.add_pop_filters(pop_params, constant.SALE_TYPE)
        pop_params = CommonUtil.add_pop_filters(pop_params, constant.REPORTING_PERIOD)
        pop_params = CommonUtil.add_pop_filters(pop_params, constant.START_DATE)
        pop_params = CommonUtil.add_pop_filters(pop_params, constant.END_DATE)
        pop_params = CommonUtil.add_pop_filters(pop_params, constant.USERID)
        pop_params = CommonUtil.add_pop_filters(pop_params, constant.USERNAME)
        pop_params = CommonUtil.add_pop_filters(pop_params, constant.REPORT_NAME)
        pop_params = CommonUtil.add_pop_filters(pop_params, constant.PO_NUMBER)
        pop_params = CommonUtil.add_pop_filters(pop_params, constant.PROMOTION_NUMBER)
        pop_params = CommonUtil.add_pop_filters(pop_params, constant.PROMOTION_START_DATE)
        pop_params = CommonUtil.add_pop_filters(pop_params, constant.PROMOTION_END_DATE)
        pop_params = CommonUtil.add_pop_filters(pop_params, constant.ITEM_STATUS)
        pop_params = CommonUtil.add_pop_filters(pop_params, constant.PROMOIND)
        pop_params = CommonUtil.add_pop_filters(pop_params, constant.DELIVERY)
        pop_params = CommonUtil.add_pop_filters(pop_params, constant.ITEM_NUMBER)
        pop_params = CommonUtil.add_pop_filters(pop_params, constant.SORT_BY)
        pop_params = CommonUtil.add_pop_filters(pop_params, constant.SORT_DIRECTION)
        pop_params = CommonUtil.add_pop_filters(pop_params, constant.SORT_BY_27)
        pop_params = CommonUtil.add_pop_filters(pop_params, constant.SORT_DIRECTION_27)
        pop_params = CommonUtil.add_pop_filters(pop_params, constant.TARGET)
        pop_params = CommonUtil.add_pop_filters(pop_params, constant.AVAILABILITY_BY)
        pop_params = CommonUtil.add_pop_filters(pop_params, constant.COMPARISON_PERIOD)
        pop_params = CommonUtil.add_pop_filters(pop_params, constant.WASTE_TYPE)
        pop_params = CommonUtil.add_pop_filters(pop_params, constant.WASTE_AS)
        pop_params = CommonUtil.add_pop_filters(pop_params, constant.WASTE_BY)
        pop_params = CommonUtil.add_pop_filters(pop_params, constant.WASTE_MEASURE)
        pop_params = CommonUtil.add_pop_filters(pop_params, constant.DEPOT_STOCK)

        for p in pop_params:
            if p in filters:
                filters.pop(p)
        

    @staticmethod
    def get_request_details(request):
        full_path = request.full_path.strip('?')
        temp_array = full_path.split('?')
        path = temp_array[0]
        limit = request.args.get(constant.LIMIT, default=None, type=None)
        offset = request.args.get(constant.OFFSET, default='0', type=str)
        startswith = request.args.get(constant.STARTS_WITH, default='', type=str)
        save = request.args.get(constant.SAVE, default='FALSE', type=str).upper()
        vendor_number = request.args.get(constant.VENDOR_NUMBER, default=constant.DEFAULT_VENDOR_NUMBER,
                                         type=str).upper()
        supplier_name = request.args.get(constant.SUPPLIER_NAME, default=constant.DEFAULT_SUPPLIER_NAME,
                                         type=str)                                 
        reporting_period = CommonUtil.get_reporting_period(path, request)
        
        in_start_date = request.args.get(constant.START_DATE, default=constant.DEFAULT_START_DATE, type=str)
        in_end_date = request.args.get(constant.END_DATE, default=constant.DEFAULT_END_DATE, type=str)
        userid = request.args.get(constant.USERID, default=constant.DEFAULT_USERID, type=str)
        username = request.args.get(constant.USERNAME, default=constant.DEFAULT_USER_NAME, type=str)
        report_name = request.args.get(constant.REPORT_NAME, default=constant.DEFAULT_REPORT_NAME, type=str)
        po_number = request.args.get(constant.PO_NUMBER, default=constant.DEFAULT_PO_NUMBER, type=str)
        promotion_number = request.args.get(constant.PROMOTION_NUMBER, default=constant.DEFAULT_PROMO_NUMBER, type=str)
        promotion_start_date = request.args.get(constant.PROMOTION_START_DATE, default=None, type=str)
        promotion_end_date = request.args.get(constant.PROMOTION_END_DATE, default=None, type=str)
        loc_type = request.args.get(constant.LOCATION_TYPE, default=constant.DEFAULT_LOCATION_TYPE, type=str)
        query_title = request.args.get(constant.QUERY_TITLE, default=constant.DEFAULT_QUERY_TITLE, type=str)
        sale_type = request.args.get(constant.SALE_TYPE, default=constant.DEFAULT_SALE_TYPE, type=str)
        sale_measure = request.args.get(constant.SALE_MEASURE, default=None, type=str)
        sale_channel = request.args.get(constant.SALE_CHANNEL, default=None, type=str)
        #added for depot stock report
        stock_type = request.args.get(constant.STOCK_TYPE, default=constant.DEFAULT_STOCK_TYPE, type=str)
        stock_measure = request.args.get(constant.STOCK_MEASURE, default=constant.DEFAULT_STOCK_MEASURE, type=str)
        #end of changes to depot stock report
        item_number = request.args.get(constant.ITEM_NUMBER, default=constant.DEFAULT_ITEM_NUMBER,
                                         type=str).upper()
        sku = request.args.get(constant.SKU, default=constant.DEFAULT_ITEM_NUMBER,
                                         type=str).upper() 

        item_status = request.args.get(constant.ITEM_STATUS, '',type=str)
        prom_status = request.args.get(constant.PROMOIND, '',type=str)                                         
        delivery_ind = request.args.get(constant.DELIVERY, '',type=str)
        itemstatus= request.args.get(constant.ITEMSTATUS, '',type=str)

        comparison_period = CommonUtil.get_comparison_period(path, request)
        
        waste_as = request.args.get(constant.WASTE_AS, default=constant.DEFAULT_WASTE_AS, type=str)
        waste_by = request.args.get(constant.WASTE_BY, default=constant.DEFAULT_WASTE_BY, type=str)
        waste_measure = request.args.get(constant.WASTE_MEASURE, default=constant.DEFAULT_WASTE_MEASURE, type=str)
        waste_type = request.args.get(constant.WASTE_TYPE, default=constant.DEFAULT_WASTE_TYPE, type=str)
        target = request.args.get(constant.TARGET, default=constant.DEFAULT_TARGET, type=str)
        availability_by = request.args.get(constant.AVAILABILITY_BY, default=constant.DEFAULT_AVAILABILITY_BY, type=str)
        depot_stock= request.args.get(constant.DEPOT_STOCK, default=constant.DEFAULT_DEPOT_STOCK, type=str)

        sort_by = CommonUtil.get_sort_by(path, request)
                    
        sort_direction = request.args.get(constant.SORT_DIRECTION, default=constant.DEFAULT_SORT_DIRECTION, type=str).upper()  
        sort_by_27 = request.args.get(constant.SORT_BY, default=constant.DEFAULT_SORT_BY_27, type=str)
        sort_direction_27 = request.args.get(constant.SORT_DIRECTION, default=constant.DEFAULT_SORT_DIRECTION_27, type=str)
        store=  request.args.get(constant.STORE, default=constant.DEFAULT_STORE, type=str)
        dist_dc=  request.args.get(constant.DISTRIBUTING_DC, default=constant.DEFAULT_DC, type=str)  
        request_dictionary = {}

        filters = CommonUtil.get_filters(request, path, full_path, temp_array)
        if constant.LOCATION_TYPE not in filters:
            filters[constant.LOCATION_TYPE] = constant.DEFAULT_LOCATION_TYPE.split('Ç')
        
        CommonUtil.pop_filters(filters)

        
        for param in linkedColumn:
            if param in filters:
                filters[linkedColumn[param]] = filters[param]

        request_dictionary[constant.FILTERS] = filters

        # Calculate start and end date for search queries based on reporting period selected...
        if path in constant.WM_PATH or path.__contains__('available') or path in constant.RP_PATH or path in constant.SS_PATH: 
            start_date=None 
            end_date=None
            po_start_date=None
        else:   
            start_date = CommonUtil._get_start_date(reporting_period, in_start_date)
            end_date = CommonUtil._get_end_date(reporting_period, in_end_date)
            if(start_date is not None):
              po_start_date= CommonUtil._get_past_date(start_date)

        # Common parameters that are not part of filters...
        request_dictionary[constant.PATH] = path
        request_dictionary[constant.LIMIT] = limit
        request_dictionary[constant.PAGE] = offset
        request_dictionary[constant.SAVE] = save
        request_dictionary[constant.STARTS_WITH] = startswith
        request_dictionary[constant.VENDOR_NUMBER] = vendor_number
        request_dictionary[constant.SUPPLIER_NAME] = supplier_name
        request_dictionary[constant.REPORTING_PERIOD] = reporting_period
        request_dictionary[constant.START_DATE] = start_date
        if start_date is not None and start_date != '':
            request_dictionary[constant.PREVIOUS_START_DATE] = CommonUtil._get_last_year_date(start_date)
            request_dictionary[constant.PREVIOUS_PO_STARTDATE]=CommonUtil._get_last_year_date(po_start_date)
        request_dictionary[constant.END_DATE] = end_date
        if end_date is not None and end_date != '':
            request_dictionary[constant.PREVIOUS_END_DATE] = CommonUtil._get_last_year_date(end_date)
        request_dictionary[constant.METHOD] = request.method.upper()
        request_dictionary[constant.USERID] = userid
        request_dictionary[constant.USERNAME] = username
        request_dictionary[constant.REPORT_NAME] = report_name
        request_dictionary[constant.PO_NUMBER] = po_number
        request_dictionary[constant.PROMOTION_NUMBER] = promotion_number
        request_dictionary[constant.PROMOTION_START_DATE] = promotion_start_date
        request_dictionary[constant.PROMOTION_END_DATE] = promotion_end_date
        request_dictionary[constant.LOCATION_TYPE] = loc_type
        request_dictionary[constant.QUERY_TITLE] = query_title
        request_dictionary[constant.SALE_TYPE] = sale_type
        request_dictionary[constant.SALE_MEASURE] = sale_measure
        request_dictionary[constant.SALE_CHANNEL] = sale_channel
        request_dictionary[constant.ITEM_NUMBER] = item_number
        request_dictionary[constant.SKU] = sku
        request_dictionary[constant.ITEM_STATUS] = item_status
        request_dictionary[constant.PROMOIND] = prom_status
        request_dictionary[constant.DELIVERY] = delivery_ind
        request_dictionary[constant.PO_STARTDATE] = po_start_date
        request_dictionary[constant.ITEMSTATUS] = itemstatus
        request_dictionary[constant.TARGET] = target
        request_dictionary[constant.AVAILABILITY_BY] = availability_by
        request_dictionary[constant.DEPOT_STOCK] = depot_stock
        #changes for wastage & Markdown
        request_dictionary[constant.COMPARISON_PERIOD] = comparison_period
        request_dictionary[constant.WASTE_AS] = waste_as
        request_dictionary[constant.WASTE_BY] = waste_by
        request_dictionary[constant.WASTE_MEASURE] = waste_measure
        request_dictionary[constant.WASTE_TYPE] = waste_type
        #end of changes for wastage & Markdown

        if(request_dictionary[constant.PATH] in constant.SF_PATH):
            request_dictionary[constant.SORT_BY_27] = sort_by_27
            request_dictionary[constant.SORT_DIRECTION_27] = sort_direction_27

        else:    
            request_dictionary[constant.SORT_BY] = sort_by
            request_dictionary[constant.SORT_DIRECTION] = sort_direction                  

        #added for depotstock report
        request_dictionary[constant.STOCK_TYPE] = stock_type
        request_dictionary[constant.STOCK_MEASURE] = stock_measure
        request_dictionary[constant.STORE] = store
        request_dictionary[constant.DISTRIBUTING_DC] = dist_dc
        #end of changes to depotstock report

        return request_dictionary

    @staticmethod
    def _get_last_year_date(in_date):
        # As per Parikshit Soni the past year date will be calculated by removing 52 weeks from the current date...
        days = (52 * 7)
        date_time_obj = datetime.strptime(in_date, DATE_FORMAT_YYYY_MM_DD)
        temp_date = date_time_obj - timedelta(days=days)
        last_year_date = temp_date.strftime(DATE_FORMAT_YYYY_MM_DD)
        return last_year_date

    @staticmethod
    def _get_past_date(in_date):
        # As per Parikshit Soni the past year date will be calculated by removing 52 weeks from the current date...
        days = (4 * 7)
        date_time_obj = datetime.strptime(in_date, DATE_FORMAT_YYYY_MM_DD)
        temp_date = date_time_obj - timedelta(days=days)
        past_date = temp_date.strftime(DATE_FORMAT_YYYY_MM_DD)
        return past_date  

    @staticmethod
    def get_start_date_for_weekly_periods (reporting_period):
        today = datetime.today()
        ago = 1 + today.weekday()
        
        if reporting_period == constant.PREVIOUS_WEEK:
            temp_start_date = today - timedelta(days=8)
            start_date = temp_start_date.strftime(DATE_FORMAT_YYYY_MM_DD)
            return start_date
        elif reporting_period == constant.WEEK_TO_DATE:
            monday = today - timedelta(days=today.weekday())
            start_date = monday.strftime(DATE_FORMAT_YYYY_MM_DD)
            return start_date
        elif any((reporting_period == constant.LAST_FULL_WEEK , reporting_period.upper()==constant.WEEK)):
            last_sunday = today - timedelta(days=ago)
            days = (1 * 7) - 1
            previous_day = last_sunday - timedelta(days=days)
            start_date = previous_day.strftime(DATE_FORMAT_YYYY_MM_DD)
            return start_date
        elif reporting_period == constant.WEEKS_52:
            last_sunday = today - timedelta(days=ago)
            days = (52 * 7) - 1
            previous_day = last_sunday - timedelta(days=days)
            start_date = previous_day.strftime(DATE_FORMAT_YYYY_MM_DD)
            return start_date
        elif reporting_period == constant.WEEK_52:
            last_sunday = today - timedelta(days=ago)
            days = (1 * 7) - 1
            previous_day = last_sunday - timedelta(days=days) - timedelta(weeks=52)
            start_date = previous_day.strftime(DATE_FORMAT_YYYY_MM_DD)
            return start_date            
        elif reporting_period == constant.WEEKS_12:
            last_sunday = today - timedelta(days=ago)
            days = (12 * 7) - 1
            previous_day = last_sunday - timedelta(days=days)
            start_date = previous_day.strftime(DATE_FORMAT_YYYY_MM_DD)
            return start_date
        elif reporting_period == constant.WEEKS_13:
            last_sunday = today - timedelta(days=ago)
            days = (13 * 7) - 1
            previous_day = last_sunday - timedelta(days=days)
            start_date = previous_day.strftime(DATE_FORMAT_YYYY_MM_DD)
            return start_date
        elif reporting_period == constant.WEEKS_4:
            last_sunday = today - timedelta(days=ago)
            days = (4 * 7) - 1
            previous_day = last_sunday - timedelta(days=days)
            start_date = previous_day.strftime(DATE_FORMAT_YYYY_MM_DD)
            return start_date
        
    @staticmethod
    def get_start_date_for_day_wise_periods(reporting_period, in_start_date):
        today = datetime.today()
        
        if reporting_period == constant.DAYS_28:
            if in_start_date is not None:
                in_start_date = in_start_date - timedelta(days=27)
            else:
                in_start_date = today - timedelta(days=28)
            start_date = in_start_date.strftime(DATE_FORMAT_YYYY_MM_DD)
            return start_date        
        elif reporting_period == constant.PREVIOUS_DAY:
            in_start_date = today - timedelta(days=2)
            start_date = in_start_date.strftime(DATE_FORMAT_YYYY_MM_DD)
            return start_date
        elif reporting_period == constant.DAY_PREV_YEAR:
            in_start_date = (today - timedelta(weeks=52)) - timedelta(days=1)
            start_date = in_start_date.strftime(DATE_FORMAT_YYYY_MM_DD)
            return start_date
        elif reporting_period == constant.LAST_SEVEN_DAYS:
            if in_start_date is not constant.DEFAULT_START_DATE:
                in_start_date = in_start_date- timedelta(days=7)
            else:
                in_start_date = today - timedelta(days=7)           
            start_date = in_start_date.strftime(DATE_FORMAT_YYYY_MM_DD)
            return start_date
        elif reporting_period == constant.YEAR_TO_DATE:
            dates= fy.FYUtil._get_business_year()                
            start_date = dates[0]['YTD_START_DATE'].strftime(DATE_FORMAT_YYYY_MM_DD)
            return start_date
        elif reporting_period == constant.MONTH_TO_DATE:
            first_date_of_month = date.today().replace(day=1)
            start_date = first_date_of_month.strftime(DATE_FORMAT_YYYY_MM_DD)
            return start_date
        elif any((reporting_period == constant.LAST_DAY,reporting_period.upper() == constant.DAY)):
            print("inside_start_date")
            previous_day = today - timedelta(days=1)
            start_date = previous_day.strftime(DATE_FORMAT_YYYY_MM_DD)
            return start_date
            
    @staticmethod
    def get_pystartdate(start_date,today)  :
            if(start_date is not None):
               start_date = start_date - timedelta(days=27) - timedelta(weeks=52)
            else:
               start_date= today - timedelta(days=28) - timedelta(weeks=52)  
            return  start_date              
        
    @staticmethod
    def get_start_date_for_prev_periods(reporting_period, in_start_date):
        today = datetime.today()
        ago = 1 + today.weekday() 

        #Changes for Wastage and Markdown report
        if reporting_period == constant.PREV_28_DAYS:
            if(in_start_date is not None):
               in_start_date = in_start_date - timedelta(days=56)
            else:
               in_start_date= today - timedelta(days=56)            
            start_date = in_start_date.strftime(DATE_FORMAT_YYYY_MM_DD)
            return start_date
        elif reporting_period == constant.PREV_YEAR_28_DAYS:          
            in_start_date = CommonUtil.get_pystartdate(in_start_date,today) 
            start_date = in_start_date.strftime(DATE_FORMAT_YYYY_MM_DD)
            return start_date
        elif reporting_period == constant.PREVIOUS_WEEK:
            temp_start_date = today - timedelta(days=8)
            start_date = temp_start_date.strftime(DATE_FORMAT_YYYY_MM_DD)
            return start_date        
        elif reporting_period == constant.PREV_YEAR:
            in_start_date = date(date.today().year-1, 1, 1)
            start_date = in_start_date.strftime(DATE_FORMAT_YYYY_MM_DD)
            return start_date
        elif reporting_period == constant.PREV_WEEKS_52:
            last_sunday = today - timedelta(days=ago)
            days = (52 * 7 * 2) - 1
            previous_day = last_sunday - timedelta(days=days)
            start_date = previous_day.strftime(DATE_FORMAT_YYYY_MM_DD)
            return start_date
        elif reporting_period == constant.PREV_WEEKS_12:            
            last_sunday = today - timedelta(days=ago)
            days = (12 * 7 * 2) - 1
            previous_day = last_sunday - timedelta(days=days)
            start_date = previous_day.strftime(DATE_FORMAT_YYYY_MM_DD)
            return start_date
        elif reporting_period == constant.PREV_YEAR_WEEKS_12:
            last_sunday = today - timedelta(days=ago)
            days = (52 * 7) - 1  #previous year from last sunday
            days = days + ( 12 *7)  #previous 12 weeks start in previous year
            in_start_date = last_sunday - timedelta(days=days)
            start_date = in_start_date.strftime(DATE_FORMAT_YYYY_MM_DD)
            return start_date 
        #end of chnages for Wastage & Markdown
        elif reporting_period == constant.PREV_WEEKS_4:            
            last_sunday = today - timedelta(days=ago)
            days = (4 * 7 * 2) - 1
            previous_day = last_sunday - timedelta(days=days)
            start_date = previous_day.strftime(DATE_FORMAT_YYYY_MM_DD)
            return start_date
        elif reporting_period == constant.PREV_YEAR_WEEKS_4:
            last_sunday = today - timedelta(days=ago)
            days = (52 * 7) - 1  #previous year from last sunday
            days = days + ( 4 *7)  #previous 4 weeks start in previous year
            in_start_date = last_sunday - timedelta(days=days)
            start_date = in_start_date.strftime(DATE_FORMAT_YYYY_MM_DD)
            return start_date 
        elif reporting_period == constant.DAY_PREV_YEAR:
            in_start_date = (today - timedelta(weeks=52)) - timedelta(days=1)
            start_date = in_start_date.strftime(DATE_FORMAT_YYYY_MM_DD)
            return start_date     
        #end of chnages for Wastage & Markdown,availability

    @staticmethod
    def _get_start_date(reporting_period, in_start_date):
        today = datetime.today()
        
        if in_start_date != 'None' and type(in_start_date) == str:
            in_start_date = datetime.strptime(in_start_date, DATE_FORMAT_YYYY_MM_DD)

        if str(reporting_period).casefold().__contains__('prev'):
            return CommonUtil.get_start_date_for_prev_periods(reporting_period, in_start_date)
        
        if str(reporting_period).casefold().__contains__('week'):
            return CommonUtil.get_start_date_for_weekly_periods(reporting_period)
        
        if str(reporting_period).casefold().__contains__('day') or str(reporting_period).casefold().__contains__('date'):
            return CommonUtil.get_start_date_for_day_wise_periods(reporting_period, in_start_date)

        else:
            if in_start_date == constant.DEFAULT_START_DATE:
                previous_day = today - timedelta(days=1)
                start_date = previous_day.strftime(DATE_FORMAT_YYYY_MM_DD)
                return start_date
            if type(in_start_date) != str:
                return in_start_date.strftime(DATE_FORMAT_YYYY_MM_DD)
            return in_start_date
        

    @staticmethod
    def get_prev_end_date(reporting_period, in_end_date):
        today = datetime.today()
        ago = 1 + today.weekday()
        
        if reporting_period == constant.PREVIOUS_WEEK:
            in_end_date = today - timedelta(days=8)
            end_date = in_end_date.strftime(DATE_FORMAT_YYYY_MM_DD)
            return end_date
        elif reporting_period == constant.DAY_PREV_YEAR:
            previous_day = (today - timedelta(weeks=52)) - timedelta(days=1)
            end_date = previous_day.strftime(DATE_FORMAT_YYYY_MM_DD)
            return end_date
        
        #Changes for Wastage and Markdown report
        elif reporting_period == constant.PREV_28_DAYS:
            if in_end_date is not None:
                in_end_date = in_end_date - timedelta(days=29)
            else:
                in_end_date = today - timedelta(days=29)
            end_date = in_end_date.strftime(DATE_FORMAT_YYYY_MM_DD)
            return end_date
        elif reporting_period == constant.PREV_YEAR_28_DAYS:
            if in_end_date is not None: 
                in_end_date = in_end_date - timedelta(weeks=52)
            else:
                in_end_date = today - timedelta(days=1) - timedelta(weeks=52)
            end_date = in_end_date.strftime(DATE_FORMAT_YYYY_MM_DD)
            return end_date
        elif reporting_period == constant.PREV_YEAR:
            in_end_date = date(date.today().year-1, 12, 31)
            end_date = in_end_date.strftime(DATE_FORMAT_YYYY_MM_DD)
            return end_date 
        elif reporting_period == constant.PREV_WEEKS_12:            
            last_sunday = today - timedelta(days=ago)
            days = (12 * 7 )
            previous_day = last_sunday - timedelta(days=days)
            start_date = previous_day.strftime(DATE_FORMAT_YYYY_MM_DD)
            return start_date
        elif reporting_period == constant.PREV_WEEKS_4:            
            last_sunday = today - timedelta(days=ago)
            days = (4 * 7 )
            previous_day = last_sunday - timedelta(days=days)
            start_date = previous_day.strftime(DATE_FORMAT_YYYY_MM_DD)
            return start_date
        elif reporting_period in [constant.PREV_YEAR_WEEKS_4,constant.PREV_YEAR_WEEKS_12, constant.PREV_WEEKS_52]:
            last_sunday = today - timedelta(days=ago)
            days = (52 * 7)
            in_end_date = last_sunday - timedelta(days=days)
            start_date = in_end_date.strftime(DATE_FORMAT_YYYY_MM_DD)
            return start_date 

        
        

    
    @staticmethod
    def _get_end_date(reporting_period, in_end_date):
        today = datetime.today()
        ago = 1 + today.weekday()
        if str(reporting_period).lower().__contains__('prev'):
            return CommonUtil.get_prev_end_date(reporting_period, in_end_date)
        elif reporting_period in [constant.WEEKS_12, constant.WEEKS_13, constant.WEEKS_4, constant.LAST_FULL_WEEK, constant.WEEKS_52]:
            days_back = 1 + today.weekday()
            previous_day = today - timedelta(days=days_back)
            end_date = previous_day.strftime(DATE_FORMAT_YYYY_MM_DD)
            return end_date
        elif reporting_period == constant.WEEK_52:
            last_sunday = today - timedelta(days=ago)
            days = (1 * 7) - 1
            previous_day = last_sunday - timedelta(days=days) - timedelta(weeks=52)
            e_date = previous_day + timedelta(days=6)
            end_date = e_date.strftime(DATE_FORMAT_YYYY_MM_DD)
            return end_date
        elif reporting_period == constant.YEAR_TO_DATE:  
            dates= fy.FYUtil._get_business_year()                
            end_date = dates[0]['YTD_END_DATE'].strftime(DATE_FORMAT_YYYY_MM_DD)
            return end_date 
        elif reporting_period in [constant.DAYS_28,constant.LAST_SEVEN_DAYS]:
            if in_end_date is not None and in_end_date!=constant.DEFAULT_END_DATE:
                previous_day = in_end_date
            else:
                previous_day = today - timedelta(days=1)
            end_date = previous_day.strftime(DATE_FORMAT_YYYY_MM_DD)
            return end_date  
        #end of chnages for Wastage & Markdown

        else:
            if in_end_date == constant.DEFAULT_END_DATE:
                previous_day = today - timedelta(days=1)
                end_date = previous_day.strftime(DATE_FORMAT_YYYY_MM_DD)
                return end_date
            if type(in_end_date)!=str:
                return in_end_date.strftime(DATE_FORMAT_YYYY_MM_DD)
            return in_end_date


    @staticmethod
    def get_filter_value(request_dictionary, filter_name):
        filter_names = []
        if constant.FILTERS in request_dictionary:
            filters = request_dictionary[constant.FILTERS]

            if filter_name in filters:
                filter_names = filters[filter_name]

        filter_as_string = ''
        if filter_names is not None and len(filter_names) > 0:
            for filter_name in filter_names:
                filter_as_string = '"' + filter_name.upper() + '",' + filter_as_string

        
        filter_as_string = filter_as_string.strip(',')  # Remove the last ,

        return filter_as_string        




    @staticmethod
    def get_filter_value_as_string(request_dictionary, filter_name):
        filter_names = []
        if constant.FILTERS in request_dictionary:
            filters = request_dictionary[constant.FILTERS]

            if filter_name in filters:
                filter_names = filters[filter_name]

        filter_as_string = ''
        if filter_names is not None and len(filter_names) > 0:
            for filter_name in filter_names:
                filter_as_string = '"' + filter_name.upper() + '",' + filter_as_string

        if len(filter_as_string) == 0:
            filter_as_string = 'UPPER(IFNULL(' + filter_name + ',"NULLVALUE"))'
        else:
            filter_as_string = filter_as_string.strip(',')  # Remove the last ,

        return filter_as_string
    
    @staticmethod
    def get_filter_value_as_string_col(request_dictionary, filter_name,col_name):
        filter_names = []
        if constant.FILTERS in request_dictionary:
            filters = request_dictionary[constant.FILTERS]

            if filter_name in filters:
                filter_names = filters[filter_name]

        filter_as_string = ''
        if filter_names is not None and len(filter_names) > 0:
            for filter_name in filter_names:
                filter_as_string = '"' + filter_name.upper() + '",' + filter_as_string

        if len(filter_as_string) == 0:
            filter_as_string = 'UPPER(IFNULL(' + col_name + ',"NULLVALUE"))'
        else:
            filter_as_string = filter_as_string.strip(',')  # Remove the last ,

        return filter_as_string

    

    @staticmethod
    def _format_output(summary_result, graph_result):
        if summary_result and graph_result:
            graph_result = CommonUtil.get_chart_date(graph_result)
            final_result = {
                'summary':{
                    'currentForecast':{
                        'latestValue':summary_result["currentForecastlatestValue"],
                        'previousValue':summary_result["previousForecastlatestValue"]
                    },
                    'comparablePeriodForecast':{
                        'latestValue':summary_result["comparablePeriodForecastlatestValue"],
                        'previousValue':summary_result["comparablePeriodForecastpreviousValue"],
                        'change':summary_result["comparableChangeValue"]
                    },
                    'dailyAverage': {
                        'latestValue': summary_result["dailyAverageForecastlatestValue"],
                        'previousValue': summary_result["dailyAverageForecastpreviousValue"],
                        'change': summary_result["dailyAverageChange"]
                    },
                    'absoluteChange': {
                        'latestValue': summary_result["absoluteChangeValue"],
                        'change': summary_result["absoluteChangePercentage"],
                    }
                },
                'chartData':graph_result
            }
            return final_result
        else:
            return {}

    @staticmethod
    def frame_error_message(error_message, more_info, error_code):
        js_result = {"errorCode": error_code, "errorMessage": error_message, "errorMoreInfo": more_info}
        return {"statusCode": error_code, "response": js_result}

    @staticmethod
    def frame_success_message(data):
        return {"statusCode": constant.HTTP_SUCCESS_CODE_200, "response": data}

    @staticmethod
    def get_request_header(method):
        headers = {}

        if method == 'OPTIONS':
            headers['Access-Control-Allow-Origin'] = '*'
            headers['Access-Control-Allow-Methods'] = 'GET'
            headers['Access-Control-Allow-Headers'] = 'Content-Type'
            headers['Access-Control-Max-Age'] = '3600'  # Cache for max 1 hour = 3600 seconds...
        else:
            headers['Access-Control-Allow-Origin'] = '*'

        return headers

    @staticmethod
    def get_request_header_token(method):
        headers = {}

        if method == 'OPTIONS':
            headers['Access-Control-Allow-Origin'] = '*'
            headers['Access-Control-Allow-Methods'] = 'GET'
            headers['Access-Control-Allow-Headers'] = ['Content-Type', 'Authorization']
            headers['Access-Control-Max-Age'] = '3600'  # Cache for max 1 hour = 3600 seconds...
        else:
            headers['Access-Control-Allow-Origin'] = '*'

        return headers    

    @staticmethod
    def get_environment_variable(token):
        return os.environ.get(token)

    @staticmethod
    def get_temporary_file_table_name_with_user(prefix, user):
        now = datetime.now(bst_tz)
        date_time = now.strftime("%d%m%Y_%H_%M_%S")
        return prefix + user + '_' + date_time

    @staticmethod
    def get_temporary_file_table_name_with_user_itemno(prefix, user,itemno):
        now = datetime.now(bst_tz)
        date_time = now.strftime("%d%m%Y_%H_%M_%S")
        return prefix +itemno+'_'+ user + '_' + date_time    

    @staticmethod
    def get_temporary_file_table_name(prefix):
        return prefix + uuid.uuid4().hex

    @staticmethod
    def get_sort_details(sort_by, sort_direction):
        sort_detail = sort_by + " " + sort_direction
        return sort_detail  

    @staticmethod
    def get_chart_date(graph_result):
        t = graph_result[0]['date']
        t1 = graph_result[-1]['date']
        graph_result[-1]['previousForecast'] = None
        count = graph_result[0]['full_count']
        t=datetime.strptime(t,DATE_FORMAT_DD_MM_YYYY)
        t1=datetime.strptime(t1,DATE_FORMAT_DD_MM_YYYY)
        t = t.date() - timedelta(days=1)
        t1= t1.date() + timedelta(days=1)
        t=datetime.strptime(str(t),DATE_FORMAT_YYYY_MM_DD).strftime(DATE_FORMAT_DD_MM_YYYY)
        t1=datetime.strptime(str(t1),DATE_FORMAT_YYYY_MM_DD).strftime(DATE_FORMAT_DD_MM_YYYY)
        temp = {'latestForecast': None, 'previousForecast': None, 'date': t, 'full_count': count}
        temp1 = {'latestForecast': None, 'previousForecast': None, 'date': t1, 'full_count': count}
        graph_result.insert(0,temp)
        graph_result.append(temp1)
        return graph_result

    @staticmethod
    def get_weekly_details(startdate, pre_start, result,default_dict, avg=True):
        try:
            weeks = 52
            final_data = []
            print("startdate",startdate)
            print("enddate",pre_start)
            dt1 = datetime.strptime(startdate, constant.DEFAULT_DATE_FORMAT)
            dt2 = datetime.strptime(pre_start, constant.DEFAULT_DATE_FORMAT)
            for i in range(0, weeks):
                print('week - ', i)
                final_data.append({'currentDate': dt1.strftime(DATE_FORMAT_YYYY_MM_DD),'previousDate': dt2.strftime(DATE_FORMAT_YYYY_MM_DD), **default_dict})
                #final_data.append({'previousDate': dt2.strftime(constant.DEFAULT_DATE_FORMAT)})
                dt1 = dt1 + timedelta(days=7)
                dt2 = dt2 + timedelta(days=7)
            print("DATES-",final_data)     

            start_date = datetime.strptime(startdate, constant.DEFAULT_DATE_FORMAT)
            print("zero",start_date)
            
            for item in result:
                
                r_date1 = datetime.strptime(item['currentDate'], constant.DEFAULT_DATE_FORMAT)
                idx1 = abs((start_date - r_date1).days) // 7
                for key in item:
                    
                    if not str(key).upper().__contains__('DATE') and item[
                        key] is not None:
                        
                        value = 0 if final_data[idx1][key] is None else final_data[idx1][key]
                        final_data[idx1][key] = value + item[key]
            print("weekly_data",final_data)           
            return final_data
        except Exception as e:
            logging.error('Exception in get_weekly_details - {}'.format(e))
            return {constant.MESSAGE: constant.ERROR_INTERNAL_SERVER}     
    

               



