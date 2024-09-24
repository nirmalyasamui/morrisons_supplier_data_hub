# Author : PURNENDU DAS

#from google.cloud import storage
from google.cloud import bigquery
from nltk.stem import PorterStemmer


def mismatch_query_generator(request):
    
    table_A_name=None
    table_B_name=None

    mismatch_query=""
    porter = PorterStemmer()

    bclient = bigquery.Client() # Bigquery Client
    #sclient = storage.Client()  # Storage Client

    project = bclient.project

    request_json = request.get_json()
    if request_json and 'table_a' in request_json:
        table_A_name = request_json['table_a'].strip()
        if(len(table_A_name)>1 and table_A_name[0]=="`"):
            table_A_name=table_A_name[1:]
        if(len(table_A_name)>1 and table_A_name[-1]=="`"):
            table_A_name=table_A_name[:-1]

    if request_json and 'table_b' in request_json:
        table_B_name = request_json['table_b'].strip()
        if(len(table_B_name)>1 and table_B_name[0]=="`"):
            table_B_name=table_B_name[1:]
        if(len(table_B_name)>1 and table_B_name[-1]=="`"):
            table_B_name=table_B_name[:-1]
        

    if(table_A_name is None or table_B_name is None):
        return "Table Names are not provided correctly !"

    if(len(table_A_name.split("."))!=3 or len(table_B_name.split("."))!=3 ):
        return "input Table name format error !\nIt should be in project_name.dataset_name.table_name format"

    else:
        invalid_table_A=False
        invalid_table_B=False

        table_A_columns =[]
        table_B_columns =[]

        is_partitioned_A=False
        partition_column_A=""
        is_partitioned_B=False
        partition_column_B=""


        try:
            project=table_name_a=table_A_name.split(".")[0]
            dataset_name_a=table_A_name.split(".")[-2]
            table_name_a=table_A_name.split(".")[-1]

            query_A="SELECT column_name, is_partitioning_column FROM `{}.{}`.INFORMATION_SCHEMA.COLUMNS\
            where table_name='{}';".format(project,dataset_name_a,table_name_a)
        
            query_job1 = bclient.query(query_A)
            results_a = query_job1.result()
            for row in results_a:
                if(row.column_name):
                    table_A_columns.append(row.column_name)
                if(row.is_partitioning_column.lower()=="yes"):
                    is_partitioned_A=True
                    partition_column_A=row.column_name

            if(len(table_A_columns)<1):
                invalid_table_A=True
        except:
            invalid_table_A=True

        
            
        try:
            dataset_name_b=table_B_name.split(".")[-2]
            table_name_b=table_B_name.split(".")[-1]

            query_B="SELECT column_name, is_partitioning_column FROM `{}.{}`.INFORMATION_SCHEMA.COLUMNS\
            where table_name='{}';".format(project,dataset_name_b,table_name_b)

            query_job2 = bclient.query(query_B)
            results_b = query_job2.result()
            for row in results_b:
                if(row.column_name):
                    table_B_columns.append(row.column_name)
                if(row.is_partitioning_column.lower()=="yes"):
                    is_partitioned_B=True
                    partition_column_B=row.column_name
                    
            if(len(table_B_columns)<1):
                invalid_table_B=True

        except:
            invalid_table_B=True



        if(invalid_table_A or invalid_table_B):
            error_message=""
            if(invalid_table_A):
                error_message+= "Inavild Table name A - {}\n".format(table_A_name)
            if(invalid_table_B):
                error_message+= "Inavild Table name B - {}".format(table_B_name)
            return error_message







        #----------------------------------- column name mapping part -----------------

        table_A_columns=[i.lower() for i in table_A_columns]
        table_B_columns=[i.lower() for i in table_B_columns]

        table_A_columns_stems=[]
        table_B_columns_stems=[]

        for column_name in table_A_columns:
            column_name_stem_name=[]
            for word in column_name.split("_"):
                column_name_stem_name.append(porter.stem(word).lower())
            table_A_columns_stems.append('_'.join(column_name_stem_name))

        for column_name in table_B_columns:
            column_name_stem_name=[]
            for word in column_name.split("_"):
                column_name_stem_name.append(porter.stem(word).lower())
            table_B_columns_stems.append('_'.join(column_name_stem_name))

        mapping_A=dict()    # Column mapping with Table A with Table B

        sorted_column_B=[]
        for i in range(len(table_A_columns_stems)):
            if(table_A_columns_stems[i] in table_B_columns_stems):
                indx=table_B_columns_stems.index(table_A_columns_stems[i])
                mapping_A[table_A_columns[i]]=table_B_columns[indx]
                sorted_column_B.append(table_B_columns[indx])
            else:
                mapping_A[table_A_columns[i]]=None

        mapping_B=dict()    # Column mapping with Table B with Table A


        for i in range(len(table_B_columns_stems)):
            if(table_B_columns_stems[i] in table_A_columns_stems):
                indx=table_A_columns_stems.index(table_B_columns_stems[i])
                mapping_B[table_B_columns[i]]=table_A_columns[indx]

            else:
                mapping_B[table_B_columns[i]]=None

        clean_cloumns_A=[i.lower() for i in table_A_columns if mapping_A[i] ]
        clean_cloumns_B=[i.lower() for i in table_B_columns if mapping_B[i] ]


        table_A_name="`"+table_A_name+"`"
        table_B_name="`"+table_B_name+"`"



        if(len(sorted_column_B)==0):
            return "Joining is not possible, No column name is common between both tables !"



        #------------------------------- Mismatch query Code -----------------------------


        mismatch_query="SELECT distinct A.* FROM ( \nSELECT "
        mismatch_query+="\n'SOURCE-TEST' mismatch,"


        # Table A
        for c1 in clean_cloumns_A:
            if((not c1.endswith("_dt")) and ("date" not in c1) and ("time" not in c1)):
                mismatch_query+="\nCAST(bq."+c1+" as STRING) "+c1+","
            else:
                 mismatch_query+="\nCAST(date(bq."+c1+") as STRING) "+c1+","

        if(is_partitioned_A):
            mismatch_query+="\nFROM (select DISTINCT * from "+table_A_name+" bq where date("+partition_column_A+")=CURRENT_DATE() ) bq"
        else:
            mismatch_query+="\nFROM (select DISTINCT * from "+table_A_name+") bq"

        if(is_partitioned_B):
            mismatch_query+="\nLEFT JOIN (select DISTINCT * from "+table_B_name+" aut where date("+partition_column_B+")=CURRENT_DATE() ) aut ON"
        else:
            mismatch_query+="\nLEFT JOIN (select DISTINCT * from "+table_B_name+") aut ON"


        counter=0
        for c1 in clean_cloumns_A:
            if((not c1.endswith("_dt")) and ("date" not in c1) and ("time" not in c1)):
                mismatch_query+="\nIFNULL(TRIM(CAST(bq."+c1+" as STRING)),'') = IFNULL(TRIM(CAST(aut."+mapping_A[c1]+" as STRING)),'') "
            else:
                mismatch_query+="\nIFNULL(TRIM(CAST(date(bq."+c1+") as STRING)),'') = IFNULL(TRIM(CAST(date(aut."+mapping_A[c1]+") as STRING)),'') "

            counter+=1
            if(counter<len(clean_cloumns_A)):
                mismatch_query+="\nAND"


        mismatch_query+="\nWhere "

        counter=0
        for nc1 in table_B_columns:
            mismatch_query+="\naut."+nc1+" is null"

            counter+=1
            if(counter<len(table_B_columns)):
                mismatch_query+="\nAND"

        mismatch_query+="\nUNION ALL\nSELECT"
        mismatch_query+="\n'TEST-SOURCE' mismatch,"






        # Table B

        for c2 in sorted_column_B:
            if((not c2.endswith("_dt")) and ("date" not in c2) and ("time" not in c2)):
                mismatch_query+="\nCAST(aut."+c2+" as STRING) "+c2+","
            else:
                 mismatch_query+="\nCAST(date(aut."+c2+") as STRING) "+c2+","

        


        if(is_partitioned_B):
            mismatch_query+="\nFROM (select DISTINCT * from "+table_B_name+" aut where date("+partition_column_B+")=CURRENT_DATE() ) aut"
        else:
            mismatch_query+="\nFROM (select DISTINCT * from "+table_B_name+") aut"

        if(is_partitioned_A):
            mismatch_query+="\nLEFT JOIN (select DISTINCT * from "+table_A_name+" bq where date("+partition_column_A+")=CURRENT_DATE() ) bq ON"
        else:
            mismatch_query+="\nLEFT JOIN (select DISTINCT * from "+table_A_name+") bq ON"


        counter=0
        for c2 in clean_cloumns_B:
            if((not c2.endswith("_dt")) and ("date" not in c2) and ("time" not in c2)):
                mismatch_query+="\nIFNULL(TRIM(CAST(aut."+c2+" as STRING)),'') = IFNULL(TRIM(CAST(bq."+mapping_B[c2]+" as STRING)),'') "
            else:
                mismatch_query+="\nIFNULL(TRIM(CAST(date(aut."+c2+") as STRING)),'') = IFNULL(TRIM(CAST(date(bq."+mapping_B[c2]+") as STRING)),'') "

            counter+=1
            if(counter<len(clean_cloumns_B)):
                mismatch_query+="\nAND"


        mismatch_query+="\nWhere "

        counter=0
        for nc2 in table_A_columns:
            mismatch_query+="\nbq."+nc2+" is null"

            counter+=1
            if(counter<len(table_A_columns)):
                mismatch_query+="\nAND"
        mismatch_query+="\n)A;"






        return mismatch_query