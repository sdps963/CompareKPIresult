import csv
import re
import os
import pymssql
from termcolor import colored

from decimal import Decimal

def read_csv(csv_file, **kwargs):
    print(csv_file)
    with open(csv_file,newline='') as  f:
        reader = csv.DictReader(f)
        kpi_dict = {}
        row_count = 0
        for row in reader:

            #print(row[kwargs['col_name_1']],row[kwargs['col_name_2']])
            regex = r"SQL"
            value = []
            if re.search(regex,csv_file):
                kpi_name = row[kwargs['col_name_1']]
                my_sql_stmt = row[kwargs['col_name_2']]
                date_val    = "2018-01-09"
                date_string = "\'" + date_val + " 00:00.000\' and \'" + date_val + " 11:59:59.998\'"
                my_new_sql_stmt = re.sub('\(\'\@startdate\@\'\) and \(\'\@enddate\@\'\)',date_string,my_sql_stmt)
                #my_new_sql_stmt = re.sub('\'\@startdate\@\' and \'\@enddate\@\'',date_string,my_sql_stmt)
                print(my_new_sql_stmt)
                print(my_new_sql_stmt)
                #words = my_sql_stmt.split("'@")
                #my_new_sql_stmt = words[0] + "'2018-07-30 00:00.000' and '2018-07-30 23:59:59.998'" + ") a"
                key = kpi_name
                #value[0] = my_new_sql_stmt
                value.append(my_new_sql_stmt)
            else:
                kpi_name    = row[kwargs['col_name_1']]
                kpi_value   = row[kwargs['col_name_2']]
                unit_type   = row[kwargs['col_name_3']]
                concat     = row[kwargs['col_name_4']]
                aggr_freq  = row[kwargs['col_name_5']]
                origin     = row[kwargs['col_name_6']]
                if (unit_type != "USD_Imp"):
                    continue
                print("KPI NAME=\n{}\n KPI VALUE=\n{}\n UNIT_TYPE=\n{}\n".format(kpi_name,kpi_value,unit_type))
                key = kpi_name
                value.append(kpi_value)
                value.append(concat)
                value.append(aggr_freq)
                value.append(origin)

            kpi_dict[key] = value
            row_count += 1
            print("row count = {}".format(row_count))

    return kpi_dict

def conn_to_sql_server(server_name,db,user,password):

    conn  = pymssql.connect(server_name,user,password,db)
    return conn

def RemoveNull(x):
    if x == None:
        return(0)
    elif x == '':
        return(0)
    else:
        return (x)


def compare(first,second,f):
    sharedKeys = set(first.keys()).intersection(second.keys())
    print("SHARED KEYS = {}".format(sharedKeys))
    sorted(sharedKeys)
    print("SORTED SHARED KEYS = {}".format(sharedKeys))
    kpi_count = 0
    test_pass_count = 0
    test_fail_count = 0
    total_test_count = 0
    print("KPI_Name\t\t\t\tKPI_Count\t\tKPI_Value_MSSQL\t\tKPI_Engine_Results_Value\t\t\t\t\t\t\t\t\t\t\tConcatenated\t\t\tAggregate Frequency\t\t\t\t\tOrigin\t\t\tTest Status",file=f)
    print("KPI_Name\t\t\t\tKPI_Count\t\tKPI_Value_MSSQL\t\tKPI_Engine_Results_Value\t\t\t\t\t\t\t\t\t\t\tConcatenated\t\t\tAggregate_Frequency\t\t\t\t\tOrigin\t\t\tTest Status")
    for key in sharedKeys:
        kpi_count += 1
        first_out = float(RemoveNull(first[key]))
        second_out = float(RemoveNull(second[key][0]))

        first_out = round(first_out,4)
        second_out = round(second_out,4)

        tp = "TEST_PASS"
        tf = "TEST_FAIL"

        if first_out != second_out:

            print("{}\t{:>16}\t{:>24}\t{:>24}\t{:>64}\t{:>16}\t{:>24}\t{:>16}".format(
                    key.strip(),
                    kpi_count,
                    first_out,
                    second_out,
                    second[key][1].strip(),
                    second[key][2].strip(),
                    second[key][3].strip(),
                    tf),file=f)
            print("{}\t{:>25}\t{:>33}\t{:>33}\t{:>73}\t{:>25}\t{:>33}\t{:>25}".format(
                colored(key.strip(),                'red'),
                colored(kpi_count,                  'red'),
                colored(first_out,                  'red'),
                colored(second_out,                 'red'),
                colored(second[key][1].strip(),     'red'),
                colored(second[key][2].strip(),     'red'),
                colored(second[key][3].strip(),     'red'),
                colored(tf,                         'red')))
            test_fail_count += 1
        else:

            print("{}\t{:>16}\t{:>24}\t{:>24}\t{:>64}\t{:>16}\t{:>24}\t{:>16}".format(
                key.strip(),
                kpi_count,
                first_out,
                second_out,
                second[key][1].strip(),
                second[key][2].strip(),
                second[key][3].strip(),
                tp),file=f)
            print("{}\t{:>16}\t{:>24}\t{:>24}\t{:>64}\t{:>16}\t{:>24}\t{:>16}".format(
                key.strip(),
                kpi_count,
                first_out,
                second_out,
                second[key][1].strip(),
                second[key][2].strip(),
                second[key][3].strip(),
                tp,))
            test_pass_count += 1
        #if first[key] != second[key]:
        #    print("Key = {}, Value 1: {}, Value 2: {} TEST FAIL".format(key,first[key],second[key]))
        #else:
        #    print("Key = {}, Value 1: {}, Value 2: {} TEST PASS".format(key,first[key],second[key]))
        total_test_count += 1


    test_pass_percent = (( test_pass_count / total_test_count ) * 100 )
    test_pass_percent = round(test_pass_percent,2)
    test_fail_percent = (( test_fail_count / total_test_count ) * 100 )
    test_fail_percent = round(test_fail_percent,2)

    print("\n\n\n",file=f)
    print("\n\n\n")
    print("Total Number of KPIs = {}".format((kpi_count)),file=f)
    print("Total Number of KPIs = {}".format((kpi_count)))
    print("Total Test Count = {}".format(total_test_count),file=f)
    print("Total Test Count = {}".format(total_test_count))
    print("Test Pass Count = {}".format(test_pass_count),file=f)
    print("Test Pass Count = {}".format(test_pass_count))
    print("Test Fail Count = {}".format(test_fail_count),file=f)
    print("Test Fail Count = {}".format(test_fail_count))
    print("Test Pass Percent = {}% ".format(test_pass_percent),file=f)
    print("Test Pass Percent = {}% ".format(test_pass_percent))
    print("Test Fail Percent = {}%".format(test_fail_percent),file=f)
    print("Test Fail Percent = {}%".format(test_fail_percent))

def main():
    #server_name = "usacorjv1.goldbar.barrick.com"
    #database_name = "jmineops"
    user_name = "njaitly"
    password = "3nV)qB7z"

    #SIMA Data (Goldstrike Processing GS PR)
    server_name = "Usagstsql5.goldbar.barrick.com"
    database_name = "SIMA"

    #Jigsaw Data(Goldstrike Open Pit GS OP)
    #server_name = "Usagstsql5.goldbar.barrick.com"
    #database_name = "JMineOPS"

    #Jigsaw Data(Cortez Open Pit CZ OP)
    #server_name   =  "usacorjv1.goldbar.barrick.com"
    #database_name =  "jmineops"

    conn = conn_to_sql_server(server_name,database_name,user_name,password)
    cursor = conn.cursor()

    #actual_sql_file  = "./task1/BPR_CZ_OP_Actuals_SQL_Queries.csv"
    #actual_sql_file  = "./task1/BPR_GS_OP_Q&H_Final_SQL_Queries.csv"
    #actual_sql_file  = "./task1/BPR_GS_OP_JIGSAW_Actuals_SQL_Queries.csv"
    #actual_sql_file  = "./task1/BPR_GS_OP_JIGSAW_Actuals_SQL_Queries.csv"
    ## July 31st 20118
    #actual_sql_file = "./data/20180731/src/BPR_GS_OP_Actuals_Final_SQL_Queries.csv"
    #actual_sql_file = "./data/20180731/src/BPR_GS_OP_Actuals_Final_SQL_Queries.csv"
    #actual_sql_file = "./data/20180731/src/BPR_CZ_OP_Acutals_Final_SQL_Queries.csv"
    #actual_sql_file = "./data/20180731/src/BPR_CZ_OP_QH_Final_SQL_Queries.csv"
    #actual_sql_file = "./data/20180731/src/BPR_CZ_OP_QH_Final_SQL_Queries.csv"
    #actual_sql_file = "./data/20180731/src/BPR_GS_OP_QH_Final_SQL_Queries.csv"
    #actual_sql_file = "./data/20180731/src/BPR_GS_OP_QH_Final_SQL_Queries.csv"
    #actual_sql_file =  "./data/20180731/src/BPR_GS_OP_Actuals_Final_SQL_Queries.csv"
    #actual_sql_file =  "./data/20180731/prod/newfiles/BPR_GS_OP_Actuals_Final_SQL_Queries.csv"
    #actual_sql_file =  "./data/20180731/prod/newfiles/BPR_GS_OP_QH_Final_SQL_Queries.csv"
    #actual_sql_file =  "./data/20180801/src/BPR_GS_PR_SIMA_SQL_Queries.csv"

    #actual_sql_file  =  "./data/20180803/prod/src/BPR_CZ_OP_Acutals_Final_SQL_Queries.csv"
    #actual_sql_file  =  "./data/20180803/prod/src/BPR_GS_OP_Actuals_Final_SQL_Queries.csv"
    #actual_sql_file  =  "./data/20180803/prod/src/BPR_GS_PR_SIMA_SQL_Queries.csv"
    #actual_sql_file  =  "./data/20180803/non-prod/src/BPR_GS_PR_SIMA_SQL_Queries.csv"
    actual_sql_file  =  "./data/20180808/non-prod/src/BPR_GS_PR_SIMA_SQL_Queries.csv"

    source_dict = read_csv(actual_sql_file,col_name_1="ID",col_name_2="Mapping")

    #kpi_results_file = "./task1/BPR_CZ_OP_Actuals_KPI_Results_2.csv"
    #kpi_results_file = "./task1/bpr_gs_op_qh_062418_KPI_Results.csv"
    #kpi_results_file = "./task1/bpr_gs_op_actuals_062018_KPI_Results.csv"
    #kpi_results_file = "./task1/bpr_gs_op_actuals_062018_KPI_Results.csv"
    ## July 31st 20118
    #kpi_results_file  = "./data/20180731/dest/gs_op_actuals_061818_KPI_RESULTS_bka.csv"
    #kpi_results_file  = "./data/20180731/dest/gs_op_actuals_061818_KPI_RESULTS_bka.csv"
    #kpi_results_file  = "./data/20180731/dest/gs_op_actuals_061818_KPI_RESULTS_bka.csv"
    #kpi_results_file  = "./data/20180731/dest/cz_op_qh_021818_KPI_RESULTS.csv"
    #kpi_results_file  = "./data/20180731/dest/cz_op_actuals_060518_KPI_RESULTS.csv"
    #kpi_results_file  = "./data/20180731/dest/cz_op_actuals_060518_KPI_RESULTS.csv"
    #kpi_results_file  = "./data/20180731/dest/gs_op_qh_060318_KPI_RESULTS.csv"
    #kpi_results_file  = "./data/20180731/prod/newfiles/CZ_OP_QH_060318.csv"
    #kpi_results_file  = "./data/20180731/prod/newfiles/GSOP_Actuals_060418.csv"
    #kpi_results_file  = "./data/20180731/prod/newfiles/GSOP_QH_041318.csv"
    #kpi_results_file  = "./data/20180731/prod/newfiles/GSPR_060118.csv"
    #kpi_results_file  = "./data/20180731/dest/gs_op_actuals_030618_KPI_RESULTS.csv"
    #kpi_results_file  = "./data/20180801/dest/gs_pr_071518_results.csv"

    #kpi_results_file   = "./data/20180803/prod/dest/cz_op_01012018_KPI_RESULTS.csv"
    #kpi_results_file   = "./data/20180803/prod/dest/cz_op_01252018_KPI_RESULTS.csv"
    #kpi_results_file   = "./data/20180803/prod/dest/cz_op_02282018_KPI_RESULTS.csv"
    #kpi_results_file   = "./data/20180803/prod/dest/cz_op_07152018_KPI_RESULTS.csv"
    #kpi_results_file   = "./data/20180803/prod/dest/cz_op_12312017_KPI_RESULTS.csv"
    #kpi_results_file   = "./data/20180803/prod/dest/gs_op_03022018_KPI_RESULTS.csv"
    #kpi_results_file   = "./data/20180803/prod/dest/gs_op_05242018_KPI_RESULTS.csv"
    #kpi_results_file   = "./data/20180803/prod/dest/gs_op_12142017_KPI_RESULTS.csv"
    #kpi_results_file   = "./data/20180803/prod/dest/gs_pr_04182018_KPI_RESULTS.csv"
    #kpi_results_file   = "./data/20180803/prod/dest/gs_pr_12072017_KPI_RESULTS.csv"
    #kpi_results_file   = "./data/20180803/non-prod/dest/gs_pr_NonProd_052118_KPI_RESULTS.csv"
    #kpi_results_file    = "./data/20180803/non-prod/dest/gs_pr_NonProd_0730218_KPI_RESULTS.csv"
    #kpi_results_file    = "./data/20180807/non-prod/dest/cz_op_actuals_02082018_KPI_RESULTS.csv"
    #kpi_results_file    = "./data/20180807/non-prod/dest/cz_op_actuals_03082018_KPI_RESULTS.csv"
    kpi_results_file    = "./data/20180808/non-prod/dest/gs_pr_01092018_KPI_RESULTS.csv"

    dest_dict = read_csv(kpi_results_file,col_name_1="KPIName",col_name_2="KPIValue",col_name_3="unit_type",
                         col_name_4="Concatenated",col_name_5="Aggregate Frequency",col_name_6="Origin")

    db_dict = {}
    for (k,v) in source_dict.items():
        print(k,v[0])
        print("VAAALLUUEEE = {}".format(v[0]))
        cursor.execute(v[0])
        row = cursor.fetchone()
        print("ROW = {}".format(row))
        if (row == "None"):
            continue
        elem1 = row[0]
        elem2 = row[1]
        print("ELEM 1 = {} ELEM 2 = {}".format(elem1,elem2))
        db_dict[elem1] = elem2

        #val1 = row[0]
        #val2 = row[1]
        #print("val 1 = {}  , val 2 = {}".format(val1,val2))

    for (k,v) in dest_dict.items():
        print(k,v)

    print("Printing Resulting Dict#####################################")
    for (k,v) in db_dict.items():
        print(k,v)

    sql_file = actual_sql_file.split('/')[5].split('.')[0]
    kpi_file = kpi_results_file.split('/')[5].split('.')[0]
    out_file = sql_file + "__Vs__" + kpi_file + ".out"
    out_file_sorted = sql_file + "__Vs__" + kpi_file + "sorted" + ".out"
    ofp = "./data/20180808/non-prod/out/"
    out_file_path = ofp + out_file
    out_file_path_sorted = ofp + out_file_sorted

    f = open(out_file_path,'w')
    print("\n",file = f)
    print("\n")
    print("KPI Comparison Results of {} and {} files ".format(actual_sql_file,kpi_results_file),file=f)
    print("KPI Comparison Results of {} and {} files ".format(actual_sql_file,kpi_results_file))
    print("---------------------------------------------------------------------------------------------------------------------------",file=f)
    print("---------------------------------------------------------------------------------------------------------------------------")
    print("\n\n",file=f)
    print("\n\n")

    compare(db_dict,dest_dict,f)
    f.close()

    bashcmd = "sort " + out_file_path + " > " + out_file_path_sorted
    os.system(bashcmd)

    conn.close()

if __name__ == "__main__":
    main()

'''
    self.cursor = self.conn.cursor()

    if __name__ == '__main__':
        s = MSSQL(server_gs, user_gs, password_gs, db_gs)
    print
    s.get_row_count("results")
    
row = cursor.fetchone()
while row:
print("ID=%d, Name=%s" % (row[0], row[1]))
row = cursor.fetchone()

conn.close()
'''
