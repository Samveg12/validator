from decimal import Decimal
from random import choices
from ssl import ALERT_DESCRIPTION_ACCESS_DENIED
from sys import prefix
from django.forms import modelform_factory, modelformset_factory
from django.shortcuts import render,redirect
from django.http import HttpResponse
from django.forms import formset_factory
from functools import partial, wraps
import os
import shutil

from filecmp import cmp
import pandas as pd
import snowflake.connector
# import config
from django.contrib import messages

from .forms import Credentials,Filter,KPIs,Level,Uploaded
num_level=0
num_kpis=0
num_filter=0
username=""
schemaa=""
databasee=""
passwordd=""
final=[]
table=""
table_name=""
dicti={}

def append_single_quotes(input):
    if (input[0] != "'" and input[-1] != "'"):
        input = "'" + input + "'"
    elif input[0] == "'" and input[-1] != "'":
        input = input + "'"
    elif input[0] != "'" and input[-1] == "'":
        input = "'" + input
    return input

def append_double_quotes(input):
    if ' ' in input and (input[0] != '"' and input[-1] != '"'):
        input = '"' + input + '"'
    elif input[0] == '"' and input[-1] != '"':
        input = input + '"'
    elif input[0] != '"' and input[-1] == '"':
        input = '"' + input
    
    return input

def index(request):
    global num_level
    global schemaa
    global username
    global databasee
    global passwordd
    global table

    global table_name
    global num_kpis
    global num_filter
    global final
    global dicti
    Credentialsset=formset_factory(Credentials,extra=1)
    if request.method ==  "POST":
        # form = Credentials(request.POST)
        formi = Credentialsset(request.POST)
        # print(form)
        if formi.is_valid():
            for f in formi: 
                cd = f.cleaned_data
                username = cd.get('username')
                passwordd = cd.get('password')
                schemaa = cd.get('schema')
                databasee=cd.get('database')
                table = cd.get('table')
                num_level=cd.get('num_level')
                num_filter=cd.get('num_filter')
                num_kpis=cd.get('num_kpis')
            table_name=table
            print(passwordd)
            print(schemaa)
            print(databasee)
            print(table)
            print(num_level)
            print(num_kpis)
            print(num_filter)

            try:
                con = snowflake.connector.connect(
                    user=username,
                    account = "colgatepalmolivedev.us-central1.gcp",
                    authenticator = "externalbrowser",
                    # password= passwordd,
                    warehouse = "DEVELOPER_WH",
                    database= databasee,
                    schema= schemaa,
                )
                print("Connecting")
                cur = con.cursor()
                print(cur)
                # STEP 2: Displaying Preview of Table

                preview_query = "SELECT * FROM " + table + " LIMIT 10 "
                print(preview_query)
                cur.execute(preview_query)
                columns = []
                for val in cur.description:
                    columns.append(val[0])
                rows = 0
                while True:
                    print("Inside while")
                    dat = cur.fetchall()
                    if not dat:
                        break
                    # print(cur.description)
                    df = pd.DataFrame(dat, columns=columns)
                    result=df.columns.values.tolist()
                    rows += df.shape[0]
            except:
                print("error")
                messages.info(request,"Wrong credentails")
                return redirect('/Test')


            # print(result)
            s=[]
            for i in range(0,len(result)):
                s.append(result[i])
            s=sorted(s)
            final=[]
            dicti={}
            for i in range(1,len(s)+1):
                z=[]
                z.append(str(i))
                z.append(str(s[i-1]))
                z=tuple(z)
                dicti[str(i)]=str(s[i-1])
                final.append(z)
            final.sort(key = lambda x: x[1])
            final=tuple(final)
            df.to_csv('SF_PREVIEW.csv', index=False)

            print("PREVIEW OF TABLE SAVED IN SF_PREVIEW.csv")
            # mail(Name,email,Country)
            return redirect('detail')
        return redirect('/Test')
    
    return render(request,"Test/index.html",{"form":Credentialsset})

def detail(request):
    global num_level
    global schemaa
    global username
    global databasee
    global passwordd
    global table
    global table_name
    global num_kpis
    global num_filter
    global final
    global dicti

    Filterset=formset_factory(wraps(Filter)(partial(Filter, choices=final)), extra=num_filter)

    Levelset=formset_factory(wraps(Level)(partial(Level, choices=final)), extra=num_level)
    KPIset=formset_factory(wraps(KPIs)(partial(KPIs, choices=final)), extra=num_kpis)
    Uploading=Uploaded()

    if request.method ==  "POST":
        formi1 = Filterset(request.POST,request.FILES,prefix=str(1))
        formi2 = Levelset(request.POST,prefix=str(2))
        formi3 = KPIset(request.POST,prefix=str(3))
        formi4=Uploaded(request.POST, request.FILES)
        print("hehehheheeh")
        print(len(formi1))
        print(len(formi2))
        print(len(formi3))
        # cd=formi3.cleaned_data
        # print(cd)
        # print(num_filter)
        # print(num_kpis)
        # print(num_level)
        if formi4.is_valid():
            filehandle = request.FILES['file']
            sap_data=pd.read_excel(filehandle, engine='openpyxl', sheet_name='Sheet1')
        # print(sap_data)
        # print(sap_data, sep='|',encoding='latin-1')
            # do pandas here to filehandle/ put filehandle into a function 
            
        levels=[]
        kpi_name=[]
        kpi_aggregation=[]
        filter_param=[]
        filter_cri=[]
        filter_val=[]
        fil_dic={1:'>',2:'<',3:"like",4:"between",5:"=",6:">=",7:'<='}
        agr_dic={1:'SUM',2:'COUNT',3:"AVG",4:"MIN",5:"MAX"}
        if formi1.is_valid() and formi2.is_valid() and formi3.is_valid():
            
            print(len(formi2))
            for i in range(0,len(formi1)):
                cd=formi1[i].cleaned_data
                # print("====")
                # print(type(cd.get('filter_parameter')))
                # print(cd.get('filter_parameter'))
                filter_param.append(dicti[append_double_quotes(str(cd.get('filter_parameter')))])
                filter_cri.append(fil_dic[int(cd.get('filter_criteria'))])
                filter_val.append(append_single_quotes(cd.get('filter_value')))
            print("Yooo")
            print(filter_cri)
            print(filter_val)
            print(filter_param)
            for f in range (0,len(formi2)):
                cd=formi2[f].cleaned_data
                levels.append(dicti[str(cd.get('levels'))])
            print("----clean----")
            print(len(formi1))
            print(len(formi2))
            print(len(formi3))
            # cdd=formi3.cleaned_data
            # cddd=formi2.cleaned_data
            # cdddd=formi1.cleaned_data
            # print(cdddd)
            # print(cddd)
            # print(cdd)
            # print("-----------------")
            for f in range(0,len(formi3)):
                cd=formi3[f].cleaned_data
                kpi_name.append(dicti[str(cd.get('kpi'))])
                kpi_aggregation.append(agr_dic[int(cd.get('aggregation'))])
            
            sql_query="SELECT "
            print(levels)
            sql_query += '"'+levels[0]+'"'
            for i in range(1,len(levels)):
                sql_query += "," + '"'+levels[i]+'"'
            for i in range(0,len(kpi_name)):
                sql_query += "," + kpi_aggregation[i] + "(" + '"' +kpi_name[i] + '"' +")"
            sql_query += " FROM " + table_name
            sql_query += " WHERE " + '"' +filter_param[0] + '"' + " " + filter_cri[0] + " " + filter_val[0]
            for i in range(1, len(filter_param)):
                sql_query += " AND " + '"' +filter_param[i] + '"' + " " + filter_cri[i] + " " + filter_val[i]
            v=""
            for i in range(1,len(levels)+1):
                v=v+str(i)+','
            v=v[:len(v)-1]

            sql_query += " GROUP BY {0} ORDER BY 1".format(v)
            try:
                con = snowflake.connector.connect(
                    user=username,
                    account = "colgatepalmolivedev.us-central1.gcp",
                    authenticator = "externalbrowser",
                    password= passwordd,
                    warehouse = "DEVELOPER_WH",
                    database= databasee,
                    schema= schemaa,
                )
                cur = con.cursor()
                df = pd.DataFrame
                cur.execute(sql_query)
                # print("QUERY EXECUTED")
                columns = []
                for val in cur.description:
                    columns.append(val[0])
                # print(columns)
                rows = 0
                while True:
                    # print("Inside while")
                    dat = cur.fetchall()
                    if not dat:
                        break
                    # print(cur.description)
                    df = pd.DataFrame(dat, columns=columns)
                    rows += df.shape[0]
            finally:
                cur.close()
                print(sql_query)
            sf_data=df
            # print("=====")
            # print(sf_data)
            cols = []
            for col in sap_data:
                cols.append(col)
            sf_data.columns = cols

            print('Column names in SF data')
            i = num_level
            for col in sf_data:
                if(i>0):
                    sf_data[col] = sf_data[col].str.lstrip('0')
                    i = i-1
                print(col)

            print("Column names in SAP data")
            for col in sap_data:
                print(col)

            # cmp_level = comparison_level[0].replace('"', '')
            cmp_level=[]
            for i in range(0,len(levels)):
                cmp_level.append(levels[i].replace('"', ''))
            kpi_list = kpi_name
            new_kpi = []
            # for val in kpi_list:
            #     val = val.replace('"', '')
            #     new_kpi.append(val)
            for val in sap_data:
                val = val.replace('"', '')
                new_kpi.append(val)
            # delta = num_kpis- num_level
            for i in range(0,num_level):
                new_kpi.pop(0)

            # print(new_kpi)

            # print(cmp_level)
            # print(kpi)

            #samveg
            s=num_kpis
            v=num_level
            m=cmp_level
            kpi=new_kpi
            print("=====")
            # print(s)
            # print(v)
            # print(m)
            # print(kpi)

            dicti_sap={}
            # print(sap_data.iloc[0,0])
            final_dicti={}
            t=[]
            orderi=[]
            for i in range(0,len(sap_data)):
                p=""
                s=[]
                for j in range(0,v):
                    p=p+str(sap_data.iloc[i,j])+"_"
                    s.append(sap_data.iloc[i,j])
                orderi.append(p)

                t.append(s)
                dicti_sap[p]=i
                final_dicti[p]=1
            # print(t)
            dicti_sf={}
            for i in range(0,len(sf_data)):
                p=""
                s=[]
                for j in range(0,v):
                    s.append(sf_data.iloc[i,j])
                    p=p+str(sf_data.iloc[i,j])+"_"
                dicti_sf[p]=i
                if(p in dicti_sap.keys()):
                    continue
                else:
                    orderi.append(p)
                    t.append(s)
            # print(t)
            print("=====")
            # print(orderi)
            print("JJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJ")
            # print(dicti_sap.keys())
            # print(dicti_sf.keys())
            print("WWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWW")
            # print(orderi)
            print(kpi)
            # print(dicti_sap)
            # not_found=[]
            # for i in range(0,s):
            df_output=pd.DataFrame(t,columns=m)
            # print(df_output)
            for i in range(0,len(kpi)):
                kpi1=[]
                kpi2=[]
                kpi_diff=[]
                vv=[]
                for j in range(0,len(orderi)):
                    if(orderi[j] in dicti_sap.keys()):
                        # print("=====")
                        # print(sap_data.at[dicti_sap[orderi[j]],kpi[i]])
                        kpi1.append(sap_data.at[dicti_sap[orderi[j]],kpi[i]])
                    else:
                        # print("Ypppp")
                        kpi1.append(0)
                    if(orderi[j] in dicti_sf.keys()):
                        kpi2.append(sf_data.at[dicti_sf[orderi[j]],kpi[i]])
                    else:
                        kpi2.append(0)
                    kpi_diff.append(abs(float(kpi1[-1])-float(kpi2[-1])))
                # vv.append(kpi1)
                # vv.append(kpi2)
                # print("Yo")
                # print(kpi1)
                # print(kpi2)
                # print(kpi[i])
                # df_output.insert()
                df_output[kpi[i]+str("_sap")]=kpi1
                df_output[kpi[i]+str("_sf")]=kpi2
                df_output["differ"+"_{0}".format(kpi[i])]=kpi_diff

            df_output.to_excel("result.xlsx", index=False)
            src_path = os.path.join(os.path.dirname(os.path.realpath(__name__)), 'result.xlsx')
            dst_path = os.path.join(os.path.dirname(os.path.realpath(__name__)), 'media/')
            shutil.copy(src_path, dst_path)
            print("Result saved as result.xlsx")   
        print(os.path.dirname('result.xlsx'))
        
        return render(request, "Test/download.html")
        # return redirect("download")
        return redirect("download")
        # return    
        # return(HttpResponse("Success"))   
    else:
        for1=Filterset(prefix=str(1))
        for2=Levelset(prefix=str(2))
        for3=KPIset(prefix=str(3))
        return render(request,"Test/detail.html",{"form1":for1,"form2":for2,'form3':for3,'form4':Uploaded})
    # if request.method ==  "POST":
    #     form = Credentials(request.POST)
    #     print(form)
    #     if form.is_valid():
    #         print("hey")
            
    # else:
    #     redentialsset=formset_factory(Credentials,extra=2)

    #     return render(request,"Test/detail.html")

def download_file(request):
    if(request.GET.get('downbtn')):
        file_path = os.path.join(os.path.dirname(os.path.realpath(__name__)), 'result.xlsx')
        if os.path.exists(file_path):
            with open(file_path, "rb") as excel:
                data = excel.read()

        response = HttpResponse(data,content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename=result.xlsx'
        # render(request, "Test/download.html")
    return response

def download(request):

    print("INSIDE DOWNLOAD")
    # return download_file()
    return render(request, "Test/download.html")


