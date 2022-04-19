from decimal import Decimal
from random import choices
from ssl import ALERT_DESCRIPTION_ACCESS_DENIED
from django.forms import modelform_factory, modelformset_factory
from django.shortcuts import render,redirect
from django.http import HttpResponse
from django.forms import formset_factory
from functools import partial, wraps

from filecmp import cmp
import pandas as pd
import snowflake.connector
# import config
from django.contrib import messages

from .forms import Credentials,Filter,KPIs,Level,Upload
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
                    password= passwordd,
                    warehouse = "DEVELOPER_WH",
                    database= databasee,
                    schema= schemaa,
                )
                cur = con.cursor()

                # STEP 2: Displaying Preview of Table

                preview_query = "SELECT * FROM " + table + " LIMIT 10 "
                # print(preview_query)
                cur.execute(preview_query)
                columns = []
                for val in cur.description:
                    columns.append(val[0])
                rows = 0
                while True:
                    # print("Inside while")
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
            final=sorted(final)
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
    print(final)
    # ServiceFormSet = formset_factory(wraps(ServiceForm)(partial(ServiceForm, affiliate=request.affiliate)), extra=3)

    Filterset=formset_factory(wraps(Filter)(partial(Filter, choices=final)), extra=num_filter)
    # ServiceFormSet = formset_factory(wraps(ServiceForm)(partial(ServiceForm, affiliate=request.affiliate)), extra=3)

    Levelset=formset_factory(wraps(Level)(partial(Level, choices=final)), extra=num_level)
    KPIset=formset_factory(wraps(KPIs)(partial(KPIs, choices=final)), extra=num_kpis)
    Uploading=Upload()
    # KPIset=formset_factory(KPIs(choices=final),extra=num_kpis)
    # Levelset=formset_factory(Level(choices=final),extra=num_filter)
    if request.method ==  "POST":
        formi1 = Filterset(request.POST,request.FILES)
        formi2 = Levelset(request.POST)
        formi3 = KPIset(request.POST)
        formi4=Upload(request.POST, request.FILES)
        if formi4.is_valid():
            filehandle = request.FILES['file']
            sap_data=pd.read_excel(filehandle)
        # print(sap_data)
        # print(sap_data, sep='|',encoding='latin-1')
            # do pandas here to filehandle/ put filehandle into a function 
            
        levels=[]
        kpi_name=[]
        kpi_aggregation=[]
        filter_param=[]
        filter_cri=[]
        filter_val=[]
        if formi1.is_valid() and formi2.is_valid() and formi3.is_valid():
            
            print(len(formi1))
            for i in range(0,len(formi1)-1):
                cd=formi1[i].cleaned_data
                print("====")
                print(type(cd.get('filter_parameter')))
                print(cd.get('filter_parameter'))
                filter_param.append(dicti[str(cd.get('filter_parameter'))])
                filter_cri.append(cd.get('filter_criteria'))
                filter_val.append(cd.get('filter_value'))
            for f in range (0,len(formi2)-1):
                cd=formi2[f].cleaned_data
                levels.append(dicti[str(cd.get('levels'))])
            for f in range(0,len(formi3)):
                cd=formi3[f].cleaned_data
                kpi_name.append(dicti[str(cd.get('kpi'))])
                kpi_aggregation.append(cd.get('aggregation'))
            sql_query="SELECT "
            sql_query += '"'+levels[0]+'"'
            for i in range(1,len(levels)):
                sql_query += "," + '"'+i+'"'
            for i in range(0,len(kpi_name)):
                sql_query += "," + kpi_aggregation[i] + "(" + '"' +kpi_name[i] + '"' +")"
            sql_query += " FROM " + table_name
            sql_query += " WHERE " + filter_param[0] + " " + filter_cri[0] + " " + filter_val[0]
            for i in range(1, len(filter_param)):
                sql_query += " AND " + filter_param[i] + " " + filter_cri[i] + " " + filter_val[i]
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

                cur.execute(sql_query)
                columns = []
                for val in cur.description:
                    columns.append(val[0])
                print(columns)
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
            print("=====")
            print(sf_data)
            cols = []
            for col in sap_data:
                cols.append(col)
            sf_data.columns = cols

            print('Column names in SF data')
            for col in sf_data:
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
            for val in kpi_list:
                val = val.replace('"', '')
                new_kpi.append(val)

            # print(new_kpi)

            # print(cmp_level)
            # print(kpi)

            # STEP 6
            # Creating a new dataframe having consolidated salesorgs
            # salesorgs = sap_data[cmp_level].to_list()

            # # sf_data[cmp_level] = sf_data[cmp_level].apply(str)

            # for val in sf_data[cmp_level].to_list():
            #     if val not in salesorgs:
            #         salesorgs.append(val)

            # # print(len(salesorgs))
            # # print(salesorgs)    

            # # adding list as first column of new dataframe
            # df_output = pd.DataFrame()
            # df_output[cmp_level] = salesorgs 


            # # testing for multiple KPIs

            # for i in range(0,num_cols):
            #     kpi_1 = []
            #     sf_dict = sf_data.set_index(cmp_level).to_dict('dict')
            #     # print(sf_dict)
            #     for org in salesorgs:
            #         if org not in sf_dict[new_kpi[i]].keys():
            #             kpi_1.append(0)
            #         else:
            #             kpi_1.append(sf_dict[new_kpi[i]][org])
            #     col1name = new_kpi[i] + "_sf"
            #     df_output[col1name] = kpi_1
            # # print(kpi_1)

            # # making KPI 2 column
            #     kpi_2 = []
            #     sap_dict = sap_data.set_index(cmp_level).to_dict('dict')
            #     for org in salesorgs:
            #         if org not in sap_dict[new_kpi[i]].keys():
            #             kpi_2.append(0)
            #         else:
            #             kpi_2.append(sap_dict[new_kpi[i]][org])
            # # print(kpi_1)
            #     col2name = new_kpi[i] + "_sap"
            #     df_output[col2name] = kpi_2


            # # extracting difference
            #     diffname = 'Diff' + str(i+1)
            #     df_output[diffname] = df_output[col1name] - df_output[col2name]


            # # making KPI 1 column

            # df_output.to_excel("result.xlsx", index=False)

            # print("Result saved as result.xlsx")   

            #samveg
            s=num_kpis
            v=num_level
            m=cmp_level
            kpi=new_kpi
            print("=====")
            print(s)
            print(v)
            print(m)
            print(kpi)

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
            print(orderi)

                

            # print(dicti_sap)
            # not_found=[]
            # for i in range(0,s):
            df_output=pd.DataFrame(t,columns=m)
            print(df_output)
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
                print(kpi1)
                print(kpi2)
                # print(kpi[i])
                # df_output.insert()
                df_output[kpi[i]+str("_sap")]=kpi1
                df_output[kpi[i]+str("_sf")]=kpi2
                df_output["differ"+"_{0}".format(kpi[i])]=kpi_diff

            df_output.to_excel("result.xlsx", index=False)

            print("Result saved as result.xlsx")   

        


        return(HttpResponse("Success"))
    else:


            





        return render(request,"Test/detail.html",{"form1":Filterset,"form2":Levelset,'form3':KPIset,'form4':Uploading})
    # if request.method ==  "POST":
    #     form = Credentials(request.POST)
    #     print(form)
    #     if form.is_valid():
    #         print("hey")
            
    # else:
    #     redentialsset=formset_factory(Credentials,extra=2)

    #     return render(request,"Test/detail.html")
