from random import choices
from ssl import ALERT_DESCRIPTION_ACCESS_DENIED
from django.forms import modelform_factory, modelformset_factory
from django.shortcuts import render,redirect
from django.http import HttpResponse
from calendar import c
from django.forms import formset_factory
from functools import partial, wraps

from filecmp import cmp
import pandas as pd
import snowflake.connector
# import config
from django.contrib import messages

from .forms import Credentials,Filter,KPIs,Level
num_level=0
num_kpis=0
num_filter=0
final=[]
def index(request):
    global num_level
    global num_kpis
    global num_filter
    global final
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

            for i in range(1,len(s)+1):
                z=[]
                z.append(str(i))
                z.append(str(s[i-1]))
                z=tuple(z)
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
    global num_kpis
    global num_filter
    global final
    print(final)
    # ServiceFormSet = formset_factory(wraps(ServiceForm)(partial(ServiceForm, affiliate=request.affiliate)), extra=3)

    Filterset=formset_factory(wraps(Filter)(partial(Filter, choices=final)), extra=num_filter)
    # ServiceFormSet = formset_factory(wraps(ServiceForm)(partial(ServiceForm, affiliate=request.affiliate)), extra=3)

    Levelset=formset_factory(wraps(Level)(partial(Level, choices=final)), extra=num_filter)
    KPIset=formset_factory(wraps(KPIs)(partial(KPIs, choices=final)), extra=num_filter)
    # KPIset=formset_factory(KPIs(choices=final),extra=num_kpis)
    # Levelset=formset_factory(Level(choices=final),extra=num_filter)





    return render(request,"Test/detail.html",{"form1":Filterset,"form2":Levelset,'form3':KPIset})
    # if request.method ==  "POST":
    #     form = Credentials(request.POST)
    #     print(form)
    #     if form.is_valid():
    #         print("hey")
            
    # else:
    #     redentialsset=formset_factory(Credentials,extra=2)

    #     return render(request,"Test/detail.html")
