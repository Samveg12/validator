from django.shortcuts import render,redirect
from django.http import HttpResponse
from calendar import c
from filecmp import cmp
import pandas as pd
import snowflake.connector
# import config

from .forms import Credentials

def index(request):
    if request.method ==  "POST":
        form = Credentials(request.POST)
        print(form)
        if form.is_valid():
            print("HITUZhfb")
            username = form.cleaned_data['username']
            print(username)
            password = form.cleaned_data['password']
            print(password)
            schema = form.cleaned_data['schema']
            print(schema)
            database = form.cleaned_data['database']
            print(database)
            table = form.cleaned_data['table']
            print(table)
            # mail(Name,email,Country)
            return redirect('detail')
        return redirect('/Test')
    form = Credentials()
    # return render(request,"country/register.html",{"form":form})
    return render(request,"Test/index.html",{"form":form})

def detail(request):
    
    return render(request,"Test/detail.html")
