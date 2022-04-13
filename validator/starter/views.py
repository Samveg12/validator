from django.shortcuts import render
from django.http import HttpResponse
# from django.forms import MyForm
from connector import sf_connector
# Create your views here.

def inital(request):
    if request.method == 'POST':
        form = MyForm(request.POST)
        print(form)
        username = request.POST.get('username')
        database = request.POST.get('database')
        schema = request.POST.get('schema')
        table = request.POST.get('table')
        sf_connector(username, database, schema, table)
    return render(request, 'query.html')