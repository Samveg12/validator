from django.urls import path

from . import views

urlpatterns = [
    path('', views.index, name='index'),
    path('detail',views.detail,name='detail'),
    path('download', views.download, name='download'),
    path('download_file', views.download_file, name='download_file')
]

