from django.contrib import admin
from django.urls import include, path

urlpatterns = [
    path('Test/', include('Test.urls')),
    path('admin/', admin.site.urls),
]