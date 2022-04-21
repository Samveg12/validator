from django.contrib import admin
from django.urls import include, path
from . import settings
from django.contrib.staticfiles.urls import static

urlpatterns = [
    path('Test/', include('Test.urls')),
    path('admin/', admin.site.urls),
] + static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)