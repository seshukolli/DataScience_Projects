from django.http import HttpResponse

def home(request):
    age =23
    return HttpResponse("Hello World seshu")
