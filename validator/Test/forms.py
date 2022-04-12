from django import forms

class Credentials(forms.Form):
    username = forms.CharField(label='Your username', max_length=100)
    password = forms.CharField(label='Your password',widget=forms.PasswordInput)
    schema = forms.CharField(label='Your schema', max_length=100)
    database = forms.CharField(label='Your database', max_length=100)
    table = forms.CharField(label='Your table', max_length=100)




    