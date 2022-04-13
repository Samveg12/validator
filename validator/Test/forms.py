from django import forms

class Credentials(forms.Form):
    username = forms.CharField(label='Your username', max_length=100)
    password = forms.CharField(label='Your password',widget=forms.PasswordInput)
    schema = forms.CharField(label='Your schema', max_length=100)
    database = forms.CharField(label='Your database', max_length=100)
    table = forms.CharField(label='Your table', max_length=100)
    num_level = forms.IntegerField(label='Enter number of levels')
    num_kpis = forms.IntegerField(label='Enter number of kpis')
    num_filter = forms.IntegerField(label='Enter number of filter parameters')

class Level(forms.Form):
    levels = forms.ChoiceField(choices=())
    def __init__(self, *args, **kwargs):
        levels_choices = kwargs.pop('levels_choices', ())
        super().__init__(*args, **kwargs)
        self.fields['levels'].choices = levels_choices

class KPIs(forms.Form):
    kpi=forms.ChoiceField(choices=())
    aggregation=forms.CharField(label='Enter aggregation')
    def __init__(self,kpi_choices, *args, **kwargs):
        kpi_choices = kwargs.pop('kpi_choices', ())
        super().__init__(*args, **kwargs)
        self.fields['kpi'].choices = kpi_choices

class Filter(forms.Form):    
    filter_parameter=forms.ChoiceField(choices=())
    filter_criteria=forms.CharField(label='Enter criteria')
    filter_value=forms.CharField(label='Enter value')
    def __init__(self, *args, **kwargs):
        filter_parameter_choices = kwargs.pop('filter_parameter_choices', ())
        super().__init__(*args, **kwargs)
        self.fields['filter_parameter'].choices = filter_parameter_choices




    