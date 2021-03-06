from random import choices
from django import forms

class Credentials(forms.Form):
    username = forms.CharField(label='Username:', max_length=100)
    database = forms.CharField(label='Database', max_length=100)
    schema = forms.CharField(label='Schema', max_length=100)
    table = forms.CharField(label='Table', max_length=100)
    num_level = forms.IntegerField(label='Enter number of Characteristics')
    num_kpis = forms.IntegerField(label='Enter number of KPIs')
    num_filter = forms.IntegerField(label='Enter number of filter parameters')

class Level(forms.Form):
    levels = forms.ChoiceField(choices=(), label="Characteristic")
    def __init__(self,choices, *args, **kwargs):
        levels_choices = kwargs.pop('levels_choices', ())
        super().__init__(*args, **kwargs)
        self.fields['levels'].choices = choices
        print(choices)

class KPIs(forms.Form):
    kpi=forms.ChoiceField(choices=(), label="KPI")
    aggregation=forms.ChoiceField(choices=((1, ("SUM")),
                                        (2, ("COUNT")),
                                        (3, ("AVG")),
                                        (4, ("MIN")),
                                        (5, ("MAX")),
                                        ),)
    def __init__(self,choices, *args, **kwargs):
        kpi_choices = kwargs.pop('kpi_choices', ())
        super().__init__(*args, **kwargs)
        self.fields['kpi'].choices = choices

class Filter(forms.Form):    
    filter_parameter=forms.ChoiceField(choices=())
    filter_criteria=forms.ChoiceField(choices=((1, (">")),
                                        (2, ("<")),
                                        (3, ("like")),
                                        (4, ("between")),
                                        (5, ("=")),(6, (">=")),(4, ("<="))
                                        ),)
    filter_value=forms.CharField(label='Enter value')
    def __init__(self,choices, *args, **kwargs):
        filter_parameter_choices = kwargs.pop('filter_parameter_choices', ())
        super().__init__(*args, **kwargs)
        self.fields['filter_parameter'].choices = choices


class Uploaded(forms.Form):
    file = forms.FileField()
    

    