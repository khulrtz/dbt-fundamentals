{% set my_dict = {"abc": 123} %}
{% set my_yaml_string = toyaml(my_dict) %}

{% do log(my_yaml_string) %}