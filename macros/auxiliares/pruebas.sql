{% macro pruebas() %}
{% set my_dict = {"nombre": 'julio'} %}
{% set my_yaml_string = toyaml(my_dict) %}

{% do log(my_yaml_string) %}
{% endmacro %}