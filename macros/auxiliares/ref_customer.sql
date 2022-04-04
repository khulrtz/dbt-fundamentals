{% macro refs(tabla) %}
    {% if tabla == 'CUSTOMER' %}
        {{ ref('STG_CUSTOMER')}}
    {% endif %}
    {% if tabla == 'NATION' %}
        {{ ref('STG_NATION')}}
    {% endif %}
{% endmacro %}