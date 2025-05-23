{% macro get_hashkey(column_name) %}
  {% set hash_query %}
    SELECT MD5(LOWER(TRIM('{{ column_name }}'))) AS hash_key
  {% endset %}
  
  {% set result = run_query(hash_query) %}
  
  {% if execute %}
    {{ return(result.columns['hash_key'].values()[0]) }}
  {% endif %}
  
  {{ return('') }}
{% endmacro %}