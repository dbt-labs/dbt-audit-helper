{% macro _get_audit_helper_namespaces() %}
  {% set override_namespaces = var('audit_helper_dispatch_list', []) %}
  {% do return(override_namespaces + ['audit_helper']) %}
{% endmacro %}
