{% materialization event_version_history, adapter='snowflake' %}
    {% set target = this.incorporate(type='table') %}
    {% set publication = ref('event_change_candidates') %}
    {% set batches = publication.incorporate(path={'identifier': 'event_candidate_batches'}) %}
    {% set applied = target.incorporate(path={'identifier': 'event_version_batches'}) %}
    {% set retention = target.incorporate(path={'identifier': 'inspection_retention_expiries'}) %}
    {% set candidates = target.incorporate(path={'identifier': 'event_history_candidates_staged'}) %}
    {% set prior = target.incorporate(path={'identifier': 'event_history_batch_prior'}) %}
    {% if not config.get('contract').enforced %}
        {{ exceptions.raise_compiler_error('event_versions requires an enforced contract') }}
    {% endif %}

    {% call statement('prepare_history', auto_begin=false) %}
        {{ snowflake_create_history_tables(target, applied, retention) }}
    {% endcall %}
    {# Inspect the actual persisted output, never this model's candidate SQL. #}
    {% call statement('history_contract', auto_begin=false) %}
        {{ snowflake_assert_relation_contract(target, model.columns, identity=true) }}
        {{ snowflake_assert_relation_contract(applied, {
            'batch_id': {'data_type': 'text'}, 'feed_name': {'data_type': 'text'},
            'observed_at': {'data_type': 'timestamp_tz'}, 'applied_at': {'data_type': 'timestamp_tz'}
        }) }}
        {{ snowflake_assert_relation_contract(retention, {
            'source_record_key': {'data_type': 'text'}, 'batch_id': {'data_type': 'text'},
            'event_version_key': {'data_type': 'bigint'}, 'observed_at': {'data_type': 'timestamp_tz'},
            'retention_cutoff': {'data_type': 'date'}
        }) }}
    {% endcall %}
    {% call statement('stage_history', auto_begin=false) %}
        create or replace temporary table {{ candidates }} as {{ sql }};
        create or replace temporary table {{ prior }} as select * from {{ target }} where false;
    {% endcall %}
    {% call statement('main', auto_begin=false) %}
        {{ snowflake_apply_history(target, applied, retention, candidates, batches, prior) }}
    {% endcall %}
    {{ return({'relations': [target]}) }}
{% endmaterialization %}
