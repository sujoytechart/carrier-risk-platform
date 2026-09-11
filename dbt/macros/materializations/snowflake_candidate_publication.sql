{% materialization snowflake_candidate_publication, adapter='snowflake' %}
    {% set target = this.incorporate(type='table') %}
    {% set batches = target.incorporate(path={'identifier': 'event_candidate_batches'}) %}
    {% set staged_candidates = target.incorporate(path={'identifier': 'event_candidates_staged'}) %}
    {% set staged_batches = target.incorporate(path={'identifier': 'event_batches_staged'}) %}

    {# DDL is deliberately outside the business transaction. Temp relations are
       connection-local; the command guard serializes every persistent writer. #}
    {% call statement('prepare_publication', auto_begin=false) %}
        create or replace temporary table {{ staged_candidates }} as {{ sql }};
        create or replace temporary table {{ staged_batches }} as
            select * from {{ ref('clean_snapshot_batches') }};
        create table if not exists {{ target }} like {{ staged_candidates }};
        create table if not exists {{ batches }} like {{ staged_batches }};
    {% endcall %}
    {% call statement('main', auto_begin=false) %}
        {{ snowflake_publish_candidates(target, batches, staged_candidates, staged_batches) }}
    {% endcall %}
    {{ return({'relations': [target]}) }}
{% endmaterialization %}
