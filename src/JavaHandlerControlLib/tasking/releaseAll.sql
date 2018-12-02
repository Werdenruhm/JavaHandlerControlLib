CREATE OR REPLACE FUNCTION pg_temp.releaseAll(
p_jhc_ex BIGINT, 
p_errorMode BOOLEAN
) RETURNS void AS $$






-----THEBEGIN-----
DECLARE
    v_r record;
BEGIN
    perform createFact('en_FactType.handleTaskReleaseError');

    for v_r in (
        select task_id, status_en from Task
        where status_en = enum_id('en_TaskStatus.inProcess') and 
            task_id in 
            (
                select lockId from TaskLOCK where jhc_ex = p_jhc_ex and lockIdType = 'i'
                union all
                select task_id from Task where queue in (select lockId::integer from TaskLOCK where jhc_ex = p_jhc_ex and lockIdType = 'q')
            )
        for update
    ) loop
        if (v_r.status_en <> enum_id('en_TaskStatus.inProcess')) then--на всякий случай, хотя этого никогда не произойдет
            RAISE EXCEPTION 'task_id=% имеет статус (%) отличный от inProcess!', v_r.task_id, enum_alias_by_id(v_r.status_en);
        end if;

        update Task set status_en = enum_id('en_TaskStatus.new') where task_id = v_r.task_id;
    end loop;

    delete from TaskLOCK where jhc_ex = p_jhc_ex and (lockIdType = 'i' or lockIdType = 'q');
    
END 
-----THEEND-----
$$ LANGUAGE plpgsql;