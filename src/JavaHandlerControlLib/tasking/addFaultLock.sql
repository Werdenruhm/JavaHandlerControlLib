CREATE OR REPLACE FUNCTION pg_temp.addFaultLock(
p_jhc_ex BIGINT, 
p_task_id BIGINT, 
p_queue INTEGER
) RETURNS void AS $$ 





-----THEBEGIN-----
DECLARE 
    v_st integer;
    v_q integer;
    v_jhc bigint;
BEGIN
    if (p_jhc_ex is null or p_task_id is null) then
        RAISE EXCEPTION 'Не указан p_jhc_ex и/или p_task_id!';
    end if;

    select status_en, queue into v_st, v_q from task where task_id = p_task_id for update;

    if (v_st is null) then
        RAISE EXCEPTION 'Указанный p_task_id=% не обнаружен!', p_task_id;
    end if;

    if (v_st <> enum_id('en_TaskStatus.inProcess')) then
        RAISE EXCEPTION 'Указанный p_task_id=% имеет статус (%) отличный от inProcess!', p_task_id, enum_alias_by_id(v_st);
    end if;

    perform createFact('en_FactType.handleTaskReleaseError');

    update task set status_en = enum_id('en_TaskStatus.fault') where task_id = p_task_id;

    if (p_queue is null) then 
        if (v_q is not null) then
            RAISE EXCEPTION 'Указанный p_task_id=% имеет queue (%) при p_queue is null!', p_task_id, v_q;
        end if;

        select jhc_ex into v_jhc from TaskLOCK where lockId = p_task_id and lockIdType = 'i';
        
        if (v_jhc is null) then
            RAISE EXCEPTION 'Указанный p_task_id=% не блокирован в TaskLOCK по task_id!', p_task_id;
        end if;

        if (v_jhc <> p_jhc_ex) then
            RAISE EXCEPTION 'Указанный p_task_id=% блокирован в TaskLOCK  по task_id за другим jhc: % !', p_task_id, v_jhc;
        end if;

        insert into TaskLOCK(jhc_ex, inserted, lockId, lockIdType)  
        values (p_jhc_ex, localtimestamp, p_task_id , 'fi');

    else
        if (v_q is null or v_q <> p_queue) then
            RAISE EXCEPTION 'Указанный p_task_id=% имеет queue (%) отличный от p_queue=%!', p_task_id, v_q, p_queue;
        end if;

        select jhc_ex into v_jhc from TaskLOCK where lockId = p_queue and lockIdType = 'q';
        
        if (v_jhc is null) then
            RAISE EXCEPTION 'Указанный p_task_id=% не блокирован в TaskLOCK по queue!', p_task_id;
        end if;

        if (v_jhc <> p_jhc_ex) then
            RAISE EXCEPTION 'Указанный p_task_id=% блокирован в TaskLOCK по queue за другим jhc: % !', p_task_id, v_jhc;
        end if;

        insert into TaskLOCK(jhc_ex, inserted, lockId, lockIdType)  
        values (p_jhc_ex, localtimestamp, p_queue , 'fq');

    end if;

    -- добавление (если не существует) метки времени ошибки
    insert into TaskLOCK(jhc_ex, inserted, lockId, lockIdType)  
    select p_jhc_ex, localtimestamp, p_task_id , 'fT' 
    where not exists (select 1 from TaskLOCK l where l.lockId = p_task_id and l.lockIdType = 'fT');

END
-----THEEND-----
$$ LANGUAGE plpgsql;