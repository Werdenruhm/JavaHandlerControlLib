CREATE OR REPLACE FUNCTION pg_temp.grab(
p_jhc_ex BIGINT,
p_taskType_ens INTEGER[],
p_limit INTEGER,
p_faultTimeout_sec INTEGER,
out out_task_id BIGINT,
out out_task_pid BIGINT,
out out_type_en INTEGER,
out out_type_alias VARCHAR,
out out_queue INTEGER,
out out_taskRow VARCHAR,
out out_Param_value VARCHAR)
RETURNS setof record AS $$




-----THEBEGIN-----
DECLARE 
    v_skipQueue integer array;
    v_r record;
    v_rr record;
    v_got integer := 0;
    v_chk1 bigint;
    v_lastTaskFaultChecked timestamp;
    v_lastTaskDeadChecked timestamp;
    c_chkInterval integer := 10;-- интервал проверок lastTaskFaultChecked и lastTaskDeadChecked, секунд
    v_taskType_ens integer[];
BEGIN
    perform createFact('en_FactType.handleTaskPrepare');

    v_taskType_ens := p_taskType_ens::integer[];    

    --проверка задач в статусе fault
    v_lastTaskFaultChecked := JavaHandlerControlVar_getT('lastTaskFaultChecked', false, false);
    if (v_lastTaskFaultChecked is null or v_lastTaskFaultChecked < localtimestamp - (c_chkInterval * interval '1 second')) then
        DECLARE
            v_ok boolean := false;
            v_rrr record;
        BEGIN
            begin
                v_lastTaskFaultChecked := JavaHandlerControlVar_getT('lastTaskFaultChecked', true, true);
                v_ok := true;
            exception
                when SQLSTATE '55P03' then --55P03 = lock_not_available
            end;
            if (v_ok and (v_lastTaskFaultChecked is null or v_lastTaskFaultChecked < localtimestamp - (c_chkInterval * interval '1 second'))) then
                PERFORM JavaHandlerControlVar_setT('lastTaskFaultChecked', date_trunc('second',localtimestamp));

                for v_rrr in (
                    select task_id, lockIdType, lockId, coalesce((select inserted from TaskLOCK where lockId = flt.lockId and lockIdType = flt.lockIdType), updated) as inserted
                    from (
                        select task_id, updated, case when queue is null then task_id else queue end as lockId, case when queue is null then 'fi' else 'fq' end as lockIdType
                        from Task where status_en = enum_id('en_TaskStatus.fault') for update nowait
                    ) flt
                ) loop

                    if (v_rrr.inserted < localtimestamp - (p_faultTimeout_sec * interval '1 second') or v_rrr.inserted is null) then
                        delete from TaskLOCK where lockId = v_rrr.lockId and lockIdType = v_rrr.lockIdType;
                        update Task set status_en = enum_id('en_TaskStatus.new') where status_en = enum_id('en_TaskStatus.fault') and task_id = v_rrr.task_id;    
                    end if;
                end loop;

                -- удаление старых меток времени ошибок
                --delete from TaskLOCK where lockIdType = 'fT' and inserted < (localtimestamp - (30 * interval '1 day'));
            end if;
        END;
    end if;

    --проверка задач, заблокированных мёртвыми обработчиками
    v_lastTaskDeadChecked := JavaHandlerControlVar_getT('lastTaskDeadChecked', false, false);
    if (v_lastTaskDeadChecked is null or v_lastTaskDeadChecked < localtimestamp - (c_chkInterval * interval '1 second')) then
        DECLARE
            v_ok boolean := false;
            v_rrr record;
            v_skip bigint array;
        BEGIN
            begin
                v_lastTaskDeadChecked := JavaHandlerControlVar_getT('lastTaskDeadChecked', true, true);
                v_ok := true;
            exception
                when SQLSTATE '55P03' then --55P03 = lock_not_available
            end;
            if (v_ok and (v_lastTaskDeadChecked is null or v_lastTaskDeadChecked < localtimestamp - (c_chkInterval * interval '1 second'))) then
                PERFORM JavaHandlerControlVar_setT('lastTaskDeadChecked', date_trunc('second',localtimestamp));
                
                for v_rrr in (                
                    select jhc_ex, lockId, lockIdType 
                    from TaskLOCK as il left join JavaHandlerControl as jhc on il.jhc_ex = jhc.jhc_id 
                    where (jhc_id is null or jhc.died is not null) and (lockIdType = 'i' or lockIdType = 'q')
                ) loop
                    if (v_skip is null or not (v_rrr.jhc_ex = any (v_skip))) then
                        delete from TaskLOCK where jhc_ex = v_rrr.jhc_ex;
                        v_skip := array_append(v_skip, v_rrr.jhc_ex);
                    end if;
                    delete from TaskLOCK where lockId = v_rrr.lockId and lockIdType = v_rrr.lockIdType;
                    update Task set status_en = enum_id('en_TaskStatus.new') 
                    where status_en = enum_id('en_TaskStatus.inProcess') and ((v_rrr.lockIdType = 'i' and task_id = v_rrr.lockId) or (v_rrr.lockIdType = 'q' and queue = v_rrr.lockId));    
                end loop;
            end if;
        END;
    end if;

    --блокировка задач
    for v_r in (
        select task_id, queue, 
            task_pid, type_en, row_to_json(Task.*) as taskRow, Param.value as Param_value
            
        from Task left join Param on Task.param_ex = Param.param_id
        where 
            (status_en = enum_id('en_TaskStatus.new') or status_en = enum_id('en_TaskStatus.retry'))
            and not exists(select 1 from TaskLOCK where lockId = Task.queue and (lockIdType = 'fq' or lockIdType = 'q'))
            and type_en = any (v_taskType_ens)            
        order by task_id limit (p_limit + p_limit/2)
    ) loop
        if (v_got < p_limit) then 
            if (v_r.queue is null) then
                begin
                    perform null from task where task_id = v_r.task_id for update nowait;
                exception
                    when SQLSTATE '55P03' then --55P03 = lock_not_available
                        continue;
                end;
                
                --сначала проверяем блокировки в незавершенных транзакциях
                if (not try_advisory_xact_lock('TaskLOCK', v_r.task_id, 'i')) then
                    continue;
                end if;
                --затем блокируем в таблице блокировок
                begin
                    insert into TaskLOCK(jhc_ex, inserted, lockId, lockIdType)  
                    values (p_jhc_ex, localtimestamp, v_r.task_id , 'i');
                exception
                    when SQLSTATE '23505' then --23505 = unique_violation
                        continue;
                end;

                update task set status_en = enum_id('en_TaskStatus.inProcess'), start = localtimestamp
                where task_id = v_r.task_id;
                
                select v_r.task_id, v_r.queue, v_r.task_pid, v_r.type_en, enum_alias_by_id(v_r.type_en), v_r.taskRow, v_r.Param_value
                into   out_task_id, out_queue, out_task_pid, out_type_en,                out_type_alias, out_taskRow, out_Param_value;
                RETURN NEXT;
                v_got := v_got + 1;
            else
                if (v_skipQueue is null or not (v_r.queue = any (v_skipQueue))) then
                    --сначала проверяем блокировки в незавершенных транзакциях
                    if (not try_advisory_xact_lock('TaskLOCK', v_r.queue, 'q')) then
                        v_skipQueue := array_append(v_skipQueue, v_r.queue);
                        continue;
                    end if;
                    --затем блокируем в таблице блокировок
                    begin
                        insert into TaskLOCK(jhc_ex, inserted, lockId, lockIdType)  
                        values (p_jhc_ex, localtimestamp, v_r.queue , 'q');
                    exception
                        when SQLSTATE '23505' then --23505 = unique_violation
                            v_skipQueue := array_append(v_skipQueue, v_r.queue);
                            continue;
                    end;
                    for v_rr in (
                        select task_id, status_en, 
                            task_pid, type_en, row_to_json(Task.*) as taskRow, Param.value as Param_value

                        from Task left join Param on Task.param_ex = Param.param_id
                        where queue = v_r.queue and (status_en = enum_id('en_TaskStatus.new') or status_en = enum_id('en_TaskStatus.retry') or status_en = enum_id('en_TaskStatus.inProcess')) 
                        order by task_id
                    ) loop
                        if (v_rr.status_en = enum_id('en_TaskStatus.inProcess')) then
                            RAISE EXCEPTION 'В очереди queue=% находится сообщение task_id=% в статусе inProcess!', v_r.queue, v_rr.task_id;
                        end if;

                        perform null from task where task_id = v_rr.task_id for update nowait;
                        
                        if (not v_rr.type_en = any (v_taskType_ens)) then
                            v_skipQueue := array_append(v_skipQueue, v_r.queue);
                            continue;
                        end if;         

                        update task set status_en = enum_id('en_TaskStatus.inProcess'), start = localtimestamp
                        where task_id = v_rr.task_id;

                        select v_rr.task_id, v_r.queue, v_rr.task_pid, v_rr.type_en, enum_alias_by_id(v_r.type_en), v_rr.taskRow, v_rr.Param_value
                        into   out_task_id,  out_queue,  out_task_pid,  out_type_en,                 out_type_alias, out_taskRow,  out_Param_value;
                        RETURN NEXT;
                        v_got := v_got + 1;
                    end loop;
                    v_skipQueue := array_append(v_skipQueue, v_r.queue);
                end if;
            end if;
        end if;
        if (v_got >= p_limit) then
            exit;
        end if;
    end loop;
END 
-----THEEND-----
$$ LANGUAGE plpgsql;