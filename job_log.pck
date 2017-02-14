create or replace package pub_ds.job_log is

  -- Author  : andrey.grigoryev
  -- Created : 22.01.2015 18:32:45
  -- Purpose : Логгирование ETL-процедур

/*
04.12.2015:
    1. Добавлена обработка вложенных вызовов start_log - они не создают новую запись в t_job_logs, а пишутся в детальный лог.
    2. Работа с таблицами обернута в Execute immediate, чтобы пакет не инвалидировался при изменении структуры таблиц.
*/

function get_filial_id(in_filial_id number) return number ;
function get_package_log_level(in_owner varchar2, in_package_name varchar2) return number;

-- Запись в основной лог в начале процедуры
procedure start_log(in_filial_id number default(null), in_procedure_name varchar2 default(null), in_comment_text varchar2);

-- Запись в основной лог в конце процедуры
procedure end_log(in_error varchar2 default(null));

--- Запись в основной лог в конце процедуры с ошибкой
procedure end_log_with_error(in_error varchar2);

-- Запись в детальный лог
procedure write_detail_log(in_comment_text varchar2, in_rows_processed number default(0));

end job_log;
/
create or replace package body pub_ds.job_log is

-- Идентификатор текущего журнала
P_LOG_ID number(10) := null;

-- Идентфикатор журнала ошибок логгирования
P_LOG_ERROR_ID number(10) := -1;

-- Счетчик уровней вложенности. Нужен, чтобы отслеживать вложенные вызовы start_log
P_LOG_STACK_LEVEL number(10) := 0;

function get_package_log_level(in_owner varchar2, in_package_name varchar2) return number
is
    result_ number;
begin
    -- Используем execute immediate, чтобы пакет не инвалидировался при изменении структуры таблицы
    execute immediate '
    select
        sign(count(1))
    from
        log_parameters lp
    where
        lp.schema = upper(:in_owner)
    and lp.package_name = upper(:in_package_name)'
    into
        result_
    using in_owner, in_package_name
    ;
    return result_;
end;

function get_session_id return number
is
    result number;
begin
    select max(nvl(SID, 0))
    into Result
    from v$session
    where audsid = USERENV('SESSIONID');
    return(Result);
end;

-- Определяем код филиала. Пытаемся определить по имени пользователя, если не получается, то возращаем то, что передали на вход, или 0, если на входе пусто
function get_filial_id(in_filial_id number) return number
is
    result_ number(10);
begin
    -- Находим код филиала по имени текущего пользователя. Используем execute immediate, чтобы пакет не инвалидировался при изменении структуры d_filials
    execute immediate '
    select
        max(t.filial_id)
    from
        pub_ds.d_filials t
    where
        t.ds_owner = user' into result_;

    if result_ is null and in_filial_id is not null then
        result_ := in_filial_id;
    end if;

    return nvl(result_,0);
end;


procedure write_log_internal(in_log_id number, in_schema varchar2, in_package_name varchar2, in_comment_text varchar2, in_rows_processed number default(0))
is
    pragma autonomous_transaction;
    sid_ number(10);
begin
    -- Находим уровень логгирования для пакета
    -- Если запись в таблице есть, ничего не делаем и выходим
    if get_package_log_level(in_schema, in_package_name) > 0 then
        return;
    end if;


    sid_ := get_session_id();

    -- Используем execute immediate, чтобы пакет не инвалидировался при изменении структуры таблицы
    execute immediate '
    insert into t_job_log_details(jldt_id, jblg_jblg_id, start_time, comment_text, rows_processed, sid)
    values(
        t_job_log_details_seq.nextval,
        :in_log_id,
        sysdate,
        :in_comment_text,
        :in_rows_processed,
        :sid_
    )'
    using in_log_id, in_comment_text, in_rows_processed, sid_
    ;

    commit;
end;


procedure start_log(in_filial_id number default(null), in_procedure_name varchar2 default(null), in_comment_text varchar2)
is
    pragma autonomous_transaction;

    owner_ VARCHAR2(400);
    name_ VARCHAR2(400);
    lineno_ NUMBER;
    caller_t_ VARCHAR2(400);

    procedure_name_part_ varchar2(400) := '';
    procedure_name_ varchar2(400);
    sid_ number(10);
    filial_id_ number;
begin
    -- Находим, кто вызывал процедуру
    OWA_UTIL.WHO_CALLED_ME(owner_, name_, lineno_, caller_t_);

    -- Если имя процедуры передано в пакет, формируем строку для записи в лог. Если не передано, запишется только имя пакета.
    if in_procedure_name is not null then
        procedure_name_part_ := '.'||in_procedure_name;
    end if;

    procedure_name_ := upper(owner_||'.'||name_||procedure_name_part_);

    -- Если уже был вызван start_log, пишем информацию в детальный лог
    if P_LOG_ID is not null then
        write_log_internal(P_LOG_ID, owner_, name_, 'START_LOG: '||procedure_name_||': '||in_comment_text, 0);
        P_LOG_STACK_LEVEL := P_LOG_STACK_LEVEL + 1;
        return;
    end if;

    sid_ := get_session_id();

    filial_id_ := get_filial_id(in_filial_id);

    -- Получаем новый ID для записи в главной таблице
    select
        t_job_logs_seq.nextval
    into
        P_LOG_ID
    from
        dual;


    -- Добавляем запись в таблицу
    -- Используем execute immediate, чтобы пакет не инвалидировался при изменении структуры таблицы
    execute immediate '
    insert into t_job_logs(
        jblg_id,
        filial_id,
        start_time,
        comment_text,
        procedure_name,
        db_user_name,
        sid)
    values(
        :P_LOG_ID,
        :filial_id_,
        sysdate,
        :in_comment_text,
        :procedure_name_,
        nvl2 (sys_context(''USERENV'', ''PROXY_USER''), sys_context(''USERENV'', ''PROXY_USER'') || ''['' || user || '']'', user),
        :sid_
    )'
    using P_LOG_ID,filial_id_, in_comment_text, procedure_name_, sid_
    ;
    commit;
end;

procedure end_log_internal(in_error varchar2 default(null), owner_ VARCHAR2, name_ varchar2)
is
    pragma autonomous_transaction;
begin
    -- Если процедура вызвана до того, как создан основной лог, сигнализируем об ошибке
    if P_LOG_ID is null then
        write_log_internal(P_LOG_ERROR_ID, owner_, name_,
                           'Вызван end_log без start_log'||case when in_error is not null then ', ошибка: '||in_error else '' end, 0);
        return;
    end if;

    if P_LOG_STACK_LEVEL > 0 then
        -- Этот вызов end_log - не верхнего уровня, поэтому не завершаем, а просто пишем информашию

        write_log_internal(P_LOG_ID, owner_, name_, 'END_LOG: '||upper(owner_||'.'||name_)||': '||in_error, 0);
        P_LOG_STACK_LEVEL := P_LOG_STACK_LEVEL - 1;
        return;
    end if;

    -- Используем execute immediate, чтобы пакет не инвалидировался при изменении структуры таблицы
    execute immediate '
    update t_job_logs t
    set
        t.end_time = sysdate,
        t.error = :in_error
    where
        t.jblg_id = :P_LOG_ID'
    using in_error, P_LOG_ID
    ;
    commit;

    -- Сбрасываем текущий идентфикатор, чтобы больше ничего не писалось в детальные логи.
    P_LOG_ID := null;
end;


procedure end_log(in_error varchar2 default(null))
is
    pragma autonomous_transaction;
    owner_ VARCHAR2(400);
    name_ VARCHAR2(400);
    lineno_ NUMBER;
    caller_t_ VARCHAR2(400);
begin
    OWA_UTIL.WHO_CALLED_ME(owner_, name_, lineno_, caller_t_);
    end_log_internal(in_error, owner_, name_);
end;

procedure end_log_with_error(in_error varchar2)
is
    pragma autonomous_transaction;
    owner_ VARCHAR2(400);
    name_ VARCHAR2(400);
    lineno_ NUMBER;
    caller_t_ VARCHAR2(400);
begin
    OWA_UTIL.WHO_CALLED_ME(owner_, name_, lineno_, caller_t_);
    end_log_internal(in_error, owner_, name_);
end;


procedure write_detail_log(in_comment_text varchar2, in_rows_processed number default(0))
is
    pragma autonomous_transaction;
    owner_ VARCHAR2(400);
    name_ VARCHAR2(400);
    lineno_ NUMBER;
    caller_t_ VARCHAR2(400);
begin
    -- Находим, кто вызывал процедуру
    OWA_UTIL.WHO_CALLED_ME(owner_, name_, lineno_, caller_t_);

    -- Если процедура вызвана до того, как создан основной лог, сигнализируем об ошибке
    if P_LOG_ID is null then
        write_log_internal(P_LOG_ERROR_ID, owner_, name_, 'Вызван write_detail_log без start_log, текст комментария: '||in_comment_text, in_rows_processed);
        return;
    end if;
    write_log_internal(P_LOG_ID, owner_, name_, in_comment_text, in_rows_processed);
end;


end job_log;
/
