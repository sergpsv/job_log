create or replace package pub_ds.job_log_psv is

  -- Author  : andrey.grigoryev
  -- Created : 22.01.2015 18:32:45
  -- Purpose : ����������� ETL-��������

-- ������������� �������� �������
P_LOG_ID           number(10) := null;
p_Copy2dbms_output number     := 0;

/*
04.12.2015:
    1. ��������� ��������� ��������� ������� start_log - ��� �� ������� ����� ������ � t_job_logs, � ������� � ��������� ���.
    2. ������ � ��������� �������� � Execute immediate, ����� ����� �� ��������������� ��� ��������� ��������� ������.

25/01/2017   �������� 
    1. ������� ������������� ������� � ������������
    2. ������� �-��� "get_session_id". ������ �� sys_context('userenv', 'sid');
    3. �� write_detail_log ����� ������� � ���� "'������ write_detail_log ��� start_log, ����� �����������: '||", ���� ����� ��� start_log
    4. ��������� Exception, ������� ��� ���������� ��� ������ ������ 4000 � �����. ����� ���� ������ ��������� ��� ������, ���� ���� ��������� 4000
    5. � write_detail_log ��������� dbms_application_info.set_client_info - ����� ���� �������, ��� ������\������ ������
    6. � start_log\end_log ��������� dbms_application_info.set_action('jblg_jblg_id='||P_LOG_ID); ������ �� V$session ����� ����� ������� � ���� � ���������� ��� �� ������� � ��� ����������
    7. �������� get_filial_id � ������ � ��� ����������� ��������� ������
    8. ������� p_Copy2dbms_output ��� ����� ������� ����������� ����� � DBMS_OUTPUT
*/

function get_filial_id(in_filial_id number) return number ;
function get_package_log_level(in_owner varchar2, in_package_name varchar2) return number;

-- ������ � �������� ��� � ������ ���������
procedure start_log(in_filial_id number default(null), in_procedure_name varchar2 default(null), in_comment_text varchar2);

-- ������ � �������� ��� � ����� ���������
procedure end_log(in_error varchar2 default(null));

--- ������ � �������� ��� � ����� ��������� � �������
procedure end_log_with_error(in_error varchar2);

-- ������ � ��������� ���
procedure write_detail_log(in_comment_text varchar2, in_rows_processed number default(0));

end;
/
create or replace package body pub_ds.job_log_psv is


-- ������������ ������� ������ ������������
P_LOG_ERROR_ID number(10) := -1;

-- ������� ������� �����������. �����, ����� ����������� ��������� ������ start_log
P_LOG_STACK_LEVEL number(10) := 0;


-------------------------------------------------------------------------------------------------------------------------
function get_package_log_level(in_owner varchar2, in_package_name varchar2) return number
is
    result_ number;
begin
    -- ���������� execute immediate, ����� ����� �� ��������������� ��� ��������� ��������� �������
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


-------------------------------------------------------------------------------------------------------------------------
-- ���������� ��� �������. �������� ���������� �� ����� ������������, ���� �� ����������, �� ��������� ��, ��� �������� �� ����, ��� 0, ���� �� ����� �����
function get_filial_id(in_filial_id number) return number
is
    result_ number(10);
    has_such_filial_ number(10);
begin
    -- ������� ��� ������� �� ����� �������� ������������. ���������� execute immediate, ����� ����� �� ��������������� ��� ��������� ��������� d_filials
    execute immediate '
    select max(t.filial_id)
      from pub_ds.d_filials t
     where t.ds_owner = user' into result_;
    
    execute immediate '
    select max(case when filial_id=:f then filial_id else 0 end)
      from pub_ds.d_filials t' 
    into has_such_filial_ 
    using in_filial_id;

    /*if result_ is null and in_filial_id is not null then
        result_ := in_filial_id;
    end if;*/

    return coalesce(result_, has_such_filial_, 0);/*nvl(result_,0)*/
end;


-------------------------------------------------------------------------------------------------------------------------
procedure write_log_internal(in_log_id number, in_schema varchar2, in_package_name varchar2, in_comment_text varchar2, in_rows_processed number default(0))
is
    pragma autonomous_transaction;
    l_writeRowCnt number :=0;
    l_tmpStr varchar2(3999);
begin
    -- ������� ������� ������������ ��� ������
    -- ���� ������ � ������� ����, ������ �� ������ � �������
    if get_package_log_level(in_schema, in_package_name) > 0 then
        return;
    end if;

    -- ���������� execute immediate, ����� ����� �� ��������������� ��� ��������� ��������� �������
    while (length(in_comment_text)/3999) > l_writeRowCnt 
    loop
			l_tmpStr := substr(in_comment_text, l_writeRowCnt*3999+1, 3999); 
      if l_writeRowCnt=0 then 
				 dbms_application_info.set_client_info(l_tmpStr);
  		end if;
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
      using in_log_id, l_tmpStr, in_rows_processed, sys_context('userenv', 'sid');
      l_writeRowCnt := l_writeRowCnt +1;
      if p_Copy2dbms_output=1 then 
        dbms_output.put_line(l_tmpStr);
      end if;
    end loop;

    commit;
end;

-------------------------------------------------------------------------------------------------------------------------
procedure start_log(in_filial_id number default(null), in_procedure_name varchar2 default(null), in_comment_text varchar2)
is
    pragma autonomous_transaction;

    owner_ VARCHAR2(400);
    name_ VARCHAR2(400);
    lineno_ NUMBER;
    caller_t_ VARCHAR2(400);

    procedure_name_part_ varchar2(400) := '';
    procedure_name_ varchar2(400);
    filial_id_ number;
begin
    -- �������, ��� ������� ���������
    OWA_UTIL.WHO_CALLED_ME(owner_, name_, lineno_, caller_t_);

    -- ���� ��� ��������� �������� � �����, ��������� ������ ��� ������ � ���. ���� �� ��������, ��������� ������ ��� ������.
    if in_procedure_name is not null then
        procedure_name_part_ := '.'||in_procedure_name;
    end if;

    procedure_name_ := upper(owner_||'.'||name_||procedure_name_part_);

    -- ���� ��� ��� ������ start_log, ����� ���������� � ��������� ���
    if P_LOG_ID is not null then
        write_log_internal(P_LOG_ID, owner_, name_, 'START_LOG: '||procedure_name_||': '||in_comment_text, 0);
        P_LOG_STACK_LEVEL := P_LOG_STACK_LEVEL + 1;
        return;
    end if;

    filial_id_ := get_filial_id(in_filial_id);

    -- �������� ����� ID ��� ������ � ������� �������
    select
        t_job_logs_seq.nextval
    into
        P_LOG_ID
    from
        dual;


    -- ��������� ������ � �������
    -- ���������� execute immediate, ����� ����� �� ��������������� ��� ��������� ��������� �������
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
    using P_LOG_ID,filial_id_, in_comment_text, procedure_name_, sys_context('userenv', 'sid');
    if p_Copy2dbms_output=1 then 
      dbms_output.put_line(in_comment_text);
    end if;

    dbms_application_info.set_action('jblg_jblg_id='||P_LOG_ID); 
    commit;
end;

-------------------------------------------------------------------------------------------------------------------
procedure end_log_internal(in_error varchar2 default(null), owner_ VARCHAR2, name_ varchar2)
is
    pragma autonomous_transaction;
begin
    -- ���� ��������� ������� �� ����, ��� ������ �������� ���, ������������� �� ������
    if P_LOG_ID is null then
        write_log_internal(P_LOG_ERROR_ID, owner_, name_,
                           '������ end_log ��� start_log'||case when in_error is not null then ', ������: '||in_error else '' end, 0);
        return;
    end if;

    if P_LOG_STACK_LEVEL > 0 then
        -- ���� ����� end_log - �� �������� ������, ������� �� ���������, � ������ ����� ����������

        write_log_internal(P_LOG_ID, owner_, name_, 'END_LOG: '||upper(owner_||'.'||name_)||': '||in_error, 0);
        P_LOG_STACK_LEVEL := P_LOG_STACK_LEVEL - 1;
        return;
    end if;

    -- ���������� execute immediate, ����� ����� �� ��������������� ��� ��������� ��������� �������
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

    dbms_application_info.set_client_info(''); --psv
    dbms_application_info.set_action('');      -- psv
    -- ���������� ������� ������������, ����� ������ ������ �� �������� � ��������� ����.
    P_LOG_ID := null;
end;

-------------------------------------------------------------------------------------------------------------------------
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

-------------------------------------------------------------------------------------------------------------------------
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


-------------------------------------------------------------------------------------------------------------------------
procedure write_detail_log(in_comment_text varchar2, in_rows_processed number default(0))
is
    pragma autonomous_transaction;
    owner_ VARCHAR2(400);
    name_ VARCHAR2(400);
    lineno_ NUMBER;
    caller_t_ VARCHAR2(400);
begin
    -- �������, ��� ������� ���������
    OWA_UTIL.WHO_CALLED_ME(owner_, name_, lineno_, caller_t_);

    -- ���� ��������� ������� �� ����, ��� ������ �������� ���, ������������� �� ������
    if P_LOG_ID is null then
        write_log_internal(P_LOG_ERROR_ID, owner_, name_, in_comment_text, in_rows_processed);
        return;
    end if;
    write_log_internal(P_LOG_ID, owner_, name_, in_comment_text, in_rows_processed);
end;


end;
/
