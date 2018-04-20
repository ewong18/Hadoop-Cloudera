--Creates select DDL for copy pasting into Hive and Impala DDL
--Takes into consideration the timestamp conversion issues
--Does not strip delimiters and newlines, as Sqoop command "--hive-drop-import-delims" should work
--Uses Oracle data dictionary under SYS user

select column_id, table_name,
        trim(column_name)||' '||case
            when data_type in ('NUMBER') and data_precision is null then 'int,'
            when data_type in ('NUMBER') and data_precision is not NULL then 'decimal('||trim(data_precision)||','||trim(data_scale)||'),'
            when data_type in ('VARCHAR2', 'CHAR') then 'string,'
            when data_type in ('DATE') then 'string, --formerly DATE in Oracle'
            end as hive_format,
        case when data_type in ('DATE') then 'cast('||trim(column_name)||' as timestamp)'
            else trim(column_name)
            end ||',' as imp_hiveSel_format,
        trim(column_name)||' '||case 
            when data_type in ('NUMBER') and data_precision is null then 'int'
            when data_type in ('NUMBER') and data_precision is not NULL then 'decimal('||trim(data_precision)||','||trim(data_scale)||')'
            when data_type in ('VARCHAR2', 'CHAR') then 'string'
            when data_type in ('DATE') then 'timestamp'
            end||',' as impala_format,
        trim(column_name)|| ' '|| case
            when data_type in ('VARCHAR2') then trim(data_type)||'('||trim(data_length)||'),'
            when data_type in ('NUMBER') and data_precision is not NULL then trim(data_type)||'('||trim(data_precision)||','||trim(data_scale)||'),'
            else trim(data_type)||','
            end as orcl_format
    from sys.all_tab_columns
        where owner = '<USER>'
        and table_name like '<ABC_%>'
order by table_name, column_id asc;
