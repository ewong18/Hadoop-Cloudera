--Creates HIVE DDL format for table view using Teradata metadata from underlying table
--Can be exported and copy/pasted into HIVE DDL 'select' command
--Also takes into consideration the order of the columns in the view vs. the table

select c2.columnid, c1.tablename, trim(c1.columnname)||' '||case when c1.columntype = 'CV' then 'varchar'||substr(c1.columnformat, 2,20)
							when c1.columntype = 'CF' then 'char'||substr(c1.columnformat, 2,20)
							when c1.columntype = 'I' then 'Integer'
							when c1.columntype = 'D' then 'decimal'||'('||cast(c1.decimaltotaldigits as varchar(6))
														||','||cast(c1.decimalfractionaldigits as varchar(6))||')'
							when c1.columntype in ('DA', 'TS') then 'varchar(35)'							
							else 'TBD' end ||',' as td_format,
				  trim(c1.columnname)||' '||case when c1.columntype = 'CV' then 'string'||','
							when c1.columntype = 'CF' then 'string'||','
							when c1.columntype = 'I' then 'int'||','
							when c1.columntype = 'D' then 'decimal'||'('||cast(c1.decimaltotaldigits as varchar(6))
														||','||cast(c1.decimalfractionaldigits as varchar(6))||')'||','
							when c1.columntype in ('DA', 'TS') then 'string'||', -- Originally Date/TS in TD'					
							else 'TBD' end  as hive_format
		from dbc.columns c1
		join dbc.columnsv c2
			on  c1.tablename = c2.tablename
			and c1.columnname = c2.columnname
		where c1.databasename = '<tablename>'
		and c2.databasename = '<viewname>'
	order by 2, 1 asc;
