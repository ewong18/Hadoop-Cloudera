--Creates HIVE DDL format for table view using Teradata metadata from underlying table
--Creates DDL for Impala publish step which also casts time and date strings to Timestamp
--Creates Teradata DDL format to be used to create new views while stripping new line characters and carriage returns
--Can be exported and copy/pasted into HIVE DDL 'select' command
--Also takes into consideration the order of the columns in the view vs. the table
--

 select c2.columnid, c1.tablename, 
					case when c1.columntype in ('CV') then 'cast(oreplace(oreplace(oreplace('||trim(c1.columnname)
										||', chr(10), '' ''), ''^'', '' ''), chr(13), '' '') as varchar'
										||trim(substr(c1.columnformat, 2,20))||') as '||trim(c1.columnname)
							when c1.columntype in ('CF') then 'cast(oreplace(oreplace(oreplace('||trim(c1.columnname)
										||', chr(10), '' ''), ''^'', '' ''), chr(13), '' '') as char'
										||trim(substr(c1.columnformat, 2,20))||') as '||trim(c1.columnname)
							else c1.columnname
							end ||',' as td_oreplace,
 					trim(c1.columnname)||' '||case when c1.columntype = 'CV' then 'string'||','
							when c1.columntype = 'CF' then 'string'||','
							when c1.columntype = 'I' then 'int'||','
							when c1.columntype = 'D' then 'decimal('||cast(c1.decimaltotaldigits as varchar(6))
														||','||cast(c1.decimalfractionaldigits as varchar(6))||')'||','
							when c1.columntype in ('DA', 'TS') then 'string'||', -- Originally Date/TS in TD'					
							else 'TBD' end  as hive_format,
					case when c1.columntype in ('DA', 'TS') then 'cast('||trim(c1.columnname)||' as timestamp)'
							else trim(c1.columnname)
							end || ',' as imp_hiveSel_format,
					trim(c1.columnname)||' '||case when c1.columntype = 'CV' then 'string'
							when c1.columntype = 'CF' then 'string'
							when c1.columntype = 'I' then 'int'
							when c1.columntype = 'D' then 'decimal'||'('||cast(c1.decimaltotaldigits as varchar(6))
														||','||cast(c1.decimalfractionaldigits as varchar(6))||')'
							when c1.columntype in ('DA', 'TS') then 'timestamp'					
							else 'TBD' end ||','  as impala_format,
 					trim(c1.columnname)||' '||case when c1.columntype = 'CV' then 'varchar'||trim(substr(c1.columnformat, 2,20))
							when c1.columntype = 'CF' then 'char'||trim(substr(c1.columnformat, 2,20))
							when c1.columntype = 'I' then 'Integer'
							when c1.columntype = 'D' then 'decimal('||cast(c1.decimaltotaldigits as varchar(6))
														||','||cast(c1.decimalfractionaldigits as varchar(6))||')'
							when c1.columntype in ('DA', 'TS') then 'varchar(35)'							
							else 'TBD' end ||',' as td_format
		from dbc.columns c1
		join dbc.columnsv c2
			on  c1.tablename = c2.tablename
			and c1.columnname = c2.columnname
		where c1.databasename = 'NACRMFBICOREDB'
		and c2.databasename = 'NACRMFBISEMVW'
	order by 2, 1 asc;
