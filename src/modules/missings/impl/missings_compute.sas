/* =========================================================================
missings_compute.sas - Computo CAS-first de analisis de missings/dummies

Macros:
%_miss_sort_cas      - ordena en CAS (table.partition) para presentacion
%_miss_compute       - genera tablas CAS de detalle y resumen

Salida de %_miss_compute:
- casuser.<detail_table> : split, variable, type, dummy_value, nmiss, pct_miss
- casuser.<summary_table>: split, variable, type, nmiss, pct_miss

Regla de sort:
- No ordenar durante agregaciones
- Ordenar solo al final para legibilidad del reporte
========================================================================= */

%macro _miss_sort_cas(table_name=, orderby=, groupby={});

    %if %length(%superq(table_name))=0 or %length(%superq(orderby))=0 %then
        %return;

    proc cas;
        session conn;
        table.partition /
            table={
                caslib="casuser",
                name="&table_name.",
                orderby=&orderby.,
                groupby=&groupby.
            },
            casout={
                caslib="casuser",
                name="&table_name.",
                replace=true
            };
    quit;

%mend _miss_sort_cas;

%macro _miss_append_stage(target_table=, stage_table=);

    proc cas;
        session conn;
        table.append /
            source={caslib="casuser", name="&stage_table."},
            target={caslib="casuser", name="&target_table."};
        table.dropTable / caslib="casuser" name="&stage_table." quiet=true;
    quit;

%mend _miss_append_stage;

%macro _miss_numeric_stage(data=, split_var=, var=, stage_table=_miss_stage);

    proc fedsql sessref=conn;
        create table casuser.&stage_table. {options replace=true} as
        select a.&split_var. as split,
               "&var." as variable,
               'num' as type,
               case
                   when a.&var. is null then '.'
                   else cast(a.&var. as varchar(64))
               end as dummy_value,
               count(*) as nmiss,
               count(*) / t.total_n as pct_miss
        from &data. a
        inner join (
            select &split_var., count(*) as total_n
            from &data.
            group by &split_var.
        ) t
            on a.&split_var.=t.&split_var.
        where a.&var. is null
           or a.&var. in (
                1111111111, -1111111111,
                2222222222, -2222222222,
                3333333333, -3333333333,
                4444444444, 5555555555,
                6666666666, 7777777777,
                -999999999
           )
        group by a.&split_var.,
                 case
                     when a.&var. is null then '.'
                     else cast(a.&var. as varchar(64))
                 end,
                 t.total_n;
    quit;

%mend _miss_numeric_stage;

%macro _miss_categ_stage(data=, split_var=, var=, stage_table=_miss_stage);

    proc fedsql sessref=conn;
        create table casuser.&stage_table. {options replace=true} as
        select a.&split_var. as split,
               "&var." as variable,
               'categ' as type,
               case
                   when a.&var. is null then '<NULL>'
                   when trim(a.&var.)='' then '<BLANK>'
                   when upcase(trim(a.&var.))='MISSING' then 'MISSING'
                   when trim(a.&var.)='.' then '.'
                   else trim(a.&var.)
               end as dummy_value,
               count(*) as nmiss,
               count(*) / t.total_n as pct_miss
        from &data. a
        inner join (
            select &split_var., count(*) as total_n
            from &data.
            group by &split_var.
        ) t
            on a.&split_var.=t.&split_var.
        where a.&var. is null
           or trim(a.&var.)=''
           or upcase(trim(a.&var.))='MISSING'
           or trim(a.&var.)='.'
        group by a.&split_var.,
                 case
                     when a.&var. is null then '<NULL>'
                     when trim(a.&var.)='' then '<BLANK>'
                     when upcase(trim(a.&var.))='MISSING' then 'MISSING'
                     when trim(a.&var.)='.' then '.'
                     else trim(a.&var.)
                 end,
                 t.total_n;
    quit;

%mend _miss_categ_stage;

%macro _miss_compute(data=, split_var=_miss_split, vars_num=, vars_cat=,
    detail_table=_miss_detail, summary_table=_miss_summary);

    %local c z v v_cat;

    proc cas;
        session conn;
        table.dropTable / caslib="casuser" name="&detail_table." quiet=true;
        table.dropTable / caslib="casuser" name="&summary_table." quiet=true;
        table.dropTable / caslib="casuser" name="_miss_stage" quiet=true;
    quit;

    proc fedsql sessref=conn;
        create table casuser.&detail_table. {options replace=true} as
        select cast('' as varchar(8)) as split,
               cast('' as varchar(128)) as variable,
               cast('' as varchar(16)) as type,
               cast('' as varchar(128)) as dummy_value,
               cast(0 as double) as nmiss,
               cast(0 as double) as pct_miss
        from &data.
        where 1=0;
    quit;

    /* Procesar variables numericas */
    %if %length(%superq(vars_num)) > 0 %then %do;
        %let c=1;
        %let v=%scan(%superq(vars_num), &c., %str( ));
        %do %while(%length(%superq(v)) > 0);
            %put NOTE: [missings] Procesando variable numerica: &v.;
            %_miss_numeric_stage(data=&data., split_var=&split_var., var=&v.,
                stage_table=_miss_stage);
            %_miss_append_stage(target_table=&detail_table.,
                stage_table=_miss_stage);

            %let c=%eval(&c. + 1);
            %let v=%scan(%superq(vars_num), &c., %str( ));
        %end;
    %end;

    /* Procesar variables categoricas */
    %if %length(%superq(vars_cat)) > 0 %then %do;
        %let z=1;
        %let v_cat=%scan(%superq(vars_cat), &z., %str( ));
        %do %while(%length(%superq(v_cat)) > 0);
            %put NOTE: [missings] Procesando variable categorica: &v_cat.;
            %_miss_categ_stage(data=&data., split_var=&split_var.,
                var=&v_cat., stage_table=_miss_stage);
            %_miss_append_stage(target_table=&detail_table.,
                stage_table=_miss_stage);

            %let z=%eval(&z. + 1);
            %let v_cat=%scan(%superq(vars_cat), &z., %str( ));
        %end;
    %end;

    proc fedsql sessref=conn;
        create table casuser.&summary_table. {options replace=true} as
        select split,
               variable,
               max(type) as type,
               sum(nmiss) as nmiss,
               sum(pct_miss) as pct_miss
        from casuser.&detail_table.
        group by split, variable;
    quit;

%mend _miss_compute;
