/* =========================================================================
missings_compute.sas - Computo CAS-first alineado a missings_legacy.sas

Metodologia preservada:
- detalle por valor missing/dummy realmente encontrado
- resumen por variable con sum(pct_miss)
- no fabricar filas en 0% para variables sin hallazgos

Salida:
- casuser.<detail_table>      : Split, Variable, Type, Dummy_Value, NMiss, Pct_Miss
- casuser.<summary_table>     : Split, Variable, Type, Pct_Miss
- casuser.<split_totals_table>: Split, Total_N
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

%macro _miss_detail_union_sql(data=, split_var=, vars_num=, vars_cat=,
    outvar=_miss_union_sql);

    %local _sql _i _var;

    %let _sql=;

    %let _i=1;
    %let _var=%scan(%superq(vars_num), &_i., %str( ));
    %do %while(%length(%superq(_var)) > 0);
        %if %length(%superq(_sql)) > 0 %then %let _sql=&_sql. union all ;
        %let _sql=&_sql.
            select a.&split_var. as Split,
                   "&_var." as Variable,
                   'num' as Type,
                   case
                       when a.&_var. is null then '.'
                       else cast(a.&_var. as varchar(128))
                   end as Dummy_Value
            from &data. a
            where a.&_var. is null
               or a.&_var. in (
                    1111111111, -1111111111,
                    2222222222, -2222222222,
                    3333333333, -3333333333,
                    4444444444, 5555555555,
                    6666666666, 7777777777,
                    -999999999
               );
        %let _i=%eval(&_i. + 1);
        %let _var=%scan(%superq(vars_num), &_i., %str( ));
    %end;

    %let _i=1;
    %let _var=%scan(%superq(vars_cat), &_i., %str( ));
    %do %while(%length(%superq(_var)) > 0);
        %if %length(%superq(_sql)) > 0 %then %let _sql=&_sql. union all ;
        %let _sql=&_sql.
            select a.&split_var. as Split,
                   "&_var." as Variable,
                   'categ' as Type,
                   case
                       when a.&_var. is null then ''
                       when trim(a.&_var.)='' then ''
                       when upcase(trim(a.&_var.))='MISSING' then 'MISSING'
                       when trim(a.&_var.)='.' then '.'
                       else trim(a.&_var.)
                   end as Dummy_Value
            from &data. a
            where a.&_var. is null
               or trim(a.&_var.)=''
               or upcase(trim(a.&_var.))='MISSING'
               or trim(a.&_var.)='.';
        %let _i=%eval(&_i. + 1);
        %let _var=%scan(%superq(vars_cat), &_i., %str( ));
    %end;

    %let &outvar.=&_sql.;

%mend _miss_detail_union_sql;

%macro _miss_compute(data=, split_var=Split, vars_num=, vars_cat=,
    detail_table=_miss_detail, summary_table=_miss_summary,
    var_catalog_table=_miss_var_catalog,
    split_totals_table=_miss_split_totals);

    %local _miss_union_sql;

    proc datasets library=casuser nolist nowarn;
        delete &detail_table. &summary_table. &var_catalog_table.
            &split_totals_table. _miss_detail_raw _miss_summary_stage;
    quit;

    proc fedsql sessref=conn;
        create table casuser.&split_totals_table. {options replace=true} as
        select &split_var. as Split,
               count(*) as Total_N
        from &data.
        group by &split_var.;
    quit;

    %_miss_detail_union_sql(data=&data., split_var=&split_var.,
        vars_num=&vars_num., vars_cat=&vars_cat., outvar=_miss_union_sql);

    %if %length(%superq(_miss_union_sql)) > 0 %then %do;
        proc fedsql sessref=conn;
            create table casuser._miss_detail_raw {options replace=true} as
            &_miss_union_sql.;
        quit;
    %end;
    %else %do;
        proc fedsql sessref=conn;
            create table casuser._miss_detail_raw {options replace=true} as
            select cast('' as varchar(16)) as Split,
                   cast('' as varchar(128)) as Variable,
                   cast('' as varchar(16)) as Type,
                   cast('' as varchar(128)) as Dummy_Value
            from &data.
            where 1=0;
        quit;
    %end;

    proc fedsql sessref=conn;
        create table casuser.&detail_table. {options replace=true} as
        select d.Split,
               d.Variable,
               d.Type,
               d.Dummy_Value,
               count(*) as NMiss,
               count(*) / t.Total_N as Pct_Miss
        from casuser._miss_detail_raw d
        inner join casuser.&split_totals_table. t
            on d.Split=t.Split
        group by d.Split, d.Variable, d.Type, d.Dummy_Value, t.Total_N;
    quit;

    proc fedsql sessref=conn;
        create table casuser.&summary_table. {options replace=true} as
        select Split,
               Variable,
               max(Type) as Type,
               sum(Pct_Miss) as Pct_Miss
        from casuser.&detail_table.
        group by Split, Variable;
    quit;

%mend _miss_compute;
