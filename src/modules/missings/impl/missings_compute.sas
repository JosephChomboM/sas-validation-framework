/* =========================================================================
missings_compute.sas - Compute de Missings alineado a missings_legacy.sas

Modelo:
- una corrida por split
- detalle solo con hallazgos reales
- resumen por variable con sum(Pct_Miss)
- listas numericas y categoricas se procesan por separado
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

%macro _miss_append_cas(source=, target=);

    proc cas;
        session conn;
        table.append /
            source={caslib="casuser", name="&source."},
            target={caslib="casuser", name="&target."};
    quit;

%mend _miss_append_cas;

%macro _miss_init_outputs(detail_table=, summary_table=);

    data casuser.&detail_table.;
        length Variable $128 Type $16 Dummy_Value $256 Total 8 NMiss 8
            Pct_Miss 8 Var_Ord 8;
        format Pct_Miss percent8.2;
        stop;
    run;

    data casuser.&summary_table.;
        length Variable $128 Type $16 Total_Pct_Missing 8 Var_Ord 8;
        format Total_Pct_Missing percent8.2;
        stop;
    run;

%mend _miss_init_outputs;

%macro _miss_count_rows(data=, outvar=);

    proc sql noprint;
        select count(*) into :&outvar. trimmed
        from &data.;
    quit;

    %if %sysevalf(%superq(&outvar.)=, boolean) %then %let &outvar.=0;

%mend _miss_count_rows;

%macro _miss_compute_numeric_var(data=, var=, total=, ord=, detail_table=,
    summary_table=);

    %local _stage_nobs;

    proc fedsql sessref=conn;
        create table casuser._miss_det_stage {options replace=true} as
        select cast('&var.' as varchar(128)) as Variable,
               cast('num' as varchar(16)) as Type,
               case
                   when &var. is null then '.'
                   else cast(&var. as varchar(256))
               end as Dummy_Value,
               &total. as Total,
               count(*) as NMiss,
               count(*) / &total. as Pct_Miss,
               &ord. as Var_Ord
        from &data.
        where &var. is null
           or &var. in (
                1111111111, -1111111111,
                2222222222, -2222222222,
                3333333333, -3333333333,
                4444444444, 5555555555,
                6666666666, 7777777777,
                -999999999
           )
        group by case
                     when &var. is null then '.'
                     else cast(&var. as varchar(256))
                 end;
    quit;

    %_miss_count_rows(data=casuser._miss_det_stage, outvar=_stage_nobs);

    %if &_stage_nobs. > 0 %then %do;
        %_miss_append_cas(source=_miss_det_stage, target=&detail_table.);

        proc fedsql sessref=conn;
            create table casuser._miss_sum_stage {options replace=true} as
            select cast('&var.' as varchar(128)) as Variable,
                   cast('num' as varchar(16)) as Type,
                   sum(Pct_Miss) as Total_Pct_Missing,
                   &ord. as Var_Ord
            from casuser._miss_det_stage;
        quit;

        %_miss_append_cas(source=_miss_sum_stage, target=&summary_table.);
    %end;

    proc datasets library=casuser nolist nowarn;
        delete _miss_det_stage _miss_sum_stage;
    quit;

%mend _miss_compute_numeric_var;

%macro _miss_compute_categ_var(data=, var=, total=, ord=, detail_table=,
    summary_table=);

    %local _stage_nobs;

    proc fedsql sessref=conn;
        create table casuser._miss_det_stage {options replace=true} as
        select cast('&var.' as varchar(128)) as Variable,
               cast('categ' as varchar(16)) as Type,
               case
                   when &var. is null then ''
                   when trim(cast(&var. as varchar(256)))='' then ''
                   when upcase(trim(cast(&var. as varchar(256))))='MISSING'
                       then 'MISSING'
                   when trim(cast(&var. as varchar(256)))='.' then '.'
                   else trim(cast(&var. as varchar(256)))
               end as Dummy_Value,
               &total. as Total,
               count(*) as NMiss,
               count(*) / &total. as Pct_Miss,
               &ord. as Var_Ord
        from &data.
        where &var. is null
           or trim(cast(&var. as varchar(256)))=''
           or upcase(trim(cast(&var. as varchar(256))))='MISSING'
           or trim(cast(&var. as varchar(256)))='.'
        group by case
                     when &var. is null then ''
                     when trim(cast(&var. as varchar(256)))='' then ''
                     when upcase(trim(cast(&var. as varchar(256))))='MISSING'
                         then 'MISSING'
                     when trim(cast(&var. as varchar(256)))='.' then '.'
                     else trim(cast(&var. as varchar(256)))
                 end;
    quit;

    %_miss_count_rows(data=casuser._miss_det_stage, outvar=_stage_nobs);

    %if &_stage_nobs. > 0 %then %do;
        %_miss_append_cas(source=_miss_det_stage, target=&detail_table.);

        proc fedsql sessref=conn;
            create table casuser._miss_sum_stage {options replace=true} as
            select cast('&var.' as varchar(128)) as Variable,
                   cast('categ' as varchar(16)) as Type,
                   sum(Pct_Miss) as Total_Pct_Missing,
                   &ord. as Var_Ord
            from casuser._miss_det_stage;
        quit;

        %_miss_append_cas(source=_miss_sum_stage, target=&summary_table.);
    %end;

    proc datasets library=casuser nolist nowarn;
        delete _miss_det_stage _miss_sum_stage;
    quit;

%mend _miss_compute_categ_var;

%macro _miss_compute_split(data=, vars_num=, vars_cat=, detail_table=,
    summary_table=);

    %local _miss_total _i _var _ord;

    proc datasets library=casuser nolist nowarn;
        delete &detail_table. &summary_table.;
    quit;

    %_miss_init_outputs(detail_table=&detail_table., summary_table=&summary_table.);
    %_miss_count_rows(data=&data., outvar=_miss_total);

    %let _ord=0;

    %let _i=1;
    %let _var=%scan(%superq(vars_num), &_i., %str( ));
    %do %while(%length(%superq(_var)) > 0);
        %let _ord=%eval(&_ord. + 1);
        %_miss_compute_numeric_var(data=&data., var=&_var., total=&_miss_total.,
            ord=&_ord., detail_table=&detail_table.,
            summary_table=&summary_table.);
        %let _i=%eval(&_i. + 1);
        %let _var=%scan(%superq(vars_num), &_i., %str( ));
    %end;

    %let _i=1;
    %let _var=%scan(%superq(vars_cat), &_i., %str( ));
    %do %while(%length(%superq(_var)) > 0);
        %let _ord=%eval(&_ord. + 1);
        %_miss_compute_categ_var(data=&data., var=&_var., total=&_miss_total.,
            ord=&_ord., detail_table=&detail_table.,
            summary_table=&summary_table.);
        %let _i=%eval(&_i. + 1);
        %let _var=%scan(%superq(vars_cat), &_i., %str( ));
    %end;

    %_miss_sort_cas(table_name=&detail_table.,
        orderby=%str({"Var_Ord", "Dummy_Value"}));
    %_miss_sort_cas(table_name=&summary_table.,
        orderby=%str({"Var_Ord"}));

%mend _miss_compute_split;
