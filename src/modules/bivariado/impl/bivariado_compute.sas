/* =========================================================================
bivariado_compute.sas - Computo de analisis bivariado (flujo unificado)

Principios:
- _scope_input se filtra una sola vez a una tabla canonica CAS
- TRAIN/OOT se derivan como columna conceptual (_biv_period)
- cortes numericos se calculan solo con TRAIN, pero se aplican a toda la
  linea temporal consolidada
- CAS se usa para stage, agregacion, append y orden
- work se usa solo para PROC RANK y la construccion de cortes
========================================================================= */

%macro _biv_sort_cas(table_name=, orderby=, groupby={});

    %if %length(%superq(table_name)) = 0 or %length(%superq(orderby)) = 0 %then
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

%mend _biv_sort_cas;

%macro _biv_init_detail_table(table_name=);

    data work.&table_name.;
        length Seccion $12 Tipo_Variable $12 Variable $64 Valor $200
            Ventana $10 Periodo 8 N 8 Pct_Cuentas 8 Defaults 8 RD 8;
        stop;
    run;

%mend _biv_init_detail_table;

%macro _biv_calcular_cortes(train_data=, var=, groups=);

    %local rnd;
    %let rnd=%sysfunc(int(%sysfunc(ranuni(0))*100000));
    %global _biv_cut_n;
    %let _biv_cut_n=0;

    data work._biv_cut_&rnd._0;
        set &train_data.(keep=&var.);
        if &var. in (., 1111111111, -1111111111, 2222222222, -2222222222,
            3333333333, -3333333333, 4444444444, 5555555555, 6666666666,
            7777777777, -999999999) then &var.=.;
        if not missing(&var.) then &var.=round(&var., 0.0001);
    run;

    proc rank data=work._biv_cut_&rnd._0 out=work._biv_cut_&rnd._1
        groups=&groups.;
        ranks RANGO;
        var &var.;
    run;

    proc sql noprint;
        create table work._biv_cut_&rnd._2 as
        select RANGO,
               min(&var.) as MINVAL,
               max(&var.) as MAXVAL
        from work._biv_cut_&rnd._1
        group by RANGO;
    quit;

    proc sort data=work._biv_cut_&rnd._2;
        by RANGO;
    run;

    data work._biv_cortes;
        set work._biv_cut_&rnd._2(rename=(RANGO=RANGO_INI)) end=EOF;
        retain MARCA 0;
        FLAG_INI=0;
        FLAG_FIN=0;
        LAGMAXVAL=lag(MAXVAL);
        RANGO=RANGO_INI + 1;
        if RANGO_INI=. then RANGO=0;
        if RANGO_INI >= 0 then MARCA=MARCA + 1;
        if MARCA=1 then FLAG_INI=1;
        if EOF then FLAG_FIN=1;
    run;

    data work._biv_cortes;
        set work._biv_cortes;
        length ETIQUETA $200;
        if RANGO=0 then ETIQUETA='00. Missing';
        else if FLAG_INI=1 then ETIQUETA=cat(put(RANGO, z2.), '. <-Inf; ',
            strip(put(MAXVAL, F12.4)), ']');
        else if FLAG_FIN=1 then ETIQUETA=cat(put(RANGO, z2.), '. <',
            strip(put(LAGMAXVAL, F12.4)), '; +Inf>');
        else ETIQUETA=cat(put(RANGO, z2.), '. <', strip(put(LAGMAXVAL, F12.4)),
            '; ', strip(put(MAXVAL, F12.4)), ']');
    run;

    proc sql noprint;
        select count(*) into :_biv_cut_n trimmed
        from work._biv_cortes
        where RANGO > 0;
    quit;

    data _null_;
        set work._biv_cortes(where=(RANGO > 0));
        _idx + 1;
        call symputx(cats('_biv_cut_label', _idx), ETIQUETA, 'G');
        call symputx(cats('_biv_cut_flag_ini', _idx), FLAG_INI, 'G');
        call symputx(cats('_biv_cut_flag_fin', _idx), FLAG_FIN, 'G');
        call symputx(cats('_biv_cut_max', _idx), strip(put(MAXVAL, best32.-L)), 'G');
        call symputx(cats('_biv_cut_lag', _idx), strip(put(LAGMAXVAL, best32.-L)), 'G');
    run;

    proc datasets library=work nolist nowarn;
        delete _biv_cut_&rnd.:;
    quit;

%mend _biv_calcular_cortes;

%macro _biv_append_numeric(source_data=, train_data=, target=, byvar=, var=,
    groups=5, section=, out_table=);

    %local _i;
    %_biv_calcular_cortes(train_data=&train_data., var=&var., groups=&groups.);

    data work._biv_stage_num;
        length Seccion $12 Tipo_Variable $12 Variable $64 Valor $200
            Ventana $10 Periodo 8 Target 8;
        set &source_data.(keep=_biv_period &byvar. &target. &var.);
        Seccion="&section.";
        Tipo_Variable='NUMERICA';
        Variable="&var.";
        Ventana=_biv_period;
        Periodo=&byvar.;
        Target=&target.;

        if missing(&var.) or &var. in (1111111111, -1111111111, 2222222222,
            -2222222222, 3333333333, -3333333333, 4444444444, 5555555555,
            6666666666, 7777777777, -999999999) then Valor='00. Missing';
        %if &_biv_cut_n. > 0 %then %do;
            %do _i=1 %to &_biv_cut_n.;
                %if &&_biv_cut_flag_ini&_i. = 1 %then %do;
                    else if &var. <= &&_biv_cut_max&_i. then Valor="&&_biv_cut_label&_i.";
                %end;
                %else %if &&_biv_cut_flag_fin&_i. = 1 %then %do;
                    else if &var. > &&_biv_cut_lag&_i. then Valor="&&_biv_cut_label&_i.";
                %end;
                %else %do;
                    else if &var. > &&_biv_cut_lag&_i. and
                        &var. <= &&_biv_cut_max&_i. then Valor="&&_biv_cut_label&_i.";
                %end;
            %end;
            else Valor='99. Sin Asignar';
        %end;
        %else %do;
            else Valor='01. Sin Corte';
        %end;

        keep Seccion Tipo_Variable Variable Valor Ventana Periodo Target;
    run;

    proc sql;
        create table work._biv_append_num as
        select a.Seccion,
               a.Tipo_Variable,
               a.Variable,
               a.Valor,
               a.Ventana,
               a.Periodo,
               count(*) as N,
               case when b.Total_Obs > 0 then count(*) / b.Total_Obs else 0 end as Pct_Cuentas,
               sum(a.Target) as Defaults,
               avg(a.Target) as RD
        from work._biv_stage_num a
        inner join work._biv_period_totals b
            on a.Periodo = b.Periodo
        group by a.Seccion, a.Tipo_Variable, a.Variable, a.Valor,
                 a.Ventana, a.Periodo, b.Total_Obs;
    quit;

    proc append base=work.&out_table. data=work._biv_append_num force;
    quit;

    proc datasets library=work nolist nowarn;
        delete _biv_cortes _biv_stage_num _biv_append_num;
    quit;

%mend _biv_append_numeric;

%macro _biv_append_categorical(source_data=, target=, byvar=, var=, section=,
    out_table=);

    proc fedsql sessref=conn;
        create table casuser._biv_stage {options replace=true} as
        select cast('&section.' as varchar(12)) as Seccion,
               cast('CATEGORICA' as varchar(12)) as Tipo_Variable,
               cast('&var.' as varchar(64)) as Variable,
               case
                   when trim(cast(a.&var. as varchar(200))) = '' then '00. Missing'
                   when cast(a.&var. as varchar(200)) is null then '00. Missing'
                   else cast(a.&var. as varchar(200))
               end as Valor,
               cast(a._biv_period as varchar(10)) as Ventana,
               a.&byvar. as Periodo,
               a.&target. as Target
        from &source_data. a;
    quit;

    proc fedsql sessref=conn;
        create table casuser._biv_append {options replace=true} as
        select a.Seccion,
               a.Tipo_Variable,
               a.Variable,
               a.Valor,
               a.Ventana,
               a.Periodo,
               count(*) as N,
               case when b.Total_Obs > 0 then count(*) / b.Total_Obs else 0 end as Pct_Cuentas,
               sum(a.Target) as Defaults,
               avg(a.Target) as RD
        from casuser._biv_stage a
        inner join casuser._biv_period_totals b
            on a.Periodo = b.Periodo
        group by a.Seccion, a.Tipo_Variable, a.Variable, a.Valor,
                 a.Ventana, a.Periodo, b.Total_Obs;
    quit;

    data work._biv_append_cat;
        set casuser._biv_append;
    run;

    proc append base=work.&out_table. data=work._biv_append_cat force;
    quit;

    proc cas;
        session conn;
        table.dropTable / caslib='casuser' name='_biv_stage' quiet=true;
        table.dropTable / caslib='casuser' name='_biv_append' quiet=true;
    quit;

    proc datasets library=work nolist nowarn;
        delete _biv_append_cat;
    quit;

%mend _biv_append_categorical;

%macro _biv_build_detail(source_data=, train_data=, target=, byvar=,
    vars_num=, vars_cat=, groups=5, section=, out_table=);

    %local _idx _var;

    %if %length(%superq(vars_num)) > 0 %then %do;
        %let _idx=1;
        %let _var=%scan(&vars_num., &_idx., %str( ));
        %do %while(%length(%superq(_var)) > 0);
            %put NOTE: [bivariado_compute] Variable numerica=&_var. section=&section.;
            %_biv_append_numeric(source_data=&source_data., train_data=&train_data.,
                target=&target., byvar=&byvar., var=&_var., groups=&groups.,
                section=&section., out_table=&out_table.);
            %let _idx=%eval(&_idx. + 1);
            %let _var=%scan(&vars_num., &_idx., %str( ));
        %end;
    %end;

    %if %length(%superq(vars_cat)) > 0 %then %do;
        %let _idx=1;
        %let _var=%scan(&vars_cat., &_idx., %str( ));
        %do %while(%length(%superq(_var)) > 0);
            %put NOTE: [bivariado_compute] Variable categorica=&_var. section=&section.;
            %_biv_append_categorical(source_data=&source_data., target=&target.,
                byvar=&byvar., var=&_var., section=&section.,
                out_table=&out_table.);
            %let _idx=%eval(&_idx. + 1);
            %let _var=%scan(&vars_cat., &_idx., %str( ));
        %end;
    %end;

%mend _biv_build_detail;

%macro _bivariado_compute(source_data=casuser._biv_input,
    train_data=casuser._biv_train, target=, byvar=, vars_num=, vars_cat=,
    dri_num=, dri_cat=, groups=5);

    proc fedsql sessref=conn;
        create table casuser._biv_period_totals {options replace=true} as
        select &byvar. as Periodo,
               count(*) as Total_Obs
        from &source_data.
        group by &byvar.;
    quit;

    %_biv_sort_cas(table_name=_biv_period_totals,
        orderby=%str({"Periodo"}));

    data work._biv_period_totals;
        set casuser._biv_period_totals;
    run;

    %_biv_init_detail_table(table_name=_biv_main_detail);
    %_biv_init_detail_table(table_name=_biv_driver_detail);

    %_biv_build_detail(source_data=&source_data., train_data=&train_data.,
        target=&target., byvar=&byvar., vars_num=&vars_num.,
        vars_cat=&vars_cat., groups=&groups., section=PRINCIPAL,
        out_table=_biv_main_detail);

    %if %length(%superq(dri_num)) > 0 or %length(%superq(dri_cat)) > 0 %then %do;
        %_biv_build_detail(source_data=&source_data., train_data=&train_data.,
            target=&target., byvar=&byvar., vars_num=&dri_num.,
            vars_cat=&dri_cat., groups=&groups., section=DRIVER,
            out_table=_biv_driver_detail);
    %end;

    proc sort data=work._biv_main_detail;
        by Variable Periodo Valor;
    run;

    proc sort data=work._biv_driver_detail;
        by Variable Periodo Valor;
    run;

%mend _bivariado_compute;
