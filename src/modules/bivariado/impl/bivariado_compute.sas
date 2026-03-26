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

    proc fedsql sessref=conn;
        create table casuser.&table_name. {options replace=true} as
        select cast('' as varchar(12)) as Seccion,
               cast('' as varchar(12)) as Tipo_Variable,
               cast('' as varchar(64)) as Variable,
               cast('' as varchar(200)) as Valor,
               cast('' as varchar(10)) as Ventana,
               cast(. as double) as Periodo,
               cast(. as double) as N,
               cast(. as double) as Pct_Cuentas,
               cast(. as double) as Defaults,
               cast(. as double) as RD
        from casuser._biv_period_totals
        where 1=0;
    quit;

%mend _biv_init_detail_table;

%macro _biv_calcular_cortes(train_data=, var=, groups=);

    %local rnd;
    %let rnd=%sysfunc(int(%sysfunc(ranuni(0))*100000));

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

    data _null_;
        set work._biv_cortes end=EOF;
        length _piece $800 _case_sql $32767;
        retain _case_sql 'case';

        if RANGO=0 then _piece = cats(
            ' when a.', "&var.",
            ' is null or a.', "&var.",
            ' in (1111111111,-1111111111,2222222222,-2222222222,3333333333,-3333333333,4444444444,5555555555,6666666666,7777777777,-999999999) then ''',
            strip(ETIQUETA), ''''
        );
        else if FLAG_INI=1 then _piece = cats(
            ' when a.', "&var.", ' <= ', strip(put(MAXVAL, best32.-L)),
            ' and a.', "&var.", ' is not null then ''', strip(ETIQUETA), ''''
        );
        else if FLAG_FIN=1 then _piece = cats(
            ' when a.', "&var.", ' > ', strip(put(LAGMAXVAL, best32.-L)),
            ' then ''', strip(ETIQUETA), ''''
        );
        else _piece = cats(
            ' when a.', "&var.", ' > ', strip(put(LAGMAXVAL, best32.-L)),
            ' and a.', "&var.", ' <= ', strip(put(MAXVAL, best32.-L)),
            ' then ''', strip(ETIQUETA), ''''
        );

        _case_sql = catx(' ', _case_sql, _piece);

        if EOF then do;
            _case_sql = catx(' ', _case_sql, " else '99. Sin Asignar' end");
            call symputx('_biv_case_sql', _case_sql, 'L');
        end;
    run;

    proc datasets library=work nolist nowarn;
        delete _biv_cut_&rnd.:;
    quit;

%mend _biv_calcular_cortes;

%macro _biv_append_numeric(source_data=, train_data=, target=, byvar=, var=,
    groups=5, section=, out_table=);

    %_biv_calcular_cortes(train_data=&train_data., var=&var., groups=&groups.);

    proc fedsql sessref=conn;
        create table casuser._biv_stage {options replace=true} as
        select cast('&section.' as varchar(12)) as Seccion,
               cast('NUMERICA' as varchar(12)) as Tipo_Variable,
               cast('&var.' as varchar(64)) as Variable,
               &_biv_case_sql. as Valor,
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

    proc cas;
        session conn;
        table.append /
            source={caslib='casuser', name='_biv_append'},
            target={caslib='casuser', name='&out_table.'};
        table.dropTable / caslib='casuser' name='_biv_stage' quiet=true;
        table.dropTable / caslib='casuser' name='_biv_append' quiet=true;
    quit;

    proc datasets library=work nolist nowarn;
        delete _biv_cortes;
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

    proc cas;
        session conn;
        table.append /
            source={caslib='casuser', name='_biv_append'},
            target={caslib='casuser', name='&out_table.'};
        table.dropTable / caslib='casuser' name='_biv_stage' quiet=true;
        table.dropTable / caslib='casuser' name='_biv_append' quiet=true;
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

    %_biv_sort_cas(table_name=_biv_main_detail,
        orderby=%str({"Variable", "Periodo", "Valor"}));
    %_biv_sort_cas(table_name=_biv_driver_detail,
        orderby=%str({"Variable", "Periodo", "Valor"}));

%mend _bivariado_compute;
