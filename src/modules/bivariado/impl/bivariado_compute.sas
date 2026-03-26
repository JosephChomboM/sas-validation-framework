/* =========================================================================
bivariado_compute.sas - Legacy-style compute for bivariado over consolidated
input.

Behavior preserved from bivariado_legacy.sas:
- numeric vars: TRAIN computes cuts, OOT reuses TRAIN cuts
- categorical vars: raw categories (no cuts)
- same variable can run twice if present in both numeric and categorical lists
- execution order: numeric list first, then categorical list

Outputs in work:
- _biv_main_report
- _biv_driver_report

Common columns:
Seccion, Run_Order, Tipo_Variable, Variable, Ventana, Ventana_Orden,
Valor_X, N, Pct_Cuentas, Defaults, RD
========================================================================= */

%macro _biv_init_result_table(table_name=);

    data work.&table_name.;
        length Seccion $12 Run_Order 8 Tipo_Variable $12 Variable $64
            Ventana $10 Ventana_Orden 8 Valor_X $200 N 8 Pct_Cuentas 8
            Defaults 8 RD 8;
        stop;
    run;

%mend _biv_init_result_table;

%macro _biv_calcular_cortes(tablain=, var=, groups=5, out_cuts=work.cortes);

    %local _rnd;
    %let _rnd=%sysfunc(int(%sysfunc(ranuni(0))*100000));

    data work._biv_cut_&_rnd._src;
        set &tablain.(keep=&var. where=(not missing(&var.)));
        &var.=round(&var., 0.0001);
    run;

    proc rank data=work._biv_cut_&_rnd._src out=work._biv_cut_&_rnd._rk
        groups=&groups.;
        ranks rango_ini;
        var &var.;
    run;

    proc sql;
        create table work._biv_cut_&_rnd._bins as
        select rango_ini,
               min(&var.) as minval,
               max(&var.) as maxval
        from work._biv_cut_&_rnd._rk
        group by rango_ini
        order by rango_ini;
    quit;

    data work._biv_cut_&_rnd._num;
        set work._biv_cut_&_rnd._bins end=eof;
        length ETIQUETA $200;
        retain prev_fin .;

        rango = rango_ini + 1;
        inicio = prev_fin;
        fin = maxval;
        flag_ini = (_n_ = 1);
        flag_fin = eof;

        if flag_ini then inicio = .;

        if flag_ini then
            ETIQUETA = cats(put(rango, z2.), '. <-Inf; ', strip(put(fin, f12.4)), ']');
        else if flag_fin then
            ETIQUETA = cats(put(rango, z2.), '. <', strip(put(inicio, f12.4)), '; +Inf>');
        else
            ETIQUETA = cats(put(rango, z2.), '. <', strip(put(inicio, f12.4)),
                            '; ', strip(put(fin, f12.4)), ']');

        prev_fin = fin;
        keep rango inicio fin flag_ini flag_fin ETIQUETA;
    run;

    data work._biv_cut_&_rnd._miss;
        length ETIQUETA $200;
        rango=0;
        inicio=.;
        fin=.;
        flag_ini=0;
        flag_fin=0;
        ETIQUETA='00. Missing';
    run;

    data &out_cuts.;
        set work._biv_cut_&_rnd._miss
            work._biv_cut_&_rnd._num;
    run;

    proc sort data=&out_cuts.;
        by rango;
    run;

    proc datasets library=work nolist nowarn;
        delete _biv_cut_&_rnd.:;
    quit;

%mend _biv_calcular_cortes;

%macro _biv_tendencia(tablain=, var=, target=, groups=5, flg_continue=1,
    reuse_cuts=0, m_data_type=, tipo_variable=, run_order=, section=,
    out_table=);

    %local _rnd _total _has_cuts;
    %let _rnd=%sysfunc(int(%sysfunc(ranuni(0))*100000));

    data work._biv_t_&_rnd._src;
        set &tablain.(keep=&var. &target.);

        %if &flg_continue.=1 %then %do;
            if &var. in (., 1111111111, -1111111111, 2222222222, -2222222222,
                3333333333, -3333333333, 4444444444, 5555555555, 6666666666,
                7777777777, -999999999) then &var.=.;
        %end;
    run;

    %let _total=0;
    proc sql noprint;
        select count(*) into :_total trimmed
        from work._biv_t_&_rnd._src;
    quit;

    %if %sysevalf(%superq(_total)=, boolean) %then %let _total=0;
    %if &_total.=0 %then %return;

    %if &flg_continue.=1 %then %do;

        %let _has_cuts=0;
        proc sql noprint;
            select count(*) into :_has_cuts trimmed
            from dictionary.tables
            where upcase(libname)='WORK'
              and upcase(memname)='CORTES';
        quit;

        %if &reuse_cuts.=0 or &_has_cuts.=0 %then %do;
            %_biv_calcular_cortes(tablain=work._biv_t_&_rnd._src,
                var=&var., groups=&groups., out_cuts=work.cortes);
        %end;

        proc sql;
            create table work._biv_t_&_rnd._tagged as
            select a.&target. as _target,
                   coalesce(b.ETIQUETA, '00. Missing') as Valor_X length=200
            from work._biv_t_&_rnd._src a
            left join work.cortes b
              on (missing(a.&var.) and b.rango=0)
              or (
                   not missing(a.&var.) and (
                        (b.flag_ini=1 and a.&var. <= b.fin)
                     or (b.flag_fin=1 and a.&var. > b.inicio)
                     or (b.flag_ini=0 and b.flag_fin=0
                         and a.&var. > b.inicio
                         and a.&var. <= b.fin)
                   )
                 );
        quit;

    %end;
    %else %do;

        data work._biv_t_&_rnd._tagged;
            set work._biv_t_&_rnd._src;
            length Valor_X $200;
            _target=&target.;

            if strip(cats(&var.))='' then Valor_X='00. Missing';
            else Valor_X=strip(cats(&var.));

            keep _target Valor_X;
        run;

    %end;

    proc sql;
        create table work._biv_t_&_rnd._agg as
        select Valor_X,
               count(*) as N,
               count(*) / &_total. as Pct_Cuentas format=percent8.2,
               sum(_target) as Defaults,
               mean(_target) as RD format=percent8.2
        from work._biv_t_&_rnd._tagged
        group by Valor_X
        order by Valor_X;
    quit;

    data work._biv_t_&_rnd._out;
        length Seccion $12 Tipo_Variable $12 Variable $64 Ventana $10;
        set work._biv_t_&_rnd._agg;

        Seccion="&section.";
        Tipo_Variable="&tipo_variable.";
        Variable="&var.";
        Ventana="&m_data_type.";
        Run_Order=&run_order.;

        if upcase(Ventana)='TRAIN' then Ventana_Orden=1;
        else if upcase(Ventana)='OOT' then Ventana_Orden=2;
        else Ventana_Orden=9;

        keep Seccion Run_Order Tipo_Variable Variable Ventana Ventana_Orden
            Valor_X N Pct_Cuentas Defaults RD;
    run;

    proc append base=work.&out_table. data=work._biv_t_&_rnd._out force;
    quit;

    proc datasets library=work nolist nowarn;
        delete _biv_t_&_rnd._src _biv_t_&_rnd._tagged _biv_t_&_rnd._agg
               _biv_t_&_rnd._out;
    quit;

%mend _biv_tendencia;

%macro _biv_trend_variables(train_data=, oot_data=, target=, vars_num=,
    vars_cat=, groups=5, section=, out_table=);

    %local _list _c _v _z _v_cat _v_aux _run_order;

    %let _list=%superq(vars_num);

    %let _z=1;
    %let _v_cat=%scan(%superq(vars_cat), &_z., %str( ));
    %do %while(%length(%superq(_v_cat)) > 0);
        %let _list=&_list. &_v_cat.#;
        %let _z=%eval(&_z. + 1);
        %let _v_cat=%scan(%superq(vars_cat), &_z., %str( ));
    %end;

    %let _run_order=0;
    %let _c=1;
    %let _v=%scan(%superq(_list), &_c., %str( ));

    %do %while(%length(%superq(_v)) > 0);

        %if %substr(%superq(_v), 1, 1) ne %str(.) %then %do;

            %if %substr(%superq(_v), %length(%superq(_v)), 1)=# %then %do;
                %let _v_aux=%substr(%superq(_v), 1,
                    %eval(%length(%superq(_v)) - 1));

                %let _run_order=%eval(&_run_order. + 1);

                %_biv_tendencia(tablain=&train_data., var=&_v_aux.,
                    target=&target., groups=&groups., flg_continue=0,
                    reuse_cuts=0, m_data_type=TRAIN,
                    tipo_variable=CATEGORICA, run_order=&_run_order.,
                    section=&section., out_table=&out_table.);

                %if %sysfunc(exist(&oot_data.)) %then %do;
                    %_biv_tendencia(tablain=&oot_data., var=&_v_aux.,
                        target=&target., groups=&groups., flg_continue=0,
                        reuse_cuts=0, m_data_type=OOT,
                        tipo_variable=CATEGORICA, run_order=&_run_order.,
                        section=&section., out_table=&out_table.);
                %end;
            %end;
            %else %do;
                %let _run_order=%eval(&_run_order. + 1);

                %_biv_tendencia(tablain=&train_data., var=&_v.,
                    target=&target., groups=&groups., flg_continue=1,
                    reuse_cuts=0, m_data_type=TRAIN,
                    tipo_variable=NUMERICA, run_order=&_run_order.,
                    section=&section., out_table=&out_table.);

                %if %sysfunc(exist(&oot_data.)) %then %do;
                    %_biv_tendencia(tablain=&oot_data., var=&_v.,
                        target=&target., groups=&groups., flg_continue=1,
                        reuse_cuts=1, m_data_type=OOT,
                        tipo_variable=NUMERICA, run_order=&_run_order.,
                        section=&section., out_table=&out_table.);

                    proc datasets library=work nolist nowarn;
                        delete cortes;
                    quit;
                %end;
            %end;

        %end;

        %let _c=%eval(&_c. + 1);
        %let _v=%scan(%superq(_list), &_c., %str( ));
    %end;

%mend _biv_trend_variables;

%macro _bivariado_compute(source_data=casuser._biv_input,
    train_data=casuser._biv_train, target=, byvar=, vars_num=, vars_cat=,
    dri_num=, dri_cat=, groups=5);

    data work._biv_source;
        set &source_data.;
    run;

    data work._biv_train;
        set &train_data.;
    run;

    data work._biv_oot;
        set work._biv_source;
        where upcase(_biv_period)='OOT';
    run;

    %_biv_init_result_table(table_name=_biv_main_report);
    %_biv_init_result_table(table_name=_biv_driver_report);

    %_biv_trend_variables(train_data=work._biv_train,
        oot_data=work._biv_oot, target=&target., vars_num=&vars_num.,
        vars_cat=&vars_cat., groups=&groups., section=PRINCIPAL,
        out_table=_biv_main_report);

    %if %length(%superq(dri_num)) > 0 or %length(%superq(dri_cat)) > 0 %then %do;
        %_biv_trend_variables(train_data=work._biv_train,
            oot_data=work._biv_oot, target=&target., vars_num=&dri_num.,
            vars_cat=&dri_cat., groups=&groups., section=DRIVER,
            out_table=_biv_driver_report);
    %end;

    proc sort data=work._biv_main_report;
        by Run_Order Ventana_Orden Valor_X;
    run;

    proc sort data=work._biv_driver_report;
        by Run_Order Ventana_Orden Valor_X;
    run;

    proc datasets library=work nolist nowarn;
        delete _biv_source _biv_train _biv_oot;
    quit;

%mend _bivariado_compute;
