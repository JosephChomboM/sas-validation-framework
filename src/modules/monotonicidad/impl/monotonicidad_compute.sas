/* =========================================================================
monotonicidad_compute.sas - Compute legacy-style para METOD7 sobre scope_input

Behavior preservado desde monotonicidad_legacy.sas:
- monotonicidad aplica a una sola variable PD
- TRAIN calcula cortes
- OOT reutiliza exactamente esos cortes
- la asignacion de buckets se hace con codigo secuencial tipo DATAAPPLY

Outputs finales en casuser:
- _mono_input
- _mono_train
- _mono_oot
- _mono_cuts
- _mono_report_train
- _mono_report_oot
- _mono_detail
========================================================================= */

%macro _mono_partition_cas(table_name=, orderby=, groupby={});

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

%mend _mono_partition_cas;

%macro _mono_prepare_input(input_caslib=, input_table=, score_var=, target=,
    byvar=, def_cld=, train_min_mes=, train_max_mes=, oot_min_mes=,
    oot_max_mes=);

    data casuser._mono_input;
        length Split $5;
        set &input_caslib..&input_table.(keep=&score_var. &target. &byvar.);

        _mono_score=&score_var.;
        _mono_target=&target.;
        _mono_byvar=&byvar.;

        if _mono_byvar <= &def_cld. then do;
            if _mono_byvar >= &train_min_mes. and _mono_byvar <= &train_max_mes.
            then do;
                Split='TRAIN';
                Split_Order=1;
                output;
            end;
            else if _mono_byvar >= &oot_min_mes.
                and _mono_byvar <= &oot_max_mes. then do;
                Split='OOT';
                Split_Order=2;
                output;
            end;
        end;

        keep Split Split_Order _mono_score _mono_target _mono_byvar;
    run;

%mend _mono_prepare_input;

%macro _mono_prepare_splits(input_table=casuser._mono_input);

    data casuser._mono_train;
        set &input_table.;
        where Split='TRAIN';
        keep _mono_score _mono_target;
    run;

    data casuser._mono_oot;
        set &input_table.;
        where Split='OOT';
        keep _mono_score _mono_target;
    run;

%mend _mono_prepare_splits;

%macro _mono_clear_work();

    proc datasets library=work nolist nowarn;
        delete _mono_: cortes;
    quit;

%mend _mono_clear_work;

%macro _mono_calcular_cortes(tablain=casuser._mono_train, groups=5,
    out_cuts=casuser._mono_cuts);

    %local _grp _cuts_name;
    %let _grp=&groups.;
    %let _cuts_name=%scan(%superq(out_cuts), -1, .);

    %if %length(%superq(_grp))=0 %then %let _grp=5;
    %if %sysevalf(&_grp. < 1) %then %let _grp=5;

    data work._mono_cut_src;
        set &tablain.;

        _mono_score=input(put(_mono_score, F12.4), ?? best32.);
    run;

    proc rank data=work._mono_cut_src out=work._mono_cut_ranked
        groups=&_grp.;
        ranks RANGO;
        var _mono_score;
    run;

    proc sql;
        create table work._mono_cut_bins as
        select RANGO,
               min(_mono_score) as MINVAL,
               max(_mono_score) as MAXVAL
        from work._mono_cut_ranked
        group by RANGO
        order by RANGO;
    quit;

    data work._mono_cuts;
        set work._mono_cut_bins(rename=(RANGO=RANGO_INI)) end=EOF;
        length VARIABLE $32 ETIQUETA $200;
        retain MARCA 0;

        N=_n_;
        FLAG_INI=0;
        FLAG_FIN=0;
        LAGMAXVAL=lag(MAXVAL);
        RANGO=RANGO_INI+1;
        if RANGO_INI=. then RANGO=0;
        if RANGO_INI>=0 then MARCA=MARCA+1;
        if MARCA=1 then FLAG_INI=1;
        if EOF then FLAG_FIN=1;

        VARIABLE='_mono_score';
        INICIO=LAGMAXVAL;
        FIN=MAXVAL;

        if RANGO=0 then
            ETIQUETA='00. Missing';
        else if FLAG_INI=1 then
            ETIQUETA=cats(put(RANGO, z2.), '. <-Inf; ',
                cats(put(MAXVAL, F12.4)), ']');
        else if FLAG_FIN=1 then
            ETIQUETA=cats(put(RANGO, z2.), '. <',
                cats(put(LAGMAXVAL, F12.4)), '; +Inf>');
        else
            ETIQUETA=cats(put(RANGO, z2.), '. <',
                cats(put(LAGMAXVAL, F12.4)), '; ',
                cats(put(MAXVAL, F12.4)), ']');

        keep VARIABLE RANGO RANGO_INI INICIO FIN FLAG_INI FLAG_FIN ETIQUETA;
    run;

    data &out_cuts.;
        set work._mono_cuts;
    run;

    %_mono_partition_cas(table_name=&_cuts_name., orderby=%str({"RANGO"}));

    proc datasets library=work nolist nowarn;
        delete _mono_cut_src _mono_cut_ranked _mono_cut_bins;
    quit;

%mend _mono_calcular_cortes;

%macro _mono_build_apply_code(cuts_table=work.cortes,
    outvar=_mono_dataapply);

    data work._mono_cut_apply;
        set &cuts_table.;
        length QUERY_BODY $600;

        if RANGO=0 then
            QUERY_BODY=cats(
                'IF MISSING(_mono_score)=1 THEN DO; ETIQUETA=',
                quote(trim(strip(ETIQUETA))),
                '; Bucket_Order=0; END;'
            );
        else if FLAG_INI=1 then
            QUERY_BODY=cats(
                'IF _mono_score<=',
                strip(put(FIN, best32.)),
                ' THEN DO; ETIQUETA=',
                quote(trim(strip(ETIQUETA))),
                '; Bucket_Order=',
                strip(put(RANGO, best32.)),
                '; END;'
            );
        else if FLAG_FIN=1 then
            QUERY_BODY=cats(
                'IF _mono_score>',
                strip(put(INICIO, best32.)),
                ' THEN DO; ETIQUETA=',
                quote(trim(strip(ETIQUETA))),
                '; Bucket_Order=',
                strip(put(RANGO, best32.)),
                '; END;'
            );
        else
            QUERY_BODY=cats(
                'IF ',
                strip(put(INICIO, best32.)),
                '<_mono_score<=',
                strip(put(FIN, best32.)),
                ' THEN DO; ETIQUETA=',
                quote(trim(strip(ETIQUETA))),
                '; Bucket_Order=',
                strip(put(RANGO, best32.)),
                '; END;'
            );
    run;

    proc sql noprint;
        select QUERY_BODY
          into :&outvar. separated by ' '
        from work._mono_cut_apply
        order by RANGO;
    quit;

    proc datasets library=work nolist nowarn;
        delete _mono_cut_apply;
    quit;

%mend _mono_build_apply_code;

%macro _mono_run_pass(tablain=, split_label=, split_order=, exist_cuts=0,
    groups=5, cuts_table=casuser._mono_cuts, out_table=);

    %local _mono_total _mono_dataapply _report_name;

    %let _mono_total=0;
    %let _report_name=%scan(%superq(out_table), -1, .);

    data work._mono_pass_src;
        set &tablain.;
    run;

    data _null_;
        if 0 then set work._mono_pass_src nobs=n;
        call symputx('_mono_total', n);
        stop;
    run;

    %if %sysevalf(%superq(_mono_total)=, boolean) %then %let _mono_total=0;
    %if &_mono_total.=0 %then %return;

    %if &exist_cuts.=0 %then %do;
        %_mono_calcular_cortes(tablain=work._mono_pass_src, groups=&groups.,
            out_cuts=&cuts_table.);
        proc sort data=work.cortes;
            by RANGO;
        run;
    %end;
    %else %if &exist_cuts.=1 %then %do;
        %put NOTE: [monotonicidad_compute] Se usan cortes previos hechos en TRAIN.;
    %end;
    %else %do;
        %put ERROR: [monotonicidad_compute] exist_cuts debe ser 0 o 1.;
        %return;
    %end;

    %_mono_build_apply_code(outvar=_mono_dataapply);

    data work._mono_tagged;
        length ETIQUETA $200;
        set work._mono_pass_src;

        Bucket_Order=.;
        ETIQUETA='';
        &_mono_dataapply.;

        keep Bucket_Order ETIQUETA _mono_target;
    run;

    proc sql;
        create table work._mono_report as
        select "&split_label." as Split length=5,
               &split_order. as Split_Order,
               Bucket_Order,
               ETIQUETA as Valor_X length=200,
               count(*) as N,
               count(*) / &_mono_total. as Pct_Cuentas format=percent8.2,
               mean(_mono_target) as Mean_Default format=percent8.2
        from work._mono_tagged
        group by Bucket_Order, ETIQUETA
        order by Bucket_Order, ETIQUETA;
    quit;

    data &out_table.;
        set work._mono_report;
    run;

    %_mono_partition_cas(table_name=&_report_name.,
        orderby=%str({"Bucket_Order"}));

    proc datasets library=work nolist nowarn;
        delete _mono_pass_src _mono_tagged _mono_report;
    quit;

    %if &exist_cuts.=1 %then %do;
        proc datasets library=work nolist nowarn;
            delete cortes;
        quit;
    %end;

%mend _mono_run_pass;

%macro _monotonicidad_compute(input_caslib=, input_table=, score_var=, target=,
    byvar=, def_cld=, groups=5, train_min_mes=, train_max_mes=, oot_min_mes=,
    oot_max_mes=);
    %_mono_clear_work();
    %put NOTE: [monotonicidad_compute] Construyendo input desde _scope_input.;

    %_mono_prepare_input(input_caslib=&input_caslib., input_table=&input_table.,
        score_var=&score_var., target=&target., byvar=&byvar.,
        def_cld=&def_cld., train_min_mes=&train_min_mes.,
        train_max_mes=&train_max_mes., oot_min_mes=&oot_min_mes.,
        oot_max_mes=&oot_max_mes.);

    %_mono_prepare_splits(input_table=casuser._mono_input);

    %put NOTE: [monotonicidad_compute] Ejecutando TRAIN y calculando cortes.;
    %_mono_run_pass(tablain=casuser._mono_train, split_label=TRAIN,
        split_order=1, exist_cuts=0, groups=&groups.,
        cuts_table=casuser._mono_cuts, out_table=casuser._mono_report_train);

    %put NOTE: [monotonicidad_compute] Ejecutando OOT reutilizando cortes TRAIN.;
    %_mono_run_pass(tablain=casuser._mono_oot, split_label=OOT,
        split_order=2, exist_cuts=1, groups=&groups.,
        cuts_table=casuser._mono_cuts, out_table=casuser._mono_report_oot);

    data casuser._mono_detail;
        set casuser._mono_report_train
            casuser._mono_report_oot;
    run;

    %_mono_partition_cas(table_name=_mono_detail,
        orderby=%str({"Split_Order", "Bucket_Order"}));

    %_mono_clear_work();

%mend _monotonicidad_compute;
