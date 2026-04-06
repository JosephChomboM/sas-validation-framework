/* =========================================================================
monotonicidad_compute.sas - Compute legacy-style para METOD7

Diseno:
- Entrada unica desde _scope_input en CAS
- Filtro previo por def_cld
- Derivacion interna de TRAIN/OOT
- Calculo principal en WORK replicando monotonicidad_legacy.sas
- Publicacion final a CAS solo de tablas de salida
========================================================================= */
%macro _mono_clear_work;
    proc datasets library=work nolist nowarn;
        delete _mono_: cortes;
    quit;
%mend _mono_clear_work;

%macro _mono_prepare_input(input_caslib=, input_table=, score_var=, target=,
    byvar=, def_cld=, train_min_mes=, train_max_mes=, oot_min_mes=,
    oot_max_mes=);

    data casuser._mono_input;
        set &input_caslib..&input_table.(keep=&score_var. &target. &byvar.);
        length Split $5;

        _mono_score=&score_var.;
        _mono_target=&target.;
        _mono_byvar=&byvar.;

        if missing(_mono_byvar) then delete;
        if _mono_byvar > &def_cld. then delete;

        if _mono_byvar >= &train_min_mes.
            and _mono_byvar <= &train_max_mes. then do;
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

        keep Split Split_Order _mono_score _mono_target _mono_byvar;
    run;

%mend _mono_prepare_input;

%macro _mono_prepare_splits;

    data casuser._mono_train casuser._mono_oot;
        set casuser._mono_input;
        if Split='TRAIN' then output casuser._mono_train;
        else if Split='OOT' then output casuser._mono_oot;
    run;

    data work._mono_train;
        set casuser._mono_train;
    run;

    data work._mono_oot;
        set casuser._mono_oot;
    run;

%mend _mono_prepare_splits;

%macro _mono_calcular_cortes_legacy(tablain=, var=, groups=5);

    data work._mono_cut_1;
        set &tablain.;
        &var. = put(&var., F12.4);
    run;

    proc rank data=work._mono_cut_1 out=work._mono_cut_2 groups=&groups.;
        ranks RANGO;
        var &var.;
    run;

    proc sql;
        create table work._mono_cut_3 as
        select RANGO,
               min(&var.) as MINVAL,
               max(&var.) as MAXVAL
        from work._mono_cut_2
        group by RANGO;
    quit;

    proc sort data=work._mono_cut_3;
        by RANGO;
    run;

    data work._mono_cut_4;
        set work._mono_cut_3(rename=(RANGO=RANGO_INI)) end=EOF;
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
    run;

    proc sql;
        create table work.cortes as
        select "&var." as VARIABLE length=32,
               RANGO,
               RANGO_INI,
               LAGMAXVAL as INICIO,
               MAXVAL as FIN,
               FLAG_INI,
               FLAG_FIN,
               case
                   when RANGO=0 then "00. Missing"
                   when FLAG_INI=1 then
                       cat(put(RANGO, z2.), ". <-Inf; ",
                           cats(put(MAXVAL, F12.4)), "]")
                   when FLAG_FIN=1 then
                       cat(put(RANGO, z2.), ". <",
                           cats(put(LAGMAXVAL, F12.4)), "; +Inf>")
                   else
                       cat(put(RANGO, z2.), ". <",
                           cats(put(LAGMAXVAL, F12.4)), "; ",
                           cats(put(MAXVAL, F12.4)), "]")
               end as ETIQUETA length=200
        from work._mono_cut_4;
    quit;

%mend _mono_calcular_cortes_legacy;

%macro _mono_build_apply_code(var=);
    %global _mono_dataapply;
    %let _mono_dataapply=;

    data work._mono_apply_1;
        set work.cortes end=EOF;
        length QUERY_START $35 QUERY_END $60;
        N=_n_;
        QUERY_START="WHEN ";
        QUERY_END="";
        if N=1 then QUERY_START="CASE WHEN ";
        if EOF then QUERY_END=" END";
    run;

    proc sql;
        create table work._mono_apply_2 as
        select *,
            case
                when RANGO=0 then
                    cat("IF MISSING(&var.)=1 THEN ETIQUETA=",
                        '"', strip(ETIQUETA), '";')
                when FLAG_INI=1 then
                    cat("IF &var.<=", FIN, " THEN ETIQUETA=",
                        '"', strip(ETIQUETA), '";')
                when FLAG_FIN=1 then
                    cat("IF &var.>", INICIO, " THEN ETIQUETA=",
                        '"', strip(ETIQUETA), '";')
                else
                    cat("IF ", INICIO, "<&var.<=", FIN, " THEN ETIQUETA=",
                        '"', strip(ETIQUETA), '";')
            end as QUERY_BODY
        from work._mono_apply_1;
    quit;

    proc sql noprint;
        select QUERY_BODY into :_mono_dataapply separated by " "
        from work._mono_apply_2;
    quit;

%mend _mono_build_apply_code;

%macro _mono_run_legacy(tablain=, split_label=, split_order=, exist_cuts=0,
    out_table=, score_var=_mono_score, target=_mono_target, groups=5);

    %local _mono_total;
    %let _mono_total=0;

    data work._mono_pass_0;
        set &tablain.;
    run;

    data _null_;
        if 0 then set work._mono_pass_0 nobs=n;
        call symputx('_mono_total', n);
        stop;
    run;

    %if &exist_cuts.=0 %then %do;
        %_mono_calcular_cortes_legacy(tablain=work._mono_pass_0,
            var=&score_var., groups=&groups.);
        proc sort data=work.cortes;
            by RANGO;
        run;
    %end;
    %else %if &exist_cuts.=1 %then %do;
        %put NOTE: [monotonicidad_compute] Reutilizando cortes de TRAIN
            para &split_label..;
    %end;
    %else %do;
        %put ERROR: [monotonicidad_compute] exist_cuts debe ser 0 o 1.;
        %return;
    %end;

    %_mono_build_apply_code(var=&score_var.);

    data work._mono_tagged;
        set work._mono_pass_0;
        length ETIQUETA $200;
        &_mono_dataapply.;
    run;

    proc sql;
        create table work._mono_report_raw as
        select ETIQUETA,
               count(*) as N,
               count(*) / &_mono_total. as Pct_Cuentas format=percent8.2,
               mean(&target.) as Mean_Default format=percent8.2
        from work._mono_tagged
        group by ETIQUETA
        order by ETIQUETA;
    quit;

    data &out_table.;
        length Split $5 Valor_X $200;
        set work._mono_report_raw(rename=(ETIQUETA=Valor_X));
        Split="&split_label.";
        Split_Order=&split_order.;
        Bucket_Order=input(scan(Valor_X, 1, '.'), best.);
        if missing(Bucket_Order) then Bucket_Order=0;
        format Pct_Cuentas percent8.2 Mean_Default percent8.2;
    run;

    proc sort data=&out_table.;
        by Split_Order Bucket_Order;
    run;

%mend _mono_run_legacy;

%macro _monotonicidad_compute(input_caslib=, input_table=, score_var=,
    target=, byvar=, def_cld=, groups=5, train_min_mes=, train_max_mes=,
    oot_min_mes=, oot_max_mes=);

    %put NOTE: [monotonicidad_compute] Inicio compute legacy-wrapper.;
    %put NOTE: [monotonicidad_compute] score_var=&score_var. target=&target.
        byvar=&byvar. def_cld=&def_cld.;

    %_mono_clear_work;

    proc datasets library=casuser nolist nowarn;
        delete _mono_input _mono_train _mono_oot _mono_report_train
            _mono_report_oot _mono_detail;
    quit;

    %_mono_prepare_input(input_caslib=&input_caslib., input_table=&input_table.,
        score_var=&score_var., target=&target., byvar=&byvar.,
        def_cld=&def_cld., train_min_mes=&train_min_mes.,
        train_max_mes=&train_max_mes., oot_min_mes=&oot_min_mes.,
        oot_max_mes=&oot_max_mes.);

    %_mono_prepare_splits;

    %_mono_run_legacy(tablain=work._mono_train, split_label=TRAIN,
        split_order=1, exist_cuts=0, out_table=work._mono_report_train,
        score_var=_mono_score, target=_mono_target, groups=&groups.);

    %_mono_run_legacy(tablain=work._mono_oot, split_label=OOT, split_order=2,
        exist_cuts=1, out_table=work._mono_report_oot, score_var=_mono_score,
        target=_mono_target, groups=&groups.);

    data work._mono_detail;
        set work._mono_report_train work._mono_report_oot;
    run;

    proc sort data=work._mono_detail;
        by Split_Order Bucket_Order;
    run;

    data casuser._mono_report_train;
        set work._mono_report_train;
    run;

    data casuser._mono_report_oot;
        set work._mono_report_oot;
    run;

    data casuser._mono_detail;
        set work._mono_detail;
    run;

    %_mono_clear_work;

    %put NOTE: [monotonicidad_compute] Fin compute legacy-wrapper.;

%mend _monotonicidad_compute;
