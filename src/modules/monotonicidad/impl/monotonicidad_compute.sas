/* =========================================================================
monotonicidad_compute.sas - Computo CAS-first de monotonicidad (METOD7)

Monotonicidad trabaja sobre una sola variable score (PD), derivando TRAIN y
OOT dentro del modulo sobre un input unificado. Los cortes se calculan con
TRAIN y luego se aplican a ambos splits. El detalle final queda en CAS y el
ordenamiento solo se realiza al final con table.partition.
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

    data casuser._mono_input_raw;
        set &input_caslib..&input_table.;

        _mono_score=input(vvaluex("&score_var."), ?? best32.);
        if _mono_score in (., 1111111111, -1111111111, 2222222222, -2222222222,
            3333333333, -3333333333, 4444444444, 5555555555, 6666666666,
            7777777777, -999999999) then _mono_score=.;
        else _mono_score=round(_mono_score, 0.0001);

        _mono_target=input(vvaluex("&target."), ?? best32.);
        _mono_byvar=input(vvaluex("&byvar."), ?? best32.);

        keep _mono_score _mono_target _mono_byvar;
    run;

    proc fedsql sessref=conn;
        create table casuser._mono_input {options replace=true} as
        select case
                   when _mono_byvar >= &train_min_mes.
                    and _mono_byvar <= &train_max_mes.
                   then 'TRAIN'
                   else 'OOT'
               end as Split,
               case
                   when _mono_byvar >= &train_min_mes.
                    and _mono_byvar <= &train_max_mes.
                   then 1
                   else 2
               end as Split_Order,
               _mono_score,
               _mono_target
        from casuser._mono_input_raw
        where _mono_byvar <= &def_cld.
          and (
                (_mono_byvar >= &train_min_mes.
                 and _mono_byvar <= &train_max_mes.)
                or
                (_mono_byvar >= &oot_min_mes.
                 and _mono_byvar <= &oot_max_mes.)
              );
    quit;

%mend _mono_prepare_input;

%macro _mono_prepare_splits(input_table=casuser._mono_input);

    proc fedsql sessref=conn;
        create table casuser._mono_train {options replace=true} as
        select _mono_score, _mono_target
        from &input_table.
        where Split='TRAIN';
    quit;

    proc fedsql sessref=conn;
        create table casuser._mono_oot {options replace=true} as
        select _mono_score, _mono_target
        from &input_table.
        where Split='OOT';
    quit;

%mend _mono_prepare_splits;

%macro _mono_calcular_cortes(tablain=casuser._mono_train, groups=5,
    out_cuts=casuser._mono_cuts);

    %local _grp _cuts_name;
    %let _grp=&groups.;
    %let _cuts_name=%scan(%superq(out_cuts), -1, .);
    %if %length(%superq(_grp))=0 %then %let _grp=5;
    %if %sysevalf(&_grp. < 1) %then %let _grp=5;

    proc rank data=&tablain.
        out=casuser._mono_ranked
        groups=&_grp.;
        var _mono_score;
        ranks rango;
    run;

    proc fedsql sessref=conn;
        create table casuser._mono_bins {options replace=true} as
        select case
                   when rango is null then -1
                   else rango
               end as Sort_Rango,
               rango as Rango_Ini,
               min(_mono_score) as minval,
               max(_mono_score) as maxval
        from casuser._mono_ranked
        group by rango;
    quit;

    %_mono_partition_cas(table_name=_mono_bins,
        orderby=%str({"Sort_Rango"}));

    data &out_cuts.;
        set casuser._mono_bins end=eof;
        length Valor_X $200 VARIABLE $32;
        retain MARCA 0;

        N=_n_;
        FLAG_INI=0;
        FLAG_FIN=0;
        LAGMAXVAL=lag(MAXVAL);
        BUCKET_ORDER=RANGO_INI+1;
        if RANGO_INI=. then BUCKET_ORDER=0;
        if RANGO_INI>=0 then MARCA=MARCA+1;
        if MARCA=1 then FLAG_INI=1;
        if EOF then FLAG_FIN=1;

        VARIABLE='_mono_score';
        INICIO=LAGMAXVAL;
        FIN=MAXVAL;

        if BUCKET_ORDER=0 then
            Valor_X='00. Missing';
        else if FLAG_INI=1 then
            Valor_X=cats(put(BUCKET_ORDER, z2.), '. <-Inf; ',
                cats(put(MAXVAL, F12.4)), ']');
        else if FLAG_FIN=1 then
            Valor_X=cats(put(BUCKET_ORDER, z2.), '. <',
                cats(put(LAGMAXVAL, F12.4)), '; +Inf>');
        else
            Valor_X=cats(put(BUCKET_ORDER, z2.), '. <',
                cats(put(LAGMAXVAL, F12.4)), '; ',
                cats(put(MAXVAL, F12.4)), ']');

        keep VARIABLE BUCKET_ORDER RANGO_INI INICIO FIN FLAG_INI FLAG_FIN
             Valor_X SORT_RANGO;
    run;

    %_mono_partition_cas(table_name=&_cuts_name.,
        orderby=%str({"Bucket_Order"}));

%mend _mono_calcular_cortes;

%macro _mono_build_apply_code(cuts_table=casuser._mono_cuts,
    outvar=_mono_dataapply);

    data work._mono_cut_apply;
        set &cuts_table.;
        length query_body $500;

        if Bucket_Order = 0 then
            query_body = cats(
                "if missing(_mono_score)=1 then do; Valor_X='",
                strip(Valor_X),
                "'; Bucket_Order=0; end;"
            );
        else if flag_ini = 1 then
            query_body = cats(
                "if _mono_score<=",
                strip(put(fin, best32.)),
                " then do; Valor_X='",
                strip(Valor_X),
                "'; Bucket_Order=",
                strip(put(Bucket_Order, best32.)),
                "; end;"
            );
        else if flag_fin = 1 then
            query_body = cats(
                "if _mono_score>",
                strip(put(inicio, best32.)),
                " then do; Valor_X='",
                strip(Valor_X),
                "'; Bucket_Order=",
                strip(put(Bucket_Order, best32.)),
                "; end;"
            );
        else
            query_body = cats(
                "if ",
                strip(put(inicio, best32.)),
                "<_mono_score<=",
                strip(put(fin, best32.)),
                " then do; Valor_X='",
                strip(Valor_X),
                "'; Bucket_Order=",
                strip(put(Bucket_Order, best32.)),
                "; end;"
            );
    run;

    proc sql noprint;
        select query_body into :&outvar. separated by ' '
        from work._mono_cut_apply
        order by Bucket_Order;
    quit;

    proc datasets library=work nolist nowarn;
        delete _mono_cut_apply;
    quit;

%mend _mono_build_apply_code;

%macro _mono_run_pass(tablain=, split_label=, split_order=, exist_cuts=0,
    groups=5, cuts_table=casuser._mono_cuts, out_table=);

    %local _mono_dataapply _mono_total;
    %let _mono_total=0;

    proc sql noprint;
        select count(*) into :_mono_total trimmed
        from &tablain.;
    quit;

    %if %sysevalf(%superq(_mono_total)=, boolean) %then %let _mono_total=0;
    %if &_mono_total.=0 %then %return;

    %if &exist_cuts.=0 %then %do;
        %_mono_calcular_cortes(tablain=&tablain., groups=&groups.,
            out_cuts=&cuts_table.);
    %end;
    %else %if &exist_cuts.=1 %then %do;
        %put NOTE: [monotonicidad_compute] Reutilizando cortes TRAIN para
            &split_label..;
    %end;
    %else %do;
        %put ERROR: [monotonicidad_compute] exist_cuts debe ser 0 o 1.;
        %return;
    %end;

    %_mono_build_apply_code(cuts_table=&cuts_table., outvar=_mono_dataapply);

    data casuser._mono_tagged_&split_order.;
        length Valor_X $200;
        set &tablain.;

        Bucket_Order=.;
        Valor_X=' ';
        &_mono_dataapply.;

        if missing(Bucket_Order) then Bucket_Order=0;
        if strip(Valor_X)='' then Valor_X='00. Missing';

        keep Bucket_Order Valor_X _mono_target;
    run;

    proc fedsql sessref=conn;
        create table &out_table. {options replace=true} as
        select "&split_label." as Split,
               &split_order. as Split_Order,
               Bucket_Order,
               Valor_X,
               count(*) as N,
               count(*) / &_mono_total. as Pct_Cuentas,
               avg(_mono_target) as Mean_Default
        from casuser._mono_tagged_&split_order.
        group by Bucket_Order, Valor_X;
    quit;

    %_mono_partition_cas(table_name=%scan(%superq(out_table), -1, .),
        orderby=%str({"Bucket_Order"}));

%mend _mono_run_pass;

%macro _monotonicidad_compute(input_caslib=, input_table=, score_var=, target=,
    byvar=, def_cld=, groups=5, train_min_mes=, train_max_mes=, oot_min_mes=,
    oot_max_mes=);

    %put NOTE: [monotonicidad_compute] Construyendo input canonico en CAS.;

    %_mono_prepare_input(input_caslib=&input_caslib., input_table=&input_table.,
        score_var=&score_var., target=&target., byvar=&byvar.,
        def_cld=&def_cld., train_min_mes=&train_min_mes.,
        train_max_mes=&train_max_mes., oot_min_mes=&oot_min_mes.,
        oot_max_mes=&oot_max_mes.);

    %_mono_prepare_splits(input_table=casuser._mono_input);

    %put NOTE: [monotonicidad_compute] Ejecutando TRAIN con calculo de cortes.;
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
        Run_Order=1;
    run;

    %_mono_partition_cas(table_name=_mono_detail,
        orderby=%str({"Run_Order", "Split_Order", "Bucket_Order"}));

%mend _monotonicidad_compute;
