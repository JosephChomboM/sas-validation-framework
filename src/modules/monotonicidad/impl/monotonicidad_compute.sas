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
        set &input_caslib..&input_table.(keep=&score_var. &target. &byvar.);

        _mono_score=&score_var.;
        if _mono_score in (., 1111111111, -1111111111, 2222222222, -2222222222,
            3333333333, -3333333333, 4444444444, 5555555555, 6666666666,
            7777777777, -999999999) then _mono_score=.;
        else _mono_score=round(_mono_score, 0.0001);

        _mono_target=&target.;
        _mono_byvar=&byvar.;

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

%macro _mono_calcular_cortes(train_table=casuser._mono_input, groups=5,
    out_cuts=casuser._mono_cuts);

    %local _grp;
    %let _grp=&groups.;
    %if %length(%superq(_grp))=0 %then %let _grp=5;
    %if %sysevalf(&_grp. < 1) %then %let _grp=5;

    proc rank data=&train_table.(where=(Split='TRAIN' and not missing(_mono_score)))
        out=casuser._mono_ranked
        groups=&_grp.;
        var _mono_score;
        ranks rango_ini;
    run;

    proc fedsql sessref=conn;
        create table casuser._mono_bins {options replace=true} as
        select rango_ini,
               min(_mono_score) as minval,
               max(_mono_score) as maxval
        from casuser._mono_ranked
        group by rango_ini
        order by rango_ini;
    quit;

    data casuser._mono_cuts_num;
        set casuser._mono_bins end=eof;
        length Valor_X $200;
        retain prev_fin .;

        Bucket_Order = rango_ini + 1;
        inicio = prev_fin;
        fin = maxval;
        flag_ini = (_n_ = 1);
        flag_fin = eof;

        if flag_ini then inicio = .;

        if flag_ini then
            Valor_X = cats(put(Bucket_Order, z2.), '. <-Inf; ',
                strip(put(fin, best12.4)), ']');
        else if flag_fin then
            Valor_X = cats(put(Bucket_Order, z2.), '. <',
                strip(put(inicio, best12.4)), '; +Inf>');
        else
            Valor_X = cats(put(Bucket_Order, z2.), '. <',
                strip(put(inicio, best12.4)), '; ',
                strip(put(fin, best12.4)), ']');

        prev_fin = fin;
        keep Bucket_Order inicio fin flag_ini flag_fin Valor_X;
    run;

    data casuser._mono_cuts_missing;
        length Valor_X $200;
        Bucket_Order=0;
        inicio=.;
        fin=.;
        flag_ini=0;
        flag_fin=0;
        Valor_X='00. Missing';
    run;

    data &out_cuts.;
        set casuser._mono_cuts_missing
            casuser._mono_cuts_num;
    run;

%mend _mono_calcular_cortes;

%macro _mono_build_detail(input_table=casuser._mono_input,
    cuts_table=casuser._mono_cuts, out_table=casuser._mono_detail);

    proc fedsql sessref=conn;
        create table casuser._mono_split_totals {options replace=true} as
        select Split,
               Split_Order,
               count(*) as Total_Split
        from &input_table.
        group by Split, Split_Order;
    quit;

    proc fedsql sessref=conn;
        create table casuser._mono_tagged {options replace=true} as
        select a.Split,
               a.Split_Order,
               coalesce(b.Bucket_Order, 0) as Bucket_Order,
               coalesce(b.Valor_X, '00. Missing') as Valor_X,
               a._mono_target
        from &input_table. a
        left join &cuts_table. b
          on (missing(a._mono_score) and b.Bucket_Order = 0)
          or (
               not missing(a._mono_score)
               and (
                    (b.flag_ini = 1 and b.Bucket_Order = 1
                        and a._mono_score <= b.fin)
                 or (b.flag_fin = 1 and a._mono_score > b.inicio)
                 or (b.flag_ini = 0 and b.flag_fin = 0
                     and a._mono_score > b.inicio
                     and a._mono_score <= b.fin)
               )
             );
    quit;

    proc fedsql sessref=conn;
        create table casuser._mono_bucket_stats {options replace=true} as
        select Split,
               Split_Order,
               Bucket_Order,
               Valor_X,
               count(*) as N,
               avg(_mono_target) as Mean_Default
        from casuser._mono_tagged
        group by Split, Split_Order, Bucket_Order, Valor_X;
    quit;

    proc fedsql sessref=conn;
        create table &out_table. {options replace=true} as
        select 1 as Run_Order,
               a.Split,
               a.Split_Order,
               a.Bucket_Order,
               a.Valor_X,
               a.N,
               case
                   when b.Total_Split > 0 then a.N / b.Total_Split
                   else .
               end as Pct_Cuentas,
               a.Mean_Default
        from casuser._mono_bucket_stats a
        left join casuser._mono_split_totals b
          on a.Split = b.Split
         and a.Split_Order = b.Split_Order;
    quit;

%mend _mono_build_detail;

%macro _monotonicidad_compute(input_caslib=, input_table=, score_var=, target=,
    byvar=, def_cld=, groups=5, train_min_mes=, train_max_mes=, oot_min_mes=,
    oot_max_mes=);

    %put NOTE: [monotonicidad_compute] Construyendo input canonico en CAS.;

    %_mono_prepare_input(input_caslib=&input_caslib., input_table=&input_table.,
        score_var=&score_var., target=&target., byvar=&byvar.,
        def_cld=&def_cld., train_min_mes=&train_min_mes.,
        train_max_mes=&train_max_mes., oot_min_mes=&oot_min_mes.,
        oot_max_mes=&oot_max_mes.);

    %put NOTE: [monotonicidad_compute] Calculando cortes TRAIN para &score_var..;

    %_mono_calcular_cortes(train_table=casuser._mono_input, groups=&groups.,
        out_cuts=casuser._mono_cuts);

    %put NOTE: [monotonicidad_compute] Construyendo detalle TRAIN/OOT.;

    %_mono_build_detail(input_table=casuser._mono_input,
        cuts_table=casuser._mono_cuts, out_table=casuser._mono_detail);

%mend _monotonicidad_compute;
