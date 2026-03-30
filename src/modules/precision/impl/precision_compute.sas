/* =========================================================================
precision_compute.sas - Computo CAS-first del modulo Precision

Todas las agregaciones parten de una sola tabla CAS con columna split.
El ordenamiento queda disponible via table.partition y se aplica solo al
momento de renderizar tablas o graficos que realmente lo necesitan.
========================================================================= */

%macro _prec_sort_cas(caslib=casuser, table_name=, orderby=, groupby={});

    %if %length(%superq(table_name))=0 or %length(%superq(orderby))=0 %then
        %return;

    proc cas;
        session conn;
        table.partition /
            table={
                caslib="&caslib.",
                name="&table_name.",
                orderby=&orderby.,
                groupby=&groupby.
            },
            casout={
                caslib="&caslib.",
                name="&table_name.",
                replace=true
            };
    quit;

%mend _prec_sort_cas;

%macro _prec_total(data=, target=, score_var=, out=, weight_var=,
    split_var=);

    %local _select_split _group_by;
    %let _select_split=;
    %let _group_by=;

    %if %length(%superq(split_var)) > 0 %then %do;
        %let _select_split=&split_var.,;
        %let _group_by=group by &split_var.;
    %end;

    proc fedsql sessref=conn;
        create table &out. {options replace=true} as
        select
            &_select_split.
            %if %length(%superq(weight_var)) > 0 %then %do;
                case
                    when sum(case when &weight_var. is not null and
                        &target. is not null then &weight_var. else 0 end)=0
                        then .
                    else sum(case when &weight_var. is not null and
                        &target. is not null then &target. * &weight_var.
                        else 0 end) /
                        sum(case when &weight_var. is not null and
                        &target. is not null then &weight_var. else 0 end)
                end as target_mean,
                case
                    when sum(case when &weight_var. is not null and
                        &score_var. is not null then &weight_var. else 0
                        end)=0 then .
                    else sum(case when &weight_var. is not null and
                        &score_var. is not null then &score_var. * &weight_var.
                        else 0 end) /
                        sum(case when &weight_var. is not null and
                        &score_var. is not null then &weight_var. else 0 end)
                end as score_mean
            %end;
            %else %do;
                avg(&target.) as target_mean,
                avg(&score_var.) as score_mean
            %end;
        from &data.
        &_group_by.;
    quit;

    proc fedsql sessref=conn;
        create table &out. {options replace=true} as
        select
            &_select_split.
            target_mean,
            score_mean,
            (score_mean - target_mean) as gap,
            abs(score_mean - target_mean) as abs_gap
        from &out.;
    quit;

%mend _prec_total;

%macro _prec_segmento(data=, target=, score_var=, segvar=, out=, weight_var=,
    split_var=);

    %local _select_split _group_cols;
    %let _select_split=;
    %let _group_cols=&segvar.;

    %if %length(%superq(split_var)) > 0 %then %do;
        %let _select_split=&split_var.,;
        %let _group_cols=&split_var., &segvar.;
    %end;

    proc fedsql sessref=conn;
        create table &out. {options replace=true} as
        select
            &_select_split.
            &segvar.,
            %if %length(%superq(weight_var)) > 0 %then %do;
                case
                    when sum(case when &weight_var. is not null and
                        &target. is not null then &weight_var. else 0 end)=0
                        then .
                    else sum(case when &weight_var. is not null and
                        &target. is not null then &target. * &weight_var.
                        else 0 end) /
                        sum(case when &weight_var. is not null and
                        &target. is not null then &weight_var. else 0 end)
                end as target_mean,
                case
                    when sum(case when &weight_var. is not null and
                        &score_var. is not null then &weight_var. else 0
                        end)=0 then .
                    else sum(case when &weight_var. is not null and
                        &score_var. is not null then &score_var. * &weight_var.
                        else 0 end) /
                        sum(case when &weight_var. is not null and
                        &score_var. is not null then &weight_var. else 0 end)
                end as score_mean
            %end;
            %else %do;
                avg(&target.) as target_mean,
                avg(&score_var.) as score_mean
            %end;
        from &data.
        group by &_group_cols.;
    quit;

    proc fedsql sessref=conn;
        create table &out. {options replace=true} as
        select
            &_select_split.
            &segvar.,
            target_mean,
            score_mean,
            (score_mean - target_mean) as gap,
            abs(score_mean - target_mean) as abs_gap
        from &out.;
    quit;

%mend _prec_segmento;

%macro _prec_plot_total(in=, out=, split_var=);

    %local _select_split;
    %let _select_split=;

    %if %length(%superq(split_var)) > 0 %then
        %let _select_split=&split_var.,;

    proc fedsql sessref=conn;
        create table &out. {options replace=true} as
        select &_select_split. 'TARGET' as metrica, target_mean as valor
        from &in.
        union all
        select &_select_split. 'SCORE' as metrica, score_mean as valor
        from &in.;
    quit;

%mend _prec_plot_total;

%macro _prec_plot_segmento(in=, segvar=, out=, split_var=);

    %local _select_split;
    %let _select_split=;

    %if %length(%superq(split_var)) > 0 %then
        %let _select_split=&split_var.,;

    proc fedsql sessref=conn;
        create table &out. {options replace=true} as
        select &_select_split. &segvar., 'TARGET' as metrica,
            target_mean as valor
        from &in.
        union all
        select &_select_split. &segvar., 'SCORE' as metrica,
            score_mean as valor
        from &in.;
    quit;

%mend _prec_plot_segmento;

%macro _precision_compute(data=, alias=, target=, score_var=, monto_var=,
    segvar=, has_weight=0, has_seg=0, split_var=split, out_caslib=casuser);

    %_prec_total(data=&data., target=&target., score_var=&score_var.,
        out=&out_caslib..&alias._total, split_var=&split_var.);
    %_prec_plot_total(in=&out_caslib..&alias._total,
        out=&out_caslib..&alias._plot_total, split_var=&split_var.);

    %if &has_weight.=1 %then %do;
        %_prec_total(data=&data., target=&target., score_var=&score_var.,
            weight_var=&monto_var., out=&out_caslib..&alias._total_w,
            split_var=&split_var.);
        %_prec_plot_total(in=&out_caslib..&alias._total_w,
            out=&out_caslib..&alias._plot_total_w, split_var=&split_var.);
    %end;

    %if &has_seg.=1 %then %do;
        %_prec_segmento(data=&data., target=&target., score_var=&score_var.,
            segvar=&segvar., out=&out_caslib..&alias._seg,
            split_var=&split_var.);
        %_prec_plot_segmento(in=&out_caslib..&alias._seg, segvar=&segvar.,
            out=&out_caslib..&alias._plot_seg, split_var=&split_var.);

        %if &has_weight.=1 %then %do;
            %_prec_segmento(data=&data., target=&target., score_var=&score_var.,
                segvar=&segvar., weight_var=&monto_var.,
                out=&out_caslib..&alias._seg_w, split_var=&split_var.);
            %_prec_plot_segmento(in=&out_caslib..&alias._seg_w,
                segvar=&segvar., out=&out_caslib..&alias._plot_seg_w,
                split_var=&split_var.);
        %end;
    %end;

%mend _precision_compute;
