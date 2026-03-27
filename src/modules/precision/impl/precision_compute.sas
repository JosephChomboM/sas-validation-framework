/* =========================================================================
precision_compute.sas - Computo del modulo Precision
========================================================================= */

%macro _prec_total(data=, target=, score_var=, out=, weight_var=, split=);
    %local _where_sql;
    %let _where_sql=;
    %if %length(%superq(split)) > 0 %then
        %let _where_sql=where upcase(Muestra)=%str(%')%upcase(&split.)%str(%');

    proc fedsql sessref=conn;
        create table &out. {options replace=true} as
        select
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
        &_where_sql.;
    quit;

    proc fedsql sessref=conn;
        create table &out. {options replace=true} as
        select target_mean,
            score_mean,
            (score_mean - target_mean) as gap,
            abs(score_mean - target_mean) as abs_gap
        from &out.;
    quit;
%mend _prec_total;

%macro _prec_segmento(data=, target=, score_var=, segvar=, out=, weight_var=,
    split=);
    %local _where_sql;
    %let _where_sql=;
    %if %length(%superq(split)) > 0 %then
        %let _where_sql=where upcase(Muestra)=%str(%')%upcase(&split.)%str(%');

    proc fedsql sessref=conn;
        create table &out. {options replace=true} as
        select
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
        &_where_sql.
        group by &segvar.;
    quit;

    proc fedsql sessref=conn;
        create table &out. {options replace=true} as
        select &segvar.,
            target_mean,
            score_mean,
            (score_mean - target_mean) as gap,
            abs(score_mean - target_mean) as abs_gap
        from &out.;
    quit;
%mend _prec_segmento;

%macro _prec_plot_total(in=, out=);
    proc fedsql sessref=conn;
        create table &out. {options replace=true} as
        select 'TARGET' as metrica, target_mean as valor from &in.
        union all
        select 'SCORE' as metrica, score_mean as valor from &in.;
    quit;
%mend _prec_plot_total;

%macro _prec_plot_segmento(in=, segvar=, out=);
    proc fedsql sessref=conn;
        create table &out. {options replace=true} as
        select &segvar., 'TARGET' as metrica, target_mean as valor from &in.
        union all
        select &segvar., 'SCORE' as metrica, score_mean as valor from &in.;
    quit;
%mend _prec_plot_segmento;

%macro _precision_compute(data=, alias=, target=, score_var=, monto_var=,
    segvar=, has_weight=0, has_seg=0, split=, out_caslib=casuser);

    %_prec_total(data=&data., target=&target., score_var=&score_var.,
        out=&out_caslib..&alias._total, split=&split.);
    %_prec_plot_total(in=&out_caslib..&alias._total,
        out=&out_caslib..&alias._plot_total);

    %if &has_weight.=1 %then %do;
        %_prec_total(data=&data., target=&target., score_var=&score_var.,
            weight_var=&monto_var., out=&out_caslib..&alias._total_w,
            split=&split.);
        %_prec_plot_total(in=&out_caslib..&alias._total_w,
            out=&out_caslib..&alias._plot_total_w);
    %end;

    %if &has_seg.=1 %then %do;
        %_prec_segmento(data=&data., target=&target., score_var=&score_var.,
            segvar=&segvar., out=&out_caslib..&alias._seg, split=&split.);
        %_prec_plot_segmento(in=&out_caslib..&alias._seg, segvar=&segvar.,
            out=&out_caslib..&alias._plot_seg);

        %if &has_weight.=1 %then %do;
            %_prec_segmento(data=&data., target=&target., score_var=&score_var.,
                segvar=&segvar., weight_var=&monto_var.,
                out=&out_caslib..&alias._seg_w, split=&split.);
            %_prec_plot_segmento(in=&out_caslib..&alias._seg_w, segvar=&segvar.,
                out=&out_caslib..&alias._plot_seg_w);
        %end;
    %end;

%mend _precision_compute;
