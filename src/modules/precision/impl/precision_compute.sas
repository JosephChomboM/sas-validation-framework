/* =========================================================================
precision_compute.sas - Computo del modulo Precision
========================================================================= */

%macro _prec_total(data=, target=, score_var=, out=, weight_var=);
    proc summary data=&data.;
        var &target. &score_var.;
        %if %length(%superq(weight_var)) > 0 %then %do;
            weight &weight_var.;
        %end;
        output out=work._prec_total_tmp(drop=_TYPE_ _FREQ_) mean= / autoname;
    run;

    data &out.;
        set work._prec_total_tmp;
        target_mean=&target._Mean;
        score_mean=&score_var._Mean;
        gap=score_mean - target_mean;
        abs_gap=abs(gap);
        keep target_mean score_mean gap abs_gap;
    run;

    proc datasets library=work nolist nowarn;
        delete _prec_total_tmp;
    quit;

%mend _prec_total;

%macro _prec_segmento(data=, target=, score_var=, segvar=, out=, weight_var=);
    proc summary data=&data. nway missing;
        class &segvar.;
        var &target. &score_var.;
        %if %length(%superq(weight_var)) > 0 %then %do;
            weight &weight_var.;
        %end;
        output out=work._prec_seg_tmp(drop=_TYPE_ _FREQ_) mean= / autoname;
    run;

    data &out.;
        set work._prec_seg_tmp;
        target_mean=&target._Mean;
        score_mean=&score_var._Mean;
        gap=score_mean - target_mean;
        abs_gap=abs(gap);
        keep &segvar. target_mean score_mean gap abs_gap;
    run;

    proc datasets library=work nolist nowarn;
        delete _prec_seg_tmp;
    quit;

%mend _prec_segmento;

%macro _prec_plot_total(in=, out=);
    data &out.;
        set &in.;
        length metrica $12;
        metrica='TARGET'; valor=target_mean; output;
        metrica='SCORE'; valor=score_mean; output;
        keep metrica valor;
    run;
%mend _prec_plot_total;

%macro _prec_plot_segmento(in=, segvar=, out=);
    data &out.;
        set &in.;
        length metrica $12;
        metrica='TARGET'; valor=target_mean; output;
        metrica='SCORE'; valor=score_mean; output;
        keep &segvar. metrica valor;
    run;
%mend _prec_plot_segmento;

%macro _precision_compute(data=, alias=, target=, score_var=, monto_var=,
    segvar=, has_weight=0, has_seg=0);

    %_prec_total(data=&data., target=&target., score_var=&score_var.,
        out=work.&alias._total);
    %_prec_plot_total(in=work.&alias._total, out=work.&alias._plot_total);

    %if &has_weight.=1 %then %do;
        %_prec_total(data=&data., target=&target., score_var=&score_var.,
            weight_var=&monto_var., out=work.&alias._total_w);
        %_prec_plot_total(in=work.&alias._total_w,
            out=work.&alias._plot_total_w);
    %end;

    %if &has_seg.=1 %then %do;
        %_prec_segmento(data=&data., target=&target., score_var=&score_var.,
            segvar=&segvar., out=work.&alias._seg);
        %_prec_plot_segmento(in=work.&alias._seg, segvar=&segvar.,
            out=work.&alias._plot_seg);

        %if &has_weight.=1 %then %do;
            %_prec_segmento(data=&data., target=&target., score_var=&score_var.,
                segvar=&segvar., weight_var=&monto_var.,
                out=work.&alias._seg_w);
            %_prec_plot_segmento(in=work.&alias._seg_w, segvar=&segvar.,
                out=work.&alias._plot_seg_w);
        %end;
    %end;

%mend _precision_compute;
