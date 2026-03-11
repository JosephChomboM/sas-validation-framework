/* =========================================================================
precision_report.sas - Reportes HTML + Excel + JPEG para Precision
========================================================================= */

%macro _precision_render(data=, alias=, data_label=, target=, score_var=,
    monto_var=, segvar=, has_weight=0, has_seg=0, file_prefix=);

    %_precision_compute(data=&data., alias=&alias., target=&target.,
        score_var=&score_var., monto_var=&monto_var., segvar=&segvar.,
        has_weight=&has_weight., has_seg=&has_seg.);

    title "&data_label.: Precision del Modelo";
    proc print data=work.&alias._total noobs label;
        label target_mean="Promedio Target"
            score_mean="Promedio Score"
            gap="Gap Score-Target"
            abs_gap="Gap Absoluto";
    run;
    title;

    ods graphics / imagename="&file_prefix._%lowcase(&data_label.)_total"
        imagefmt=jpeg;
    title "&data_label.: Target vs Score";
    proc sgplot data=work.&alias._plot_total;
        vbarparm category=metrica response=valor /
            fillattrs=(color=cx4C9A8A) datalabel;
        yaxis label="Promedio";
        xaxis label="Metrica";
    run;
    title;

    %if &has_seg.=1 %then %do;
        title "&data_label.: Precision por Segmento";
        proc print data=work.&alias._seg noobs label;
            label target_mean="Promedio Target"
                score_mean="Promedio Score"
                gap="Gap Score-Target"
                abs_gap="Gap Absoluto";
        run;
        title;

        ods graphics / imagename="&file_prefix._%lowcase(&data_label.)_seg"
            imagefmt=jpeg;
        title "&data_label.: Target vs Score por Segmento";
        proc sgplot data=work.&alias._plot_seg;
            vbarparm category=&segvar. response=valor / group=metrica
                groupdisplay=cluster datalabel;
            yaxis label="Promedio";
            xaxis label="Segmento";
        run;
        title;
    %end;

    %if &has_weight.=1 %then %do;
        title "&data_label.: Precision Ponderada por Monto - &monto_var.";
        proc print data=work.&alias._total_w noobs label;
            label target_mean="Promedio Target"
                score_mean="Promedio Score"
                gap="Gap Score-Target"
                abs_gap="Gap Absoluto";
        run;
        title;

        ods graphics / imagename="&file_prefix._%lowcase(&data_label.)_totalw"
            imagefmt=jpeg;
        title "&data_label.: Target vs Score Ponderado";
        proc sgplot data=work.&alias._plot_total_w;
            vbarparm category=metrica response=valor /
                fillattrs=(color=cxD98F5C) datalabel;
            yaxis label="Promedio Ponderado";
            xaxis label="Metrica";
        run;
        title;

        %if &has_seg.=1 %then %do;
            title "&data_label.: Precision Ponderada por Segmento";
            proc print data=work.&alias._seg_w noobs label;
                label target_mean="Promedio Target"
                    score_mean="Promedio Score"
                    gap="Gap Score-Target"
                    abs_gap="Gap Absoluto";
            run;
            title;

            ods graphics / imagename="&file_prefix._%lowcase(&data_label.)_segw"
                imagefmt=jpeg;
            title "&data_label.: Target vs Score Ponderado por Segmento";
            proc sgplot data=work.&alias._plot_seg_w;
                vbarparm category=&segvar. response=valor / group=metrica
                    groupdisplay=cluster datalabel;
                yaxis label="Promedio Ponderado";
                xaxis label="Segmento";
            run;
            title;
        %end;
    %end;

%mend _precision_render;

%macro _precision_report(input_caslib=, train_table=, oot_table=, target=,
    score_var=, monto_var=, segvar=, byvar=, def_cld=0, ponderado=1,
    report_path=, images_path=, file_prefix=);

    %local _keep_vars _has_weight _has_seg;
    %let _has_weight=0;
    %let _has_seg=0;

    %let _keep_vars=&target. &score_var. &byvar.;
    %if %length(%superq(monto_var)) > 0 %then %let _keep_vars=&_keep_vars. &monto_var.;
    %if %length(%superq(segvar)) > 0 %then %let _keep_vars=&_keep_vars. &segvar.;
    %let _keep_vars=%sysfunc(compbl(&_keep_vars.));

    %if %length(%superq(monto_var)) > 0 and &ponderado.=1 %then %let _has_weight=1;
    %if %length(%superq(segvar)) > 0 %then %let _has_seg=1;

    data work._prec_train;
        set &input_caslib..&train_table.(keep=&_keep_vars.);
        where &byvar. <= &def_cld.;
    run;

    data work._prec_oot;
        set &input_caslib..&oot_table.(keep=&_keep_vars.);
        where &byvar. <= &def_cld.;
    run;

    ods graphics on;
    ods listing gpath="&images_path.";

    ods html5 file="&report_path./&file_prefix._train.html"
        options(bitmap_mode="inline");
    ods excel file="&report_path./&file_prefix..xlsx"
        options(sheet_name="TRAIN_Precision" sheet_interval="none"
        embedded_titles="yes");

    %_precision_render(data=work._prec_train, alias=_prec_trn,
        data_label=TRAIN, target=&target., score_var=&score_var.,
        monto_var=&monto_var., segvar=&segvar., has_weight=&_has_weight.,
        has_seg=&_has_seg., file_prefix=&file_prefix.);

    ods html5 close;
    ods graphics / reset=all;

    ods html5 file="&report_path./&file_prefix._oot.html"
        options(bitmap_mode="inline");
    ods excel options(sheet_name="OOT_Precision" sheet_interval="now"
        embedded_titles="yes");

    %_precision_render(data=work._prec_oot, alias=_prec_oot,
        data_label=OOT, target=&target., score_var=&score_var.,
        monto_var=&monto_var., segvar=&segvar., has_weight=&_has_weight.,
        has_seg=&_has_seg., file_prefix=&file_prefix.);

    ods excel close;
    ods html5 close;
    ods graphics / reset=all;
    ods graphics off;

    proc datasets library=work nolist nowarn;
        delete _prec_:;
    quit;

%mend _precision_report;
