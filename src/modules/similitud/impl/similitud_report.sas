/* =========================================================================
similitud_report.sas - Generacion de reportes HTML + Excel + JPEG
para Similitud de Muestras

Genera:
<report_path>/<prefix>.html          - HTML con bucket plots + similitud
<report_path>/<prefix>.xlsx          - Excel multi-hoja:
Hoja 1: BUCKET_EVOLUTION (distribucion por buckets TRAIN+OOT)
Hoja 2: SIMILITUD (comparacion estadistica TRAIN vs OOT)
<images_path>/<prefix>_*.jpeg        - Graficos JPEG independientes

Tablas temporales se copian a work para computo
(PROC RANK, PROC SORT, PROC FREQ BY, PROC TRANSPOSE, PROC MEANS
no son CAS-compatibles).
========================================================================= */
%macro _similitud_report( input_caslib=, train_table=, oot_table=, target=,
    byvar=, vars_num=, vars_cat=, groups=, report_path=, images_path=,
    file_prefix=);

    %put NOTE: [similitud_report] Generando reportes...;
    %put NOTE: [similitud_report] report_path=&report_path.;
    %put NOTE: [similitud_report] file_prefix=&file_prefix.;
    %put NOTE: [similitud_report] target=&target.;
    %put NOTE: [similitud_report] byvar=&byvar.;

    /* ---- Copiar tablas CAS a work (Pattern B) -------------------------- */
    data work._simil_train;
        set &input_caslib..&train_table.;
    run;

    data work._simil_oot;
        set &input_caslib..&oot_table.;
    run;

    /* ---- Crear directorios si no existen ------------------------------- */
    %local _dir_rc;
    %let _dir_rc=%sysfunc(dcreate(METOD6, &report_path./../));
    %let _dir_rc=%sysfunc(dcreate(., &report_path.));
    %let _dir_rc=%sysfunc(dcreate(METOD6, &images_path./../));
    %let _dir_rc=%sysfunc(dcreate(., &images_path.));

    /* ==================================================================
    Seccion 1: BUCKET EVOLUTION (distribucion por buckets TRAIN+OOT)
    ================================================================== */
    ods graphics on;
    ods listing gpath="&images_path.";

    ods html5 file="&report_path./&file_prefix..html"
        options(bitmap_mode="inline");
    ods excel file="&report_path./&file_prefix..xlsx"
        options(sheet_name="BUCKET_EVOLUTION" sheet_interval="none"
        embedded_titles="yes");
    ods graphics / imagename="&file_prefix._bkt" imagefmt=jpeg;

    title "Analisis de Distribucion por Buckets - TRAIN vs OOT";

    %_simil_bucket_variables( train_data=work._simil_train,
        oot_data=work._simil_oot, byvar=&byvar., vars_num=&vars_num.,
        vars_cat=&vars_cat., groups=&groups. );

    title;

    /* ==================================================================
    Seccion 2: SIMILITUD (comparacion estadistica TRAIN vs OOT)
    ================================================================== */
    ods excel options(sheet_name="SIMILITUD" sheet_interval="now"
        embedded_titles="yes");
    ods graphics / imagename="&file_prefix._sim" imagefmt=jpeg;

    /* Similitud numericas (mediana) */
    %_simil_similitud_num( train_data=work._simil_train,
        oot_data=work._simil_oot, vars_num=&vars_num., target=&target. );

    /* Similitud categoricas (moda) */
    %if %length(&vars_cat.) > 0 %then %do;
        %_simil_similitud_cat( train_data=work._simil_train,
            oot_data=work._simil_oot, vars_cat=&vars_cat. );
    %end;

    ods excel close;
    ods html5 close;
    ods graphics / reset=all;
    ods graphics off;

    /* ---- Cleanup tablas work ------------------------------------------- */
    proc datasets library=work nolist nowarn;
        delete _simil_train _simil_oot;
    quit;

    %put NOTE: [similitud_report] HTML=> &report_path./&file_prefix..html;
    %put NOTE: [similitud_report] Excel=> &report_path./&file_prefix..xlsx;
    %put NOTE: [similitud_report] Images=> &images_path./;

%mend _similitud_report;
