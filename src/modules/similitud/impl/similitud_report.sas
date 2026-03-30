/* =========================================================================
similitud_report.sas - Reportes HTML + Excel + JPEG para Similitud

Input:
- Tabla CAS unificada con columna Split=TRAIN/OOT
- Computo y temporales en casuser (CAS-first)
========================================================================= */
%macro _similitud_report(input_caslib=, input_table=, split_var=Split,
    target=, byvar=, vars_num=, vars_cat=, groups=, report_path=,
    images_path=, file_prefix=);

    %local _dir_rc;

    %put NOTE: [similitud_report] Generando reportes...;
    %put NOTE: [similitud_report] input=&input_caslib..&input_table.;
    %put NOTE: [similitud_report] report_path=&report_path.;
    %put NOTE: [similitud_report] images_path=&images_path.;
    %put NOTE: [similitud_report] file_prefix=&file_prefix.;

    %let _dir_rc=%sysfunc(dcreate(METOD6, &report_path./../));
    %let _dir_rc=%sysfunc(dcreate(., &report_path.));
    %let _dir_rc=%sysfunc(dcreate(METOD6, &images_path./../));
    %let _dir_rc=%sysfunc(dcreate(., &images_path.));

    ods graphics on;
    ods listing gpath="&images_path.";

    ods html5 file="&report_path./&file_prefix..html"
        options(bitmap_mode='inline');
    ods excel file="&report_path./&file_prefix..xlsx"
        options(sheet_name='BUCKET_EVOLUTION' sheet_interval='none'
        embedded_titles='yes');

    ods graphics / imagename="&file_prefix._bkt" imagefmt=jpeg;
    title 'Analisis de Distribucion por Buckets - TRAIN vs OOT';

    %_simil_bucket_variables(data=&input_caslib..&input_table.,
        split_var=&split_var., byvar=&byvar., vars_num=&vars_num.,
        vars_cat=&vars_cat., groups=&groups.);

    title;

    ods excel options(sheet_name='SIMILITUD' sheet_interval='now'
        embedded_titles='yes');
    ods graphics / imagename="&file_prefix._sim" imagefmt=jpeg;

    %_simil_similitud_num(data=&input_caslib..&input_table.,
        split_var=&split_var., vars_num=&vars_num., target=&target.);

    %if %length(%superq(vars_cat)) > 0 %then %do;
        %_simil_similitud_cat(data=&input_caslib..&input_table.,
            split_var=&split_var., vars_cat=&vars_cat.);
    %end;

    ods excel close;
    ods html5 close;
    ods graphics / reset=all;
    ods graphics off;

    proc datasets library=casuser nolist nowarn;
        delete _simil_:;
    quit;

    %put NOTE: [similitud_report] HTML=> &report_path./&file_prefix..html;
    %put NOTE: [similitud_report] Excel=> &report_path./&file_prefix..xlsx;
    %put NOTE: [similitud_report] Images=> &images_path./;

%mend _similitud_report;
