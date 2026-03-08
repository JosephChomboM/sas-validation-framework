/* =========================================================================
missings_report.sas - Generacion de reportes HTML + Excel + JPEG
para Missings/Dummies

Genera:
<report_path>/<prefix>_train.html  - HTML con tablas TRAIN (inline)
<report_path>/<prefix>_oot.html    - HTML con tablas OOT (inline)
<report_path>/<prefix>.xlsx        - Excel multi-hoja (TRAIN + OOT)
<images_path>/<prefix>_*.jpeg      - Graficos como JPEG independientes

Tablas temporales se crean en casuser (CAS) via PROC FEDSQL.
Formato de imagen: JPEG. HTML usa bitmap_mode=inline.
========================================================================= */
%macro _missings_report( input_caslib=, train_table=, oot_table=, vars_num=,
    vars_cat=, threshold=, report_path=, images_path=, file_prefix=);

    %put NOTE: [missings_report] Generando reportes...;
    %put NOTE: [missings_report] report_path=&report_path.;
    %put NOTE: [missings_report] images_path=&images_path.;
    %put NOTE: [missings_report] file_prefix=&file_prefix.;
    %put NOTE: [missings_report] threshold=&threshold.;

    /* ---- Copiar tablas CAS a casuser (FEDSQL para CAS-to-CAS) --------- */
    proc fedsql sessref=conn;
        create table casuser._miss_train {options replace=true} as select * from
            &input_caslib..&train_table.;
    quit;

    proc fedsql sessref=conn;
        create table casuser._miss_oot {options replace=true} as select * from
            &input_caslib..&oot_table.;
    quit;

    /* ---- Crear directorios si no existen ------------------------------- */
    %local _dir_rc;
    %let _dir_rc=%sysfunc(dcreate(METOD4.2, &report_path./../));
    %let _dir_rc=%sysfunc(dcreate(., &report_path.));
    %let _dir_rc=%sysfunc(dcreate(METOD4.2, &images_path./../));
    %let _dir_rc=%sysfunc(dcreate(., &images_path.));

    /* ==================================================================
    TRAIN: HTML + primera hoja Excel
    ================================================================== */
    ods graphics on;
    ods listing gpath="&images_path.";

    ods html5 file="&report_path./&file_prefix._train.html"
        options(bitmap_mode="inline");
    ods excel file="&report_path./&file_prefix..xlsx"
        options(sheet_name="TRAIN_Missings" sheet_interval="none"
        embedded_titles="yes");
    ods graphics / imagename="&file_prefix._trn_miss" imagefmt=jpeg;

    title "TRAIN: Analisis de Missings";

    %_miss_compute(data=casuser._miss_train, vars_num=&vars_num.,
        vars_cat=&vars_cat., threshold=&threshold.);

    title;
    ods html5 close;
    ods graphics / reset=all;

    /* ==================================================================
    OOT: HTML + segunda hoja Excel
    ================================================================== */
    ods html5 file="&report_path./&file_prefix._oot.html"
        options(bitmap_mode="inline");
    ods excel options(sheet_name="OOT_Missings" sheet_interval="now"
        embedded_titles="yes");
    ods graphics / imagename="&file_prefix._oot_miss" imagefmt=jpeg;

    title "OOT: Analisis de Missings";

    %_miss_compute(data=casuser._miss_oot, vars_num=&vars_num.,
        vars_cat=&vars_cat., threshold=&threshold.);

    title;
    ods excel close;
    ods html5 close;
    ods graphics / reset=all;
    ods graphics off;

    /* ---- Cleanup tablas temporales CAS --------------------------------- */
    proc datasets library=casuser nolist nowarn;
        delete _miss_train _miss_oot;
    quit;

    %put NOTE: [missings_report] HTML TRAIN=>
        &report_path./&file_prefix._train.html;
    %put NOTE: [missings_report] HTML OOT=>
        &report_path./&file_prefix._oot.html;
    %put NOTE: [missings_report] Excel=> &report_path./&file_prefix..xlsx;

%mend _missings_report;
