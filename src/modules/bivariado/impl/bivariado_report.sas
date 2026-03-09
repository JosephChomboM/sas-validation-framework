/* =========================================================================
bivariado_report.sas - Generacion de reportes HTML + Excel + JPEG
para Bivariado (tendencia)

Genera:
<report_path>/<prefix>.html       - HTML con graficos TRAIN+OOT (inline)
<report_path>/<prefix>.xlsx       - Excel multi-hoja:
Hoja 1: TRAIN-OOT (variables principales)
Hoja 2: DRIVERS (si hay dri_num/dri_cat)
<images_path>/<prefix>_*.jpeg     - Graficos JPEG independientes

Tablas temporales se copian a work para computo
(PROC RANK + DATA step dinamico no son CAS-compatibles).
========================================================================= */
%macro _bivariado_report( input_caslib=, train_table=, oot_table=, target=,
    vars_num=, vars_cat=, dri_num=, dri_cat=, groups=, report_path=,
    images_path=, file_prefix=);

    %put NOTE: [bivariado_report] Generando reportes...;
    %put NOTE: [bivariado_report] report_path=&report_path.;
    %put NOTE: [bivariado_report] file_prefix=&file_prefix.;
    %put NOTE: [bivariado_report] target=&target.;

    /* ---- Copiar tablas CAS a work (PROC RANK no soporta CAS) ---------- */
    data work._biv_train;
        set &input_caslib..&train_table.;
    run;

    data work._biv_oot;
        set &input_caslib..&oot_table.;
    run;

    /* ---- Crear directorios si no existen ------------------------------- */
    %local _dir_rc _has_drivers;
    %let _dir_rc=%sysfunc(dcreate(METOD4.3, &report_path./../));
    %let _dir_rc=%sysfunc(dcreate(., &report_path.));
    %let _dir_rc=%sysfunc(dcreate(METOD4.3, &images_path./../));
    %let _dir_rc=%sysfunc(dcreate(., &images_path.));

    /* Determinar si hay drivers */
    %let _has_drivers=0;
    %if %length(&dri_num.) > 0 or %length(&dri_cat.) > 0 %then %let
        _has_drivers=1;

    /* ==================================================================
    Hoja 1: TRAIN-OOT (variables principales)
    ================================================================== */
    ods graphics on;
    ods listing gpath="&images_path.";

    ods html5 file="&report_path./&file_prefix..html"
        options(bitmap_mode="inline");
    ods excel file="&report_path./&file_prefix..xlsx"
        options(sheet_name="TRAIN - OOT" sheet_interval="none"
        embedded_titles="yes");
    ods graphics / imagename="&file_prefix._biv" imagefmt=jpeg;

    %_biv_trend_variables(train_data=work._biv_train, oot_data=work._biv_oot,
        target=&target., vars_num=&vars_num., vars_cat=&vars_cat.,
        groups=&groups.);

    ods html5 close;

    /* ==================================================================
    Hoja 2: DRIVERS (si existen)
    ================================================================== */
    %if &_has_drivers.=1 %then %do;
        %put NOTE: [bivariado_report] Generando hoja DRIVERS...;
        ods html5 file="&report_path./&file_prefix._drivers.html"
            options(bitmap_mode="inline");
        ods excel options(sheet_name="DRIVERS" sheet_interval="now"
            embedded_titles="yes");
        ods graphics / imagename="&file_prefix._drv" imagefmt=jpeg;

        %_biv_trend_variables(train_data=work._biv_train,
            oot_data=work._biv_oot, target=&target., vars_num=&dri_num.,
            vars_cat=&dri_cat., groups=&groups.);

        ods html5 close;
    %end;

    ods excel close;
    ods graphics / reset=all;
    ods graphics off;

    /* ---- Cleanup tablas work ------------------------------------------- */
    proc datasets library=work nolist nowarn;
        delete _biv_train _biv_oot;
    quit;

    %put NOTE: [bivariado_report] HTML=> &report_path./&file_prefix..html;
    %if &_has_drivers.=1 %then %put NOTE: [bivariado_report] HTML drivers=>
        &report_path./&file_prefix._drivers.html;
    %put NOTE: [bivariado_report] Excel=> &report_path./&file_prefix..xlsx;
    %put NOTE: [bivariado_report] Images=> &images_path./;

%mend _bivariado_report;
