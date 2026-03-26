/* =========================================================================
estabilidad_report.sas - Generacion de reportes HTML + Excel + JPEG
para Estabilidad Temporal

Genera:
<report_path>/<prefix>.html        - HTML combinado TRAIN + OOT (inline)
<report_path>/<prefix>.xlsx        - Excel combinado TRAIN + OOT
<images_path>/<prefix>_*.jpeg      - Graficos como JPEG independientes

Consume una sola tabla consolidada con columna Muestra.
Tablas temporales se crean en casuser (CAS) via PROC FEDSQL.
Formato de imagen: JPEG. HTML usa bitmap_mode=inline.
========================================================================= */
%macro _estabilidad_report(input_caslib=, input_table=, byvar=,
    vars_num=, vars_cat=, report_path=, images_path=, file_prefix=);

    %put NOTE: [estabilidad_report] Generando reportes...;
    %put NOTE: [estabilidad_report] report_path=&report_path.;
    %put NOTE: [estabilidad_report] images_path=&images_path.;
    %put NOTE: [estabilidad_report] file_prefix=&file_prefix.;

    /* ---- Copiar input consolidado a casuser --------------------------- */
    proc fedsql sessref=conn;
        create table casuser._estab_all {options replace=true} as
        select *
        from &input_caslib..&input_table.;
    quit;

    /* ---- Crear directorios si no existen ------------------------------- */
    %local _dir_rc;
    %let _dir_rc=%sysfunc(dcreate(METOD4.2, &report_path./../));
    %let _dir_rc=%sysfunc(dcreate(., &report_path.));
    %let _dir_rc=%sysfunc(dcreate(METOD4.2, &images_path./../));
    %let _dir_rc=%sysfunc(dcreate(., &images_path.));

    /* ==================================================================
    Reporte combinado TRAIN + OOT
    ================================================================== */
    ods graphics on;
    ods listing gpath="&images_path.";

    ods html5 file="&report_path./&file_prefix..html"
        options(bitmap_mode="inline");
    ods excel file="&report_path./&file_prefix..xlsx"
        options(sheet_name="Estabilidad" sheet_interval="none"
        embedded_titles="yes");
    ods graphics / imagename="&file_prefix._estab" imagefmt=jpeg;

    title "Analisis de Estabilidad Temporal - TRAIN y OOT";

    %_estab_variables(data=casuser._estab_all, byvar=&byvar.,
        vars_num=&vars_num., vars_cat=&vars_cat.);

    title;
    ods html5 close;
    ods excel close;
    ods graphics / reset=all;
    ods graphics off;

    /* ---- Cleanup tablas temporales CAS --------------------------------- */
    proc datasets library=casuser nolist nowarn;
        delete _estab_all;
    quit;

    %put NOTE: [estabilidad_report] HTML=>
        &report_path./&file_prefix..html;
    %put NOTE: [estabilidad_report] Excel=> &report_path./&file_prefix..xlsx;
    %put NOTE: [estabilidad_report] Images=> &images_path./;

%mend _estabilidad_report;
