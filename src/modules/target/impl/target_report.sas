/* =========================================================================
target_report.sas - Generacion de reportes HTML + Excel + JPEG para Target

Genera:
<report_path>/<prefix>_train.html  - HTML con graficos TRAIN (inline)
<report_path>/<prefix>_oot.html    - HTML con graficos OOT (inline)
<report_path>/<prefix>.xlsx        - Excel multi-hoja (TRAIN + OOT)
<images_path>/<prefix>_*.jpeg      - Graficos como JPEG independientes

El reporte combina TRAIN y OOT en un solo Excel:
- Hoja TRAIN_DescribeTarget
- Hoja OOT_DescribeTarget

Las bandas +/-2s se calculan desde TRAIN y se aplican a OOT
(via macrovars globales _tgt_global_avg, _tgt_std_monthly, etc.).

Tablas temporales se crean en casuser (CAS) via PROC FEDSQL.
Formato de imagen: JPEG. HTML usa bitmap_mode=inline.
========================================================================= */
%macro _target_report( input_caslib=, train_table=, oot_table=, byvar=,
    target=, monto_var=, def_cld=0, report_path=, images_path=,
    file_prefix=);

    %put NOTE: [target_report] Generando reportes...;
    %put NOTE: [target_report] report_path=&report_path.;
    %put NOTE: [target_report] images_path=&images_path.;
    %put NOTE: [target_report] file_prefix=&file_prefix.;
    %put NOTE: [target_report] def_cld=&def_cld.;

    /* ---- Copiar tablas CAS a casuser (FEDSQL para CAS-to-CAS) --------- */
    proc fedsql sessref=conn;
        create table casuser._tgt_train {options replace=true} as
        select * from &input_caslib..&train_table.;
    quit;

    proc fedsql sessref=conn;
        create table casuser._tgt_oot {options replace=true} as
        select * from &input_caslib..&oot_table.;
    quit;

    /* ---- Crear directorios si no existen ------------------------------- */
    %local _dir_rc;
    %let _dir_rc=%sysfunc(dcreate(METOD2.1, &report_path./../));
    %let _dir_rc=%sysfunc(dcreate(., &report_path.));
    %let _dir_rc=%sysfunc(dcreate(METOD2.1, &images_path./../));
    %let _dir_rc=%sysfunc(dcreate(., &images_path.));

    /* ---- Detectar si monto esta disponible ------------------------------ */
    %local _has_monto;
    %let _has_monto=0;
    %if %length(%superq(monto_var)) > 0 %then %do;
        %let _has_monto=1;
    %end;

    /* ==================================================================
       TRAIN: HTML + primera hoja Excel + JPEG images
       ================================================================== */
    ods graphics on;
    ods listing gpath="&images_path.";

    ods html5 file="&report_path./&file_prefix._train.html"
        options(bitmap_mode="inline");
    ods excel file="&report_path./&file_prefix..xlsx"
        options(sheet_name="TRAIN_DescribeTarget" sheet_interval="none"
        embedded_titles="yes");
    ods graphics / imagename="&file_prefix._trn_rd" imagefmt=jpeg;

    %_target_describe(data=casuser._tgt_train, target=&target., byvar=&byvar.,
        def_cld=&def_cld.);

    ods graphics / imagename="&file_prefix._trn_bandas" imagefmt=jpeg;

    %_target_bandas(data=casuser._tgt_train, data_type=TRAIN, target=&target.,
        byvar=&byvar., is_train=1);

    %if &_has_monto.=1 %then %do;
        ods graphics / imagename="&file_prefix._trn_pond_prom" imagefmt=jpeg;

        %_target_ponderado_promedio(data=casuser._tgt_train, data_type=TRAIN,
            target=&target., monto=&monto_var., byvar=&byvar., is_train=1);

        ods graphics / imagename="&file_prefix._trn_pond_sum" imagefmt=jpeg;

        %_target_ponderado_suma(data=casuser._tgt_train, data_type=TRAIN,
            target=&target., monto=&monto_var., byvar=&byvar., is_train=1);
    %end;

    ods html5 close;
    ods graphics / reset=all;

    /* ==================================================================
       OOT: HTML + segunda hoja Excel
       ================================================================== */
    ods html5 file="&report_path./&file_prefix._oot.html"
        options(bitmap_mode="inline");
    ods excel options(sheet_name="OOT_DescribeTarget" sheet_interval="now"
        embedded_titles="yes");
    ods graphics / imagename="&file_prefix._oot_rd" imagefmt=jpeg;

    %_target_describe(data=casuser._tgt_oot, target=&target., byvar=&byvar.,
        def_cld=&def_cld.);

    ods graphics / imagename="&file_prefix._oot_bandas" imagefmt=jpeg;

    %_target_bandas(data=casuser._tgt_oot, data_type=OOT, target=&target.,
        byvar=&byvar., is_train=0);

    %if &_has_monto.=1 %then %do;
        ods graphics / imagename="&file_prefix._oot_pond_prom" imagefmt=jpeg;

        %_target_ponderado_promedio(data=casuser._tgt_oot, data_type=OOT,
            target=&target., monto=&monto_var., byvar=&byvar., is_train=0);

        ods graphics / imagename="&file_prefix._oot_pond_sum" imagefmt=jpeg;

        %_target_ponderado_suma(data=casuser._tgt_oot, data_type=OOT,
            target=&target., monto=&monto_var., byvar=&byvar., is_train=0);
    %end;

    ods excel close;
    ods html5 close;
    ods graphics / reset=all;
    ods graphics off;

    /* ---- Cleanup tablas temporales CAS --------------------------------- */
    proc datasets library=casuser nolist nowarn;
        delete _tgt_train _tgt_oot;
    quit;

    %put NOTE: [target_report] HTML TRAIN=>
        &report_path./&file_prefix._train.html;
    %put NOTE: [target_report] HTML OOT=>
        &report_path./&file_prefix._oot.html;
    %put NOTE: [target_report] Excel=> &report_path./&file_prefix..xlsx;
    %put NOTE: [target_report] Images=> &images_path./;

%mend _target_report;
