/* =========================================================================
universe_report.sas - Generación de reportes HTML + Excel para Universe

Genera:
<report_path>/<prefix>_train.html  - HTML con gráficos TRAIN
<report_path>/<prefix>_oot.html    - HTML con gráficos OOT
<report_path>/<prefix>.xlsx        - Excel multi-hoja (TRAIN + OOT)

El reporte combina TRAIN y OOT en un solo Excel:
- Hoja TRAIN_DescribeUniverso
- Hoja OOT_DescribeUniverso

Las bandas ±2σ se calculan desde TRAIN y se aplican a OOT
(vía macrovars globales _univ_mean, _univ_std en universe_compute).

Migrado de universe_legacy.sas (__describe_universe_report).
========================================================================= */
%macro _universe_report( input_caslib=, train_table=, oot_table=, byvar=, id_var
    =, monto_var=, report_path=, file_prefix=);

    %put NOTE: [universe_report] Generando reportes...;
    %put NOTE: [universe_report] report_path=&report_path.;
    %put NOTE: [universe_report] file_prefix=&file_prefix.;

    /* ---- Copiar tablas CAS a WORK para procesamiento local -------------- */
    data work._univ_train;
        set &input_caslib..&train_table.;
    run;

    data work._univ_oot;
        set &input_caslib..&oot_table.;
    run;

    /* ---- Crear directorio de reportes si no existe ---------------------- */
    %local _dir_rc;
    %let _dir_rc=%sysfunc(dcreate(metod_1_1, &report_path./../));
    %let _dir_rc=%sysfunc(dcreate(., &report_path.));

    /* ---- Detectar si monto está disponible ------------------------------ */
    %local _has_monto;
    %let _has_monto=0;
    %if %length(%superq(monto_var)) > 0 %then %do;
        %let _has_monto=1;
    %end;

    /* ==================================================================
    TRAIN: HTML + primera hoja Excel
    ================================================================== */
    ods graphics on / outputfmt=svg;

    ods html5 file="&report_path./&file_prefix._train.html";
    ods excel file="&report_path./&file_prefix..xlsx"
        options(sheet_name="TRAIN_DescribeUniverso" sheet_interval="none"
        embedded_titles="yes");

    %_univ_describe_id(data=work._univ_train, byvar=&byvar., id_var=&id_var.);

    %if &_has_monto.=1 %then %do;
        %_univ_describe_monto(data=work._univ_train, monto_var=&monto_var.,
            byvar=&byvar.);
    %end;

    %_univ_bandas_cuentas(data=work._univ_train, byvar=&byvar., id_var=&id_var.,
        is_train=1);

    %if &_has_monto.=1 %then %do;
        %_univ_evolutivo_monto(data=work._univ_train, monto_var=&monto_var.,
            byvar=&byvar.);
    %end;

    ods html5 close;

    /* ==================================================================
    OOT: HTML + segunda hoja Excel
    ================================================================== */
    ods html5 file="&report_path./&file_prefix._oot.html";
    ods excel options(sheet_name="OOT_DescribeUniverso" sheet_interval="now"
        embedded_titles="yes");

    %_univ_describe_id(data=work._univ_oot, byvar=&byvar., id_var=&id_var.);

    %if &_has_monto.=1 %then %do;
        %_univ_describe_monto(data=work._univ_oot, monto_var=&monto_var.,
            byvar=&byvar.);
    %end;

    %_univ_bandas_cuentas(data=work._univ_oot, byvar=&byvar., id_var=&id_var.,
        is_train=0);

    %if &_has_monto.=1 %then %do;
        %_univ_evolutivo_monto(data=work._univ_oot, monto_var=&monto_var.,
            byvar=&byvar.);
    %end;

    ods excel close;
    ods html5 close;
    ods graphics off;

    /* ---- Cleanup -------------------------------------------------------- */
    proc datasets library=work nolist nowarn;
        delete _univ_train _univ_oot;
    quit;

    %put NOTE: [universe_report] HTML TRAIN=>
        &report_path./&file_prefix._train.html;
    %put NOTE: [universe_report] HTML OOT=>
        &report_path./&file_prefix._oot.html;
    %put NOTE: [universe_report] Excel=> &report_path./&file_prefix..xlsx;

%mend _universe_report;
