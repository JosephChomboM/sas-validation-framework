/* =========================================================================
bivariado_report.sas - Reportes HTML + Excel + JPEG para Bivariado

Genera:
- un unico HTML por scope
- un unico Excel por scope
- un grafico temporal consolidado por variable
========================================================================= */

%macro _biv_report_section(detail_table=, catalog_table=, byvar=, oot_min_mes=,
    image_prefix=, section_title=);

    %local _nvars _idx _var _var_list _tipo_var _valor_label _xaxis_label;

    proc sql noprint;
        select Variable into :_var_list separated by '|'
        from &catalog_table.
        group by Variable, Tipo_Orden
        order by Tipo_Orden, Variable;
    quit;

    %let _nvars=%sysfunc(countw(%superq(_var_list), |));
    %if %sysevalf(%superq(_nvars)=, boolean) %then %let _nvars=0;

    %do _idx=1 %to &_nvars.;
        %let _var=%scan(%superq(_var_list), &_idx., |);
        %let _tipo_var=;
        %let _valor_label=&_var.;
        %let _xaxis_label=Categorias;

        proc sql noprint;
            select distinct Tipo_Variable into :_tipo_var trimmed
            from &catalog_table.
            where Variable="&_var.";
        quit;

        %if %upcase(%superq(_tipo_var))=NUMERICA %then
            %let _xaxis_label=Buckets variable;

        title "&section_title. - &_var.";
        proc print data=&detail_table.(where=(Variable="&_var.")) noobs label;
            var Variable Valor Ventana N Pct_Cuentas Defaults RD;
            format Pct_Cuentas percent8.2 RD percent8.2;
            label Variable="Variable"
                  Valor="&_valor_label."
                  Ventana="Ventana";
        run;
        title;

        ods graphics / imagename="&image_prefix._&_idx." imagefmt=jpeg;
        %if %upcase(%superq(_tipo_var))=NUMERICA %then %do;
            title "Bivariado por bucket - &_var.";
            title2 "Buckets definidos en TRAIN y aplicados a OOT.";
        %end;
        %else %do;
            title "Bivariado por categoria - &_var.";
            title2 "Comparacion TRAIN vs OOT sobre categorias observadas.";
        %end;
        proc sgplot data=&detail_table.(where=(Variable="&_var."));
            vbar Valor / response=Pct_Cuentas group=Ventana
                groupdisplay=cluster nooutline transparency=0.15
                name="bars";
            vline Valor / response=RD group=Ventana y2axis markers
                markerattrs=(symbol=circlefilled)
                name="lines";
            keylegend "bars" / title="Ventana";
            xaxis type=discrete discreteorder=data label="&_xaxis_label.";
            yaxis label="% Cuentas";
            y2axis min=0 label="RD" valuesformat=percent8.2;
        run;
        title;
        title2;
    %end;

%mend _biv_report_section;

%macro _bivariado_report(byvar=, oot_min_mes=, report_path=, images_path=,
    file_prefix=);

    %local _dir_rc _has_drivers;

    %put NOTE: [bivariado_report] Generando reporte unificado...;
    %put NOTE: [bivariado_report] report_path=&report_path.;
    %put NOTE: [bivariado_report] images_path=&images_path.;
    %put NOTE: [bivariado_report] file_prefix=&file_prefix.;

    %let _dir_rc=%sysfunc(dcreate(METOD4.3, &report_path./../));
    %let _dir_rc=%sysfunc(dcreate(., &report_path.));
    %let _dir_rc=%sysfunc(dcreate(METOD4.3, &images_path./../));
    %let _dir_rc=%sysfunc(dcreate(., &images_path.));

    %let _has_drivers=0;
    proc sql noprint;
        select count(*) into :_has_drivers trimmed
        from work._biv_driver_report;
    quit;

    ods graphics on;
    ods listing gpath="&images_path.";

    ods html5 file="&report_path./&file_prefix..html"
        options(bitmap_mode="inline");
    ods excel file="&report_path./&file_prefix..xlsx"
        options(sheet_name="VARIABLES" sheet_interval="none"
        embedded_titles="yes");

    %_biv_report_section(detail_table=work._biv_main_report,
        catalog_table=work._biv_main_catalog,
        byvar=&byvar., oot_min_mes=&oot_min_mes.,
        image_prefix=&file_prefix._main, section_title=Variables Principales);

    %if &_has_drivers. > 0 %then %do;
        ods excel options(sheet_name="DRIVERS" sheet_interval="now"
            embedded_titles="yes");

        %_biv_report_section(detail_table=work._biv_driver_report,
            catalog_table=work._biv_driver_catalog,
            byvar=&byvar., oot_min_mes=&oot_min_mes.,
            image_prefix=&file_prefix._drv, section_title=Drivers);
    %end;

    ods excel close;
    ods html5 close;
    ods graphics / reset=all;
    ods graphics off;

    %put NOTE: [bivariado_report] HTML=> &report_path./&file_prefix..html;
    %put NOTE: [bivariado_report] Excel=> &report_path./&file_prefix..xlsx;
    %put NOTE: [bivariado_report] Images=> &images_path./;

%mend _bivariado_report;
