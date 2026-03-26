/* =========================================================================
bivariado_report.sas - Report-only layer for bivariado.
Consumes precomputed tables from compute:
- work._biv_main_report
- work._biv_driver_report
========================================================================= */

%macro _biv_render_run(detail_table=, run_order=, image_prefix=,
    section_title=);

    %local _var _tipo _x_label;

    proc sql noprint;
        select max(Variable),
               max(Tipo_Variable)
          into :_var trimmed,
               :_tipo trimmed
        from &detail_table.
        where Run_Order=&run_order.;
    quit;

    %if %length(%superq(_var))=0 %then %return;

    %if %upcase(%superq(_tipo))=NUMERICA %then %let _x_label=Buckets variable;
    %else %let _x_label=Categorias;

    proc sort data=&detail_table.(where=(Run_Order=&run_order.))
        out=work._biv_run_view;
        by Ventana_Orden Valor_X;
    run;

    proc sort data=&detail_table.(where=(Run_Order=&run_order.))
        out=work._biv_run_graph;
        by Valor_X Ventana_Orden;
    run;

    title "&section_title. - &_var.";
    proc print data=work._biv_run_view noobs label;
        var Variable Ventana Valor_X N Pct_Cuentas Defaults RD;
        format Pct_Cuentas percent8.2 RD percent8.2;
        label Valor_X="&_var."
              Ventana="Ventana";
    run;
    title;

    ods graphics / imagename="&image_prefix._&run_order." imagefmt=jpeg;

    %if %upcase(%superq(_tipo))=NUMERICA %then %do;
        title "Bivariado por bucket - &_var.";
        title2 "Buckets definidos en TRAIN y reutilizados en OOT.";
    %end;
    %else %do;
        title "Bivariado por categoria - &_var.";
        title2 "Categorias originales de la variable (sin buckets).";
    %end;

    proc sgplot data=work._biv_run_graph;
        vbar Valor_X / response=Pct_Cuentas group=Ventana
            groupdisplay=cluster nooutline transparency=0.15
            name="bars";
        vline Valor_X / response=RD group=Ventana y2axis markers
            markerattrs=(symbol=circlefilled)
            name="lines";
        keylegend "bars" / title="Ventana";
        xaxis type=discrete discreteorder=data label="&_x_label.";
        yaxis label="% Cuentas";
        y2axis min=0 label="RD" valuesformat=percent8.2;
    run;
    title;
    title2;

    proc datasets library=work nolist nowarn;
        delete _biv_run_view _biv_run_graph;
    quit;

%mend _biv_render_run;

%macro _biv_report_section(detail_table=, image_prefix=, section_title=);

    %local _runs _n_runs _i _run;

    proc sql noprint;
        select distinct Run_Order
          into :_runs separated by ' '
        from &detail_table.
        order by Run_Order;
    quit;

    %let _n_runs=%sysfunc(countw(%superq(_runs), %str( )));

    %do _i=1 %to &_n_runs.;
        %let _run=%scan(%superq(_runs), &_i., %str( ));
        %_biv_render_run(detail_table=&detail_table., run_order=&_run.,
            image_prefix=&image_prefix., section_title=&section_title.);
    %end;

%mend _biv_report_section;

%macro _bivariado_report(byvar=, oot_min_mes=, report_path=, images_path=,
    file_prefix=);

    %local _dir_rc _has_drivers;

    %put NOTE: [bivariado_report] Generando reporte...;
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
        image_prefix=&file_prefix._main,
        section_title=Variables_Principales);

    %if &_has_drivers. > 0 %then %do;
        ods excel options(sheet_name="DRIVERS" sheet_interval="now"
            embedded_titles="yes");

        %_biv_report_section(detail_table=work._biv_driver_report,
            image_prefix=&file_prefix._drv,
            section_title=Drivers);
    %end;

    ods excel close;
    ods html5 close;
    ods graphics / reset=all;
    ods graphics off;

    %put NOTE: [bivariado_report] HTML=> &report_path./&file_prefix..html;
    %put NOTE: [bivariado_report] Excel=> &report_path./&file_prefix..xlsx;
    %put NOTE: [bivariado_report] Images=> &images_path./;

%mend _bivariado_report;
