/* =========================================================================
monotonicidad_report.sas - Reporte unificado HTML + Excel + JPEG para METOD7

Consume:
- casuser._mono_detail

Genera:
- <report_path>/<prefix>.html
- <report_path>/<prefix>.xlsx
- <images_path>/<prefix>_*.jpeg
========================================================================= */
%macro _monotonicidad_report(score_var=, target=, report_path=, images_path=,
    file_prefix=);

    %local _dir_rc;

    %put NOTE: [monotonicidad_report] Generando reporte unificado...;
    %put NOTE: [monotonicidad_report] score_var=&score_var. target=&target.;

    %if %index(%upcase(&report_path.), %str(EXPERIMENTS))=0 %then %do;
        %let _dir_rc=%sysfunc(dcreate(METOD7, &report_path./../));
        %let _dir_rc=%sysfunc(dcreate(., &report_path.));
    %end;
    %if %index(%upcase(&images_path.), %str(EXPERIMENTS))=0 %then %do;
        %let _dir_rc=%sysfunc(dcreate(METOD7, &images_path./../));
        %let _dir_rc=%sysfunc(dcreate(., &images_path.));
    %end;

    %_mono_partition_cas(table_name=_mono_detail,
        orderby=%str({"Run_Order", "Split_Order", "Bucket_Order"}));

    data casuser._mono_detail_view;
        set casuser._mono_detail;
    run;

    ods graphics on;
    ods listing gpath="&images_path.";

    ods html5 file="&report_path./&file_prefix..html"
        options(bitmap_mode="inline");
    ods excel file="&report_path./&file_prefix..xlsx"
        options(sheet_name="Monotonicidad" sheet_interval="none"
        embedded_titles="yes");

    title "Monotonicidad &score_var. - TRAIN vs OOT";
    proc print data=casuser._mono_detail_view noobs label;
        var Split Valor_X N Pct_Cuentas Mean_Default;
        format Pct_Cuentas percent8.2 Mean_Default percent8.2;
        label Valor_X="Bucket"
              Split="Ventana"
              N="Cuentas"
              Pct_Cuentas="% Cuentas"
              Mean_Default="Mean Default";
    run;
    title;

    ods graphics / imagename="&file_prefix._mono" imagefmt=jpeg;

    title "Monotonicidad por bucket - &score_var.";
    title2 "Buckets definidos en TRAIN y reutilizados en OOT.";
    proc sgplot data=casuser._mono_detail_view;
        vbar Valor_X / response=Pct_Cuentas group=Split
            groupdisplay=cluster nooutline transparency=0.15
            name="bars";
        vline Valor_X / response=Mean_Default group=Split y2axis markers
            markerattrs=(symbol=circlefilled)
            name="lines";
        keylegend "bars" "lines" / title="Ventana";
        xaxis type=discrete discreteorder=data label="Buckets &score_var.";
        yaxis label="% Cuentas";
        y2axis min=0 label="Mean &target." valuesformat=percent8.2;
    run;
    title;
    title2;

    ods excel close;
    ods html5 close;
    ods graphics / reset=all;
    ods graphics off;

    proc datasets library=casuser nolist nowarn;
        delete _mono_detail_view;
    quit;

    %put NOTE: [monotonicidad_report] HTML=> &report_path./&file_prefix..html;
    %put NOTE: [monotonicidad_report] Excel=> &report_path./&file_prefix..xlsx;
    %put NOTE: [monotonicidad_report] Images=> &images_path./;

%mend _monotonicidad_report;
