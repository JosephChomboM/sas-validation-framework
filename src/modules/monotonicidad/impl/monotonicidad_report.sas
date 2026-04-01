/* =========================================================================
monotonicidad_report.sas - Reporte legacy-style HTML + Excel + JPEG para METOD7

Consume:
- casuser._mono_report_train
- casuser._mono_report_oot

Genera:
- <report_path>/<prefix>.html
- <report_path>/<prefix>.xlsx
- <images_path>/<prefix>_*.jpeg
========================================================================= */
%macro _mono_render_section(data_table=, split_label=, score_var=, target=,
    file_prefix=);

    %local _view_name;
    %let _view_name=_mono_view_%lowcase(&split_label.);

    %_mono_partition_cas(table_name=%scan(%superq(data_table), -1, .),
        orderby=%str({"Bucket_Order"}));

    data casuser.&_view_name.;
        set &data_table.;
    run;

    title "Granulado Score &score_var. - &split_label.";
    proc print data=casuser.&_view_name. noobs label;
        var Valor_X N Pct_Cuentas Mean_Default;
        format Pct_Cuentas percent8.2 Mean_Default percent8.2;
        label Valor_X="Bucket"
              N="Cuentas"
              Pct_Cuentas="% Cuentas"
              Mean_Default="Mean Default";
    run;
    title;

    ods graphics / imagename="&file_prefix._%lowcase(&split_label.)" imagefmt=jpeg;

    title "Granulado Score &score_var. - &split_label.";
    proc sgplot data=casuser.&_view_name.;
        keylegend / title=" " opaque;
        vbar Valor_X / response=Pct_Cuentas barwidth=.4 nooutline;
        vline Valor_X / response=Mean_Default markers
            markerattrs=(symbol=circlefilled) y2axis;
        yaxis label="% Cuentas (bar)" labelattrs=(size=8)
            valueattrs=(size=8);
        y2axis min=0 label="Mean &target." labelattrs=(size=8);
        xaxis type=discrete discreteorder=data label="Buckets &score_var."
            labelattrs=(size=8);
    run;
    title;

%mend _mono_render_section;

%macro _monotonicidad_report(score_var=, target=, report_path=, images_path=,
    file_prefix=);

    %local _dir_rc;

    %put NOTE: [monotonicidad_report] Generando reporte legacy-style...;
    %put NOTE: [monotonicidad_report] score_var=&score_var. target=&target.;

    %if %index(%upcase(&report_path.), %str(EXPERIMENTS))=0 %then %do;
        %let _dir_rc=%sysfunc(dcreate(METOD7, &report_path./../));
        %let _dir_rc=%sysfunc(dcreate(., &report_path.));
    %end;
    %if %index(%upcase(&images_path.), %str(EXPERIMENTS))=0 %then %do;
        %let _dir_rc=%sysfunc(dcreate(METOD7, &images_path./../));
        %let _dir_rc=%sysfunc(dcreate(., &images_path.));
    %end;

    ods graphics on;
    ods listing gpath="&images_path.";

    ods html5 file="&report_path./&file_prefix..html"
        options(bitmap_mode="inline");
    ods excel file="&report_path./&file_prefix..xlsx"
        options(sheet_name="Monotonicidad" sheet_interval="none"
        embedded_titles="yes");

    %_mono_render_section(data_table=casuser._mono_report_train,
        split_label=TRAIN, score_var=&score_var., target=&target.,
        file_prefix=&file_prefix.);

    %_mono_render_section(data_table=casuser._mono_report_oot,
        split_label=OOT, score_var=&score_var., target=&target.,
        file_prefix=&file_prefix.);

    ods excel close;
    ods html5 close;
    ods graphics / reset=all;
    ods graphics off;

    proc datasets library=casuser nolist nowarn;
        delete _mono_view_train _mono_view_oot;
    quit;

    %put NOTE: [monotonicidad_report] HTML=> &report_path./&file_prefix..html;
    %put NOTE: [monotonicidad_report] Excel=> &report_path./&file_prefix..xlsx;
    %put NOTE: [monotonicidad_report] Images=> &images_path./;

%mend _monotonicidad_report;
