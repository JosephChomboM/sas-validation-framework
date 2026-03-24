/* =========================================================================
target_report.sas - Reporte combinado TRAIN y OOT para Target
========================================================================= */
%macro _target_report(input_caslib=, train_table=, oot_table=, byvar=, target=,
    monto_var=, def_cld=0, report_path=, images_path=, file_prefix=);

    %local _dir_rc _has_monto;

    %put NOTE: [target_report] Generando reporte combinado TRAIN/OOT.;
    %put NOTE: [target_report] def_cld=&def_cld.;

    %let _dir_rc=%sysfunc(dcreate(METOD2.1, &report_path./../));
    %let _dir_rc=%sysfunc(dcreate(., &report_path.));
    %let _dir_rc=%sysfunc(dcreate(METOD2.1, &images_path./../));
    %let _dir_rc=%sysfunc(dcreate(., &images_path.));

    %let _has_monto=0;
    %if %length(%superq(monto_var)) > 0 %then %let _has_monto=1;

    %_target_prepare_base(train_data=&input_caslib..&train_table.,
        oot_data=&input_caslib..&oot_table., byvar=&byvar., def_cld=&def_cld.);
    %_target_build_describe(target=&target., byvar=&byvar.);
    %_target_build_bandas(target=&target., byvar=&byvar.);

    %if &_has_monto.=1 %then %do;
        %_target_build_ponderado_promedio(target=&target., monto=&monto_var.,
            byvar=&byvar.);
        %_target_build_ponderado_suma(target=&target., monto=&monto_var.,
            byvar=&byvar.);
    %end;

    ods graphics on;
    ods listing gpath="&images_path.";

    ods html5 file="&report_path./&file_prefix..html"
        options(bitmap_mode="inline");
    ods excel file="&report_path./&file_prefix..xlsx"
        options(sheet_name="Target" sheet_interval="none"
        embedded_titles="yes");

    ods graphics / imagename="&file_prefix._describe" imagefmt=jpeg;
    title "Evolutivo del target - TRAIN y OOT";
    proc sgplot data=casuser._tgt_describe;
        series x=&byvar. y=avg_target / group=Muestra markers
            lineattrs=(thickness=2);
        xaxis type=discrete label="&byvar.";
        yaxis label="Promedio del target";
        keylegend / title="Muestra";
    run;
    title;

    title "Resumen mensual del target";
    proc print data=casuser._tgt_describe noobs label;
        var Muestra &byvar. N avg_target;
        label Muestra="Muestra"
            &byvar.="Periodo"
            N="N"
            avg_target="AVG_TARGET";
    run;
    title;

    title "Materialidad del target";
    proc print data=casuser._tgt_materialidad noobs label;
        var Muestra &byvar. Valor_Target N;
        label Muestra="Muestra"
            &byvar.="Periodo"
            Valor_Target="Valor target"
            N="N";
    run;
    title;

    title "Diferencia relativa del target";
    proc print data=casuser._tgt_diff_rel noobs label;
        var Muestra Metric Value;
        label Muestra="Muestra"
            Metric="Metrica"
            Value="Valor";
    run;
    title;

    ods excel options(sheet_name="Bandas" sheet_interval="now"
        embedded_titles="yes");
    ods graphics / imagename="&file_prefix._bandas" imagefmt=jpeg;
    title "Bandas del target con referencia TRAIN";
    proc sgplot data=casuser._tgt_bandas;
        band x=&byvar. lower=lower_band upper=upper_band /
            transparency=0.45 fillattrs=(color=cxD9D9D9)
            legendlabel="Bandas TRAIN +/- 2 desv";
        series x=&byvar. y=avg_target / group=Muestra markers
            lineattrs=(thickness=2);
        series x=&byvar. y=global_avg /
            lineattrs=(color=red pattern=shortdash thickness=2)
            legendlabel="Promedio global TRAIN";
        xaxis type=discrete label="&byvar.";
        yaxis label="Promedio del target";
        keylegend / title="Serie";
    run;
    title;

    title "Tabla de bandas del target";
    proc print data=casuser._tgt_bandas noobs label;
        var Muestra &byvar. avg_target lower_band upper_band global_avg;
        label Muestra="Muestra"
            &byvar.="Periodo"
            avg_target="AVG_TARGET"
            lower_band="LOWER_BAND"
            upper_band="UPPER_BAND"
            global_avg="GLOBAL_AVG";
    run;
    title;

    %if &_has_monto.=1 %then %do;
        %if %sysfunc(exist(casuser._tgt_pond_prom_bandas,DATA)) %then %do;
            ods excel options(sheet_name="PondProm" sheet_interval="now"
                embedded_titles="yes");
            ods graphics / imagename="&file_prefix._pond_prom" imagefmt=jpeg;
            title "Target ponderado por monto promedio";
            proc sgplot data=casuser._tgt_pond_prom_bandas;
                band x=&byvar. lower=lower_band upper=upper_band /
                    transparency=0.45 fillattrs=(color=cxD9D9D9)
                    legendlabel="Bandas TRAIN +/- 2 desv";
                series x=&byvar. y=avg_target_pond / group=Muestra markers
                    lineattrs=(thickness=2);
                series x=&byvar. y=global_mean /
                    lineattrs=(color=red pattern=shortdash thickness=2)
                    legendlabel="Promedio global TRAIN";
                xaxis type=discrete label="&byvar.";
                yaxis label="Target ponderado promedio";
                keylegend / title="Serie";
            run;
            title;

            proc print data=casuser._tgt_pond_prom_bandas noobs label;
                var Muestra &byvar. avg_target_pond lower_band upper_band
                    global_mean;
                label Muestra="Muestra"
                    &byvar.="Periodo"
                    avg_target_pond="AVG_TARGET_POND"
                    lower_band="LOWER_BAND"
                    upper_band="UPPER_BAND"
                    global_mean="GLOBAL_MEAN";
            run;
        %end;

        %if %sysfunc(exist(casuser._tgt_sum_pond_bandas,DATA)) %then %do;
            ods excel options(sheet_name="PondSuma" sheet_interval="now"
                embedded_titles="yes");
            ods graphics / imagename="&file_prefix._pond_suma" imagefmt=jpeg;
            title "Target ponderado por suma de monto";
            proc sgplot data=casuser._tgt_sum_pond_bandas;
                band x=&byvar. lower=lower_band upper=upper_band /
                    transparency=0.45 fillattrs=(color=cxD9D9D9)
                    legendlabel="Bandas TRAIN +/- 2 desv";
                series x=&byvar. y=sum_target_pond / group=Muestra markers
                    lineattrs=(thickness=2);
                series x=&byvar. y=global_mean /
                    lineattrs=(color=red pattern=shortdash thickness=2)
                    legendlabel="Promedio global TRAIN";
                xaxis type=discrete label="&byvar.";
                yaxis label="Target ponderado suma";
                keylegend / title="Serie";
            run;
            title;

            proc print data=casuser._tgt_sum_pond_bandas noobs label;
                var Muestra &byvar. sum_target_pond total_monto lower_band
                    upper_band global_mean;
                label Muestra="Muestra"
                    &byvar.="Periodo"
                    sum_target_pond="SUM_TARGET_POND"
                    total_monto="TOTAL_MONTO"
                    lower_band="LOWER_BAND"
                    upper_band="UPPER_BAND"
                    global_mean="GLOBAL_MEAN";
            run;
        %end;

        %if %sysfunc(exist(casuser._tgt_ratio_bandas,DATA)) %then %do;
            ods excel options(sheet_name="Ratio" sheet_interval="now"
                embedded_titles="yes");
            ods graphics / imagename="&file_prefix._ratio" imagefmt=jpeg;
            title "Ratio target sobre monto";
            proc sgplot data=casuser._tgt_ratio_bandas;
                band x=&byvar. lower=lower_band upper=upper_band /
                    transparency=0.45 fillattrs=(color=cxD9D9D9)
                    legendlabel="Bandas TRAIN +/- 2 desv";
                series x=&byvar. y=ratio_default_monto / group=Muestra markers
                    lineattrs=(thickness=2);
                series x=&byvar. y=global_mean /
                    lineattrs=(color=red pattern=shortdash thickness=2)
                    legendlabel="Promedio global TRAIN";
                xaxis type=discrete label="&byvar.";
                yaxis label="Ratio target / monto";
                keylegend / title="Serie";
            run;
            title;

            proc print data=casuser._tgt_ratio_bandas noobs label;
                var Muestra &byvar. ratio_default_monto lower_band upper_band
                    global_mean;
                label Muestra="Muestra"
                    &byvar.="Periodo"
                    ratio_default_monto="RATIO_DEFAULT_MONTO"
                    lower_band="LOWER_BAND"
                    upper_band="UPPER_BAND"
                    global_mean="GLOBAL_MEAN";
            run;
        %end;
    %end;

    ods excel close;
    ods html5 close;
    ods graphics / reset=all;
    ods graphics off;

    proc datasets library=casuser nolist nowarn;
        delete _tgt_:;
    quit;

    %put NOTE: [target_report] HTML=> &report_path./&file_prefix..html;
    %put NOTE: [target_report] Excel=> &report_path./&file_prefix..xlsx;
    %put NOTE: [target_report] Images=> &images_path./;

%mend _target_report;
