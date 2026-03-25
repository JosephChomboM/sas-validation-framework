/* =========================================================================
target_report.sas - Reporte consolidado TRAIN + OOT para Target

Genera un unico HTML y un unico Excel para cada contexto, con metricas y
graficos consolidados entre TRAIN y OOT.
========================================================================= */

%macro _target_report(input_caslib=, train_table=, oot_table=, byvar=, target=,
    monto_var=, def_cld=0, has_monto=0, report_path=, images_path=,
    file_prefix=);

    %global _tgt_global_avg _tgt_min_val _tgt_max_val;

    %put NOTE: [target_report] Generando reporte consolidado TRAIN + OOT.;
    %put NOTE: [target_report] byvar=&byvar. target=&target. def_cld=&def_cld.;
    %put NOTE: [target_report] has_monto=&has_monto.;

    %_target_compute(input_caslib=&input_caslib., train_table=&train_table.,
        oot_table=&oot_table., byvar=&byvar., target=&target.,
        monto_var=&monto_var., def_cld=&def_cld., has_monto=&has_monto.);

    ods graphics on;
    ods listing gpath="&images_path.";

    ods excel file="&report_path./&file_prefix..xlsx"
        options(sheet_name="RD" sheet_interval="none" embedded_titles="yes");
    ods html5 file="&report_path./&file_prefix..html"
        options(bitmap_mode="inline");

    ods graphics / imagename="&file_prefix._rd" imagefmt=jpeg;
    title "Evolutivo del target consolidado";
    proc sgplot data=casuser._tgt_describe;
        series x=&byvar. y=avg_target / group=Muestra markers
            lineattrs=(thickness=2);
        xaxis type=discrete label="Periodo";
        yaxis label="RD";
        keylegend / title="Muestra";
    run;
    title;

    title "Resumen mensual del target";
    proc print data=casuser._tgt_describe noobs label;
        var Muestra &byvar. N avg_target;
        label Muestra="Muestra"
            &byvar.="Periodo"
            N="N Target"
            avg_target="RD";
        format avg_target percent10.4;
    run;
    title;

    ods excel options(sheet_name="Materialidad" sheet_interval="now"
        embedded_titles="yes");
    title "Materialidad del target por muestra";
    proc freqtab data=casuser._tgt_base;
        tables Muestra * &byvar. * &target. / norow nocol nopercent nocum;
    run;
    title;

    ods excel options(sheet_name="DiferenciaRel" sheet_interval="now"
        embedded_titles="yes");
    title "Diferencia relativa";
    proc print data=casuser._tgt_diff_rel noobs label;
        var Muestra Metric Value;
        label Muestra="Muestra"
            Metric="Metrica"
            Value="Valor";
        format Value percent10.4;
    run;
    title;

    ods excel options(sheet_name="Bandas_RD" sheet_interval="now"
        embedded_titles="yes");
    ods graphics / imagename="&file_prefix._bandas_rd" imagefmt=jpeg;
    title "RD con bandas TRAIN";
    proc sgplot data=casuser._tgt_bandas;
        band x=&byvar. lower=lower_band upper=upper_band /
            transparency=0.45 fillattrs=(color=cxD9D9D9)
            legendlabel="Bandas TRAIN +/- 2 desv";
        series x=&byvar. y=avg_target / group=Muestra markers
            lineattrs=(thickness=2);
        refline &_tgt_global_avg. /
            lineattrs=(color=red pattern=shortdash thickness=2);
        xaxis type=discrete label="Periodo";
        yaxis min=&_tgt_min_val. max=&_tgt_max_val. label="RD";
        keylegend / title="Serie";
    run;
    title;

    title "Tabla de bandas RD";
    proc print data=casuser._tgt_bandas noobs label;
        var Muestra &byvar. avg_target lower_band upper_band global_avg;
        label Muestra="Muestra"
            &byvar.="Periodo"
            avg_target="RD"
            lower_band="Limite Inferior"
            upper_band="Limite Superior"
            global_avg="Promedio TRAIN";
        format avg_target lower_band upper_band global_avg percent10.4;
    run;
    title;

    %if &has_monto.=1 %then %do;
        ods excel options(sheet_name="PondProm" sheet_interval="now"
            embedded_titles="yes");
        ods graphics / imagename="&file_prefix._pond_prom" imagefmt=jpeg;
        title "RD ponderado por monto";
        proc sgplot data=casuser._tgt_pond_prom_bandas;
            band x=&byvar. lower=lower_band upper=upper_band /
                transparency=0.45 fillattrs=(color=cxD9D9D9)
                legendlabel="Bandas TRAIN +/- 2 desv";
            series x=&byvar. y=avg_target_pond / group=Muestra markers
                lineattrs=(thickness=2);
            refline &_tgt_global_pond_mean. /
                lineattrs=(color=red pattern=shortdash thickness=2);
            xaxis type=discrete label="Periodo";
            yaxis label="RD ponderado";
            keylegend / title="Serie";
        run;
        title;

        proc print data=casuser._tgt_pond_prom_bandas noobs label;
            var Muestra &byvar. avg_target_pond lower_band upper_band global_mean;
            label Muestra="Muestra"
                &byvar.="Periodo"
                avg_target_pond="RD Ponderado"
                lower_band="Limite Inferior"
                upper_band="Limite Superior"
                global_mean="Promedio TRAIN";
            format avg_target_pond lower_band upper_band global_mean percent10.4;
        run;

        ods excel options(sheet_name="PondSuma" sheet_interval="now"
            embedded_titles="yes");
        ods graphics / imagename="&file_prefix._pond_suma" imagefmt=jpeg;
        title "Suma ponderada por monto";
        proc sgplot data=casuser._tgt_sum_pond_bandas;
            band x=&byvar. lower=lower_band upper=upper_band /
                transparency=0.45 fillattrs=(color=cxD9D9D9)
                legendlabel="Bandas TRAIN +/- 2 desv";
            series x=&byvar. y=sum_target_pond / group=Muestra markers
                lineattrs=(thickness=2);
            refline &_tgt_global_sum_mean. /
                lineattrs=(color=red pattern=shortdash thickness=2);
            xaxis type=discrete label="Periodo";
            yaxis label="Suma ponderada";
            keylegend / title="Serie";
        run;
        title;

        proc print data=casuser._tgt_sum_pond_bandas noobs label;
            var Muestra &byvar. sum_target_pond total_monto lower_band upper_band global_mean;
            label Muestra="Muestra"
                &byvar.="Periodo"
                sum_target_pond="Suma Target*Monto"
                total_monto="Monto Total"
                lower_band="Limite Inferior"
                upper_band="Limite Superior"
                global_mean="Promedio TRAIN";
            format sum_target_pond total_monto lower_band upper_band global_mean comma18.2;
        run;

        ods excel options(sheet_name="RatioMonto" sheet_interval="now"
            embedded_titles="yes");
        ods graphics / imagename="&file_prefix._ratio" imagefmt=jpeg;
        title "Ratio target sobre monto";
        proc sgplot data=casuser._tgt_ratio_bandas;
            band x=&byvar. lower=lower_band upper=upper_band /
                transparency=0.45 fillattrs=(color=cxD9D9D9)
                legendlabel="Bandas TRAIN +/- 2 desv";
            series x=&byvar. y=ratio_default_monto / group=Muestra markers
                lineattrs=(thickness=2);
            refline &_tgt_global_ratio_mean. /
                lineattrs=(color=red pattern=shortdash thickness=2);
            xaxis type=discrete label="Periodo";
            yaxis label="Ratio";
            keylegend / title="Serie";
        run;
        title;

        proc print data=casuser._tgt_ratio_bandas noobs label;
            var Muestra &byvar. ratio_default_monto lower_band upper_band global_mean;
            label Muestra="Muestra"
                &byvar.="Periodo"
                ratio_default_monto="Ratio"
                lower_band="Limite Inferior"
                upper_band="Limite Superior"
                global_mean="Promedio TRAIN";
            format ratio_default_monto lower_band upper_band global_mean percent10.4;
        run;
    %end;

    ods excel close;
    ods html5 close;
    ods graphics / reset=all;
    ods graphics off;

    %put NOTE: [target_report] HTML => &report_path./&file_prefix..html;
    %put NOTE: [target_report] Excel => &report_path./&file_prefix..xlsx;
    %put NOTE: [target_report] Images => &images_path./;

%mend _target_report;
