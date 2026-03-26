/* =========================================================================
segmentacion_report.sas - Reportes HTML + Excel + JPEG para Segmentacion

Genera un unico reporte consolidado y un unico grafico temporal continuo.
Los periodos TRAIN/OOT siguen siendo identificables de forma implicita a
traves de la columna Periodo y de la linea de corte en el grafico.
========================================================================= */

%macro _seg_get_ticks(data=, byvar=, out_vals=_seg_ticks,
    out_disp=_seg_tdisp);

    %global &out_vals. &out_disp.;

    proc sql noprint;
        select distinct &byvar. into :&out_vals. separated by ' '
        from &data.
        order by &byvar.;
    quit;

    %local n v t;
    %let n = 1;
    %let v = %scan(&&&out_vals., &n., %str( ));
    %let t = ;

    %do %while(%length(&v.) > 0);
        %if %sysfunc(mod(&n., 2)) = 0 %then %do;
            %if %length(&t.) = 0 %then %let t = %str(" ");
            %else %let t = %sysfunc(catx(%str( ), &t., %str(" ")));
        %end;
        %else %do;
            %if %length(&t.) = 0 %then %let t = "&v.";
            %else %let t = %sysfunc(catx(%str( ), &t., "&v."));
        %end;
        %let n = %eval(&n. + 1);
        %let v = %scan(&&&out_vals., &n., %str( ));
    %end;

    %let &out_disp. = &t.;

%mend _seg_get_ticks;

%macro _seg_plot_unificado(data=, target=, byvar=, segvar=, oot_min_mes=);

    %_seg_get_ticks(data=&data., byvar=&byvar.);

    title "Evolucion temporal consolidada por &segvar.";
    title2 "La linea roja marca el inicio del periodo OOT dentro del flujo continuo.";

    proc sgplot data=&data.;
        vbar &byvar. / nooutline group=&segvar.;
        vline &byvar. / response=&target. group=&segvar.
            stat=mean markers markerattrs=(symbol=circlefilled) y2axis;
        refline &oot_min_mes. / axis=x
            lineattrs=(color=red pattern=shortdash thickness=2)
            label=("Inicio OOT");
        yaxis label="Cuentas";
        y2axis min=0 label="Mean &target." valuesformat=percentn8.0;
        xaxis values=(&_seg_ticks.) valuesdisplay=(&_seg_tdisp.);
    run;

    title;
    title2;

%mend _seg_plot_unificado;

%macro _seg_report(report_path=, images_path=, file_prefix=, data=,
    target=, byvar=, segvar=, has_segm=0, has_id=0, oot_min_mes=,
    plot_sep=0);

    %local _has_ks _has_mig _has_kw;

    %put NOTE: [seg_report] Generando reportes consolidados...;
    %put NOTE: [seg_report] report_path=&report_path.;
    %put NOTE: [seg_report] file_prefix=&file_prefix.;

    %if &plot_sep. ne 0 %then
        %put NOTE: [seg_report] seg_plot_sep=&plot_sep. se ignora por compatibilidad; el modulo genera un unico grafico consolidado.;

    proc format;
        value $statusmtd
            'CUMPLE'     = 'LightGreen'
            'NO CUMPLE'  = 'LightRed';
        value $statusks
            'DIFERENTES' = 'LightGreen'
            'SIMILARES'  = 'LightRed';
    run;

    %let _has_ks = 0;
    %let _has_mig = 0;
    %let _has_kw = 0;

    proc sql noprint;
        select count(*) into :_has_ks trimmed
        from dictionary.tables
        where upcase(libname) = 'CASUSER'
          and upcase(memname) = '_SEG_KS_RESULTS';

        select count(*) into :_has_mig trimmed
        from dictionary.tables
        where upcase(libname) = 'CASUSER'
          and upcase(memname) = '_SEG_MIG_TIPOS';

        select count(*) into :_has_kw trimmed
        from dictionary.tables
        where upcase(libname) = 'CASUSER'
          and upcase(memname) = '_SEG_KW_TEST';
    quit;

    ods graphics on;
    ods listing gpath="&images_path.";

    ods html5 file="&report_path./&file_prefix..html"
        options(bitmap_mode="inline");
    ods excel file="&report_path./&file_prefix..xlsx"
        options(sheet_name="MATERIALIDAD_GLOBAL" sheet_interval="none"
        embedded_titles="yes");

    title "Validacion de Suficiencia Consolidada";
    proc print data=casuser._seg_mtd_global noobs label;
        var Periodo Materialidad;
        var Verif_Materialidad / style={background=$statusmtd.};
        var Cantidad_Target;
        var Verif_Target / style={background=$statusmtd.};
        label Cantidad_Target = 'Total Default';
    run;
    title;

    %if &has_segm. = 1 %then %do;
        ods excel options(sheet_name="MATERIALIDAD_SEGMENTO"
            sheet_interval="now");

        title "Materialidad de Segmentos - Vista Consolidada";
        proc print data=casuser._seg_mtd_segm noobs label;
            var Periodo Segmento Materialidad;
            var Verif_Materialidad / style={background=$statusmtd.};
            var Cantidad_Target;
            var Verif_Target / style={background=$statusmtd.};
            label Cantidad_Target = 'Total Default';
        run;
        title;

        title "Resumen de Cumplimiento por Periodo";
        proc print data=casuser._seg_mtd_resumen noobs label;
            format PCT_Cumplimiento percent8.2;
        run;
        title;
    %end;

    %if &has_segm. = 1 and &_has_ks. > 0 %then %do;
        ods excel options(sheet_name="HETEROGENEIDAD_KS"
            sheet_interval="now");

        title "Test KS entre Segmentos - Ventana Consolidada";
        proc print data=casuser._seg_ks_results noobs;
            var Tipo_Muestra Segmento1 Segmento2 KS_Statistic D_Statistic P_Value;
            var Prueba_KS / style={background=$statusks.};
            format P_Value pvalue6.4 KS_Statistic D_Statistic 6.4;
        run;
        title;

        title "Resumen Heterogeneidad";
        proc print data=casuser._seg_ks_resumen noobs;
            format Proporcion_Diferentes percent8.1;
        run;
        title;
    %end;

    %if &has_segm. = 1 %then %do;
        ods excel options(sheet_name="KRUSKAL_WALLIS"
            sheet_interval="now");

        title "Medias de &target. por Segmento y Periodo Temporal";
        proc print data=casuser._seg_kw_means noobs;
        run;
        title;

        %if &_has_kw. > 0 %then %do;
            title "Test de Kruskal-Wallis";
            proc print data=casuser._seg_kw_test noobs;
            run;
            title;
        %end;
    %end;

    %if &has_segm. = 1 and &has_id. = 1 and &_has_mig. > 0 %then %do;
        ods excel options(sheet_name="MIGRACION" sheet_interval="now");

        title "Distribucion por Tipo de Cliente";
        proc print data=casuser._seg_mig_tipos noobs;
            format PCT_Total 8.2;
        run;
        title;

        title "Migracion de Segmentos: primer mes TRAIN vs ultimo mes OOT";
        proc print data=casuser._seg_mig_resumen noobs;
            format Pct_Retirados Pct_Nuevos 8.2;
        run;
        title;

        title "Matriz de Cruce de Segmentos";
        proc print data=casuser._seg_mig_cruce noobs;
            format Percent 8.2;
        run;
        title;
    %end;

    %if &has_segm. = 1 %then %do;
        ods excel options(sheet_name="GRAFICOS" sheet_interval="now");
        ods graphics / imagename="&file_prefix._timeline" imagefmt=jpeg;

        %_seg_plot_unificado(data=&data., target=&target., byvar=&byvar.,
            segvar=&segvar., oot_min_mes=&oot_min_mes.);
    %end;

    ods excel close;
    ods html5 close;
    ods graphics / reset=all;
    ods graphics off;

    %put NOTE: [seg_report] HTML => &report_path./&file_prefix..html;
    %put NOTE: [seg_report] Excel => &report_path./&file_prefix..xlsx;
    %put NOTE: [seg_report] Image => &images_path./&file_prefix._timeline.jpeg;

%mend _seg_report;
