/* =========================================================================
segmentacion_report.sas - Reportes HTML + Excel + JPEG para Segmentacion

Genera:
<report_path>/<prefix>.html   - HTML con graficos y resultados
<report_path>/<prefix>.xlsx   - Excel multi-hoja:
    Hoja 1: MATERIALIDAD_GLOBAL (suficiencia global)
    Hoja 2: MATERIALIDAD_SEGMENTO (suficiencia por segmento, si aplica)
    Hoja 3: HETEROGENEIDAD_KS (test KS entre pares, si aplica)
    Hoja 4: KRUSKAL_WALLIS (test KW, si aplica)
    Hoja 5: MIGRACION (tipos + resumen + heatmap, si aplica)
    Hoja 6: GRAFICOS (distribucion mensual por segmento)
<images_path>/<prefix>_*.jpeg  - Graficos JPEG independientes

Consume tablas de work generadas por _seg_compute.

Convencion ODS: JPEG, bitmap_mode=inline, dual ODS (HTML5 + Excel),
reset=all al cerrar (ver design.md 7.9).
========================================================================= */

/* =====================================================================
%_seg_get_ticks - Helper para etiquetas de eje x en PROC SGPLOT

Genera macro vars con valores de byvar y etiquetas para display
(muestra solo cada N-esima etiqueta para evitar solapamiento).

Parametros:
data     - dataset de entrada
byvar    - variable temporal
out_vals - nombre de macrovar para valores (default: _seg_ticks)
out_disp - nombre de macrovar para display labels (default: _seg_tdisp)
===================================================================== */
%macro _seg_get_ticks(data=, byvar=, out_vals=_seg_ticks,
    out_disp=_seg_tdisp);

    %global &out_vals. &out_disp.;

    proc sql noprint;
        select distinct &byvar. into :&out_vals. separated by ' '
        from &data.;
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

/* =====================================================================
%_seg_plot_distrib - Grafico de distribucion mensual por segmento

sep=0: combinado vbar + vline (dual y-axis)
sep=1: graficos separados (vbar, vline, cluster)
===================================================================== */
%macro _seg_plot_distrib(data=, target=, byvar=, segvar=, sep=0);

    %_seg_get_ticks(data=&data., byvar=&byvar.);

    %if &sep. = 0 %then %do;

        title "Distribucion mensual por &segvar.";
        proc sgplot data=&data.;
            vbar &byvar. / nooutline group=&segvar.;
            vline &byvar. / response=&target. group=&segvar.
                markers stat=mean
                markerattrs=(symbol=circlefilled)
                y2axis;
            yaxis label="Cuentas";
            y2axis min=0 label="Mean &target."
                valuesformat=percentn8.0;
            xaxis values=(&_seg_ticks.)
                valuesdisplay=(&_seg_tdisp.);
        run;
        title;

    %end;
    %else %do;

        /* Calcular distribucion porcentual */
        proc sql;
            create table work._seg_rpt_dist as
            select &segvar., &byvar.,
                count(*) as cuentas,
                (select count(*) from &data.
                 where &byvar. = a.&byvar.) as total_por_periodo
            from &data. as a
            group by &byvar., &segvar.;
        quit;

        proc sql;
            create table work._seg_rpt_distpct as
            select *, round((cuentas / total_por_periodo) * 100, 0.01) as pct
            from work._seg_rpt_dist;
        quit;

        title "Distribucion de cuentas por &segvar.";
        proc sgplot data=&data.;
            vbar &byvar. / nooutline group=&segvar.;
            yaxis label="Cuentas";
            xaxis values=(&_seg_ticks.)
                valuesdisplay=(&_seg_tdisp.);
        run;
        title;

        title "Mean &target. por segmento &segvar.";
        proc sgplot data=&data.;
            vline &byvar. / response=&target. group=&segvar.
                markers stat=mean
                markerattrs=(symbol=circlefilled);
            yaxis min=0 label="Mean &target."
                valuesformat=percentn8.0;
            xaxis type=discrete values=(&_seg_ticks.)
                valuesdisplay=(&_seg_tdisp.);
        run;
        title;

        title "Distribucion porcentual por &segvar.";
        proc sgplot data=work._seg_rpt_distpct;
            vbar &byvar. / response=cuentas group=&segvar.
                groupdisplay=cluster datalabel=pct;
            xaxis display=(nolabel);
            yaxis label="Cuentas";
        run;
        title;

        proc datasets lib=work nolist nowarn;
            delete _seg_rpt_dist _seg_rpt_distpct;
        quit;

    %end;

%mend _seg_plot_distrib;

/* =====================================================================
%_seg_report - Orquestador de reportes

Parametros:
report_path  - Directorio para HTML + Excel
images_path  - Directorio para JPEG independientes
file_prefix  - Prefijo de nombre de archivos
data         - Dataset de trabajo (work)
target       - Variable target
byvar        - Variable temporal
segvar       - Variable segmentadora
has_segm     - 1 si hay segmentos, 0 si no
data_type    - TRAIN u OOT
plot_sep     - 0=combinado, 1=separado (graficos distribucion)
===================================================================== */
%macro _seg_report(report_path=, images_path=, file_prefix=, data=,
    target=, byvar=, segvar=, has_segm=0, data_type=, plot_sep=0);

    %put NOTE: [seg_report] Generando reportes...;
    %put NOTE: [seg_report] report_path=&report_path.;
    %put NOTE: [seg_report] file_prefix=&file_prefix.;

    /* ---- Formatos condicionales para proc print ----------------------- */
    proc format;
        value $statusmtd
            'CUMPLE'     = 'LightGreen'
            'NO CUMPLE'  = 'LightRed';
        value $statusks
            'DIFERENTES' = 'LightGreen'
            'SIMILARES'  = 'LightRed';
    run;

    /* ---- Abrir destinos ODS (HTML5 + Excel simultaneo) ---------------- */
    ods graphics on;
    ods listing gpath="&images_path.";

    ods html5 file="&report_path./&file_prefix..html"
        options(bitmap_mode="inline");
    ods excel file="&report_path./&file_prefix..xlsx"
        options(sheet_name="MATERIALIDAD_GLOBAL" sheet_interval="none"
        embedded_titles="yes");

    /* ==================================================================
    Hoja 1: MATERIALIDAD_GLOBAL - Suficiencia global
    ================================================================== */
    ods graphics / imagename="&file_prefix._mtd_global" imagefmt=jpeg;

    title "Validacion de Suficiencia Global - &data_type.";
    proc print data=work._seg_mtd_global noobs label;
        var Tipo_Muestra Materialidad;
        var Verif_Materialidad / style={background=$statusmtd.};
        var Cantidad_Target;
        var Verif_Target / style={background=$statusmtd.};
        label Cantidad_Target = 'Total Default';
    run;
    title;

    /* ==================================================================
    Hoja 2: MATERIALIDAD_SEGMENTO - Suficiencia por segmento
    ================================================================== */
    %if &has_segm. = 1 %then %do;
        ods excel options(sheet_name="MATERIALIDAD_SEGMENTO"
            sheet_interval="now");

        title "Materialidad de Segmentos - &data_type.";
        proc print data=work._seg_mtd_segm noobs label;
            var Segmento Materialidad;
            var Verif_Materialidad / style={background=$statusmtd.};
            var Cantidad_Target;
            var Verif_Target / style={background=$statusmtd.};
            label Cantidad_Target = 'Total Default';
        run;
        title;

        title "Resumen de Cumplimiento - &data_type.";
        proc print data=work._seg_mtd_resumen noobs label;
        run;
        title;
    %end;

    /* ==================================================================
    Hoja 3: HETEROGENEIDAD_KS - Test KS entre pares de segmentos
    ================================================================== */
    %if &has_segm. = 1 %then %do;
        ods excel options(sheet_name="HETEROGENEIDAD_KS"
            sheet_interval="now");
        ods graphics / imagename="&file_prefix._ks" imagefmt=jpeg;

        title "Test de Heterogeneidad KS entre Segmentos - &data_type.";
        proc print data=work._seg_ks_results noobs;
            var Segmento1 Segmento2 KS_Statistic D_Statistic P_Value;
            var Prueba_KS / style={background=$statusks.};
            format P_Value pvalue6.4 KS_Statistic D_Statistic 6.4;
        run;
        title;

        title "Resumen Heterogeneidad - &data_type.";
        proc print data=work._seg_ks_resumen noobs;
        run;
        title;
    %end;

    /* ==================================================================
    Hoja 4: KRUSKAL_WALLIS - Test de diferencias entre segmentos
    ================================================================== */
    %if &has_segm. = 1 %then %do;
        ods excel options(sheet_name="KRUSKAL_WALLIS"
            sheet_interval="now");

        title "Medias de &target. por Segmento y Periodo";
        proc print data=work._seg_kw_means noobs;
        run;
        title;

        title "Test de Kruskal-Wallis";
        proc print data=work._seg_kw_test noobs;
        run;
        title;
    %end;

    /* ==================================================================
    Hoja 5: MIGRACION - Analisis de migracion de segmentos
    ================================================================== */
    %if &has_segm. = 1 and %sysfunc(exist(work._seg_mig_tipos))
    %then %do;
        ods excel options(sheet_name="MIGRACION" sheet_interval="now");
        ods graphics / imagename="&file_prefix._mig" imagefmt=jpeg;

        title "Distribucion por Tipo de Cliente - &data_type.";
        proc print data=work._seg_mig_tipos noobs;
        run;
        title;

        title "Migracion de Segmentos - &data_type.";
        proc print data=work._seg_mig_resumen noobs;
            var Segmento Cant_Retirados Pct_Retirados Cant_Nuevos Pct_Nuevos;
        run;
        title;

        /* Heatmap de migracion */
        proc template;
            define statgraph _seg_migration_heatmap;
                begingraph;
                    entrytitle "Migracion entre Segmentos - &data_type.";
                    layout overlay /
                        xaxisopts=(label="Segmento Inicial")
                        yaxisopts=(label="Segmento Final");
                        heatmapparm x=seg_primer_mes y=seg_ultimo_mes
                            colorresponse=Percent /
                            name="heatmap" colormodel=ThreeColorRamp
                            primary=true display=all;
                        continuouslegend "heatmap" / title="Porcentaje";
                    endlayout;
                endgraph;
            end;
        run;

        proc sgrender data=work._seg_mig_cruce
            template=_seg_migration_heatmap;
        run;
    %end;

    /* ==================================================================
    Hoja 6: GRAFICOS - Distribucion mensual por segmento
    ================================================================== */
    %if &has_segm. = 1 %then %do;
        ods excel options(sheet_name="GRAFICOS" sheet_interval="now");
        ods graphics / imagename="&file_prefix._dist" imagefmt=jpeg;

        %_seg_plot_distrib(data=&data., target=&target., byvar=&byvar.,
            segvar=&segvar., sep=&plot_sep.);
    %end;

    /* ---- Cerrar destinos ODS ------------------------------------------ */
    ods excel close;
    ods html5 close;
    ods graphics / reset=all;
    ods graphics off;

    %put NOTE: [seg_report] HTML => &report_path./&file_prefix..html;
    %put NOTE: [seg_report] Excel => &report_path./&file_prefix..xlsx;
    %put NOTE: [seg_report] Images => &images_path./;

%mend _seg_report;
