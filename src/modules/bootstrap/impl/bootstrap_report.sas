/* =========================================================================
bootstrap_report.sas - Reportes HTML + Excel + JPEG para Bootstrapping

Genera:
<report_path>/<prefix>.html          - HTML con graficos resumen + histogramas
<report_path>/<prefix>.xlsx          - Excel multi-hoja:
    Hoja 1: BOOTS_ITERACIONES (cubo wide)
    Hoja 2: BOOTS_CUBO (formato largo)
    Hoja 3: RESUMEN_ESTABILIDAD (stats + percentiles + alertas)
    Hoja 4: GRAFICOS (highlow resumen + histogramas por variable)
<images_path>/<prefix>_*.jpeg        - Graficos JPEG independientes

Consume tablas de work generadas por _boot_compute:
work._boot_tablaout, work._boot_cubo_wide, work._boot_report_final

Convencion ODS: JPEG, bitmap_mode=inline, dual ODS (HTML5 + Excel + listing),
reset=all al cerrar (ver design.md 7.9).
========================================================================= */

/* =====================================================================
%_boot_plot_resumen - Grafico highlow (p5-p95) con betas TRAIN/OOT
===================================================================== */
%macro _boot_plot_resumen(alias=);

    title "Bootstrapping: Intervalos Bootstrap vs Betas TRAIN/OOT &alias.";

    proc sgplot data=work._boot_report_final noautolegend;
        highlow x=Variable high=p95 low=p5 / type=bar
            fillattrs=(color=LIGHTSTEELBLUE) nooutline;
        scatter x=Variable y=beta_dev /
            markerattrs=(color=BLUE symbol=CircleFilled size=10)
            legendlabel="Beta TRAIN";
        scatter x=Variable y=beta_oot /
            markerattrs=(color=RED symbol=DiamondFilled size=10)
            legendlabel="Beta OOT";
        refline 0 / axis=y lineattrs=(color=gray pattern=dash);
        xaxis valueattrs=(size=8) labelattrs=(size=10) discreteorder=data;
    run;

    title;

%mend _boot_plot_resumen;

/* =====================================================================
%_boot_plot_by_var - Histograma + densidad por variable con
lineas de referencia (beta TRAIN y OOT)
===================================================================== */
%macro _boot_plot_by_var;

    %local n_vars var_list i var_name beta_dev beta_oot
        flag_signo pct_consist;

    proc sql noprint;
        select distinct Variable into :var_list separated by '|'
        from work._boot_tablaout;
        select count(distinct Variable) into :n_vars trimmed
        from work._boot_tablaout;
    quit;

    %do i = 1 %to &n_vars.;
        %let var_name = %scan(&var_list., &i., |);

        proc sql noprint;
            select beta_dev, beta_oot, flag_signo, pct_signo_consistente
            into :beta_dev trimmed, :beta_oot trimmed,
                 :flag_signo trimmed, :pct_consist trimmed
            from work._boot_report_final
            where Variable = "&var_name.";
        quit;

        title "Bootstrap - &var_name.";
        title2 "Estabilidad: &flag_signo. (&pct_consist. consistente)";

        proc sgplot data=work._boot_tablaout(where=(Variable="&var_name."));
            histogram Estimate / binwidth=0.01 transparency=0.3
                fillattrs=(color=steelblue);
            density Estimate / type=kernel
                lineattrs=(color=navy thickness=2);
            refline 0 / axis=x lineattrs=(color=gray pattern=dash thickness=1)
                label="Cero" labelattrs=(size=8);
            refline &beta_dev. / axis=x
                lineattrs=(color=blue thickness=2)
                label="Beta TRAIN" labelattrs=(size=8 color=blue);
            refline &beta_oot. / axis=x
                lineattrs=(color=red thickness=2 pattern=shortdash)
                label="Beta OOT" labelattrs=(size=8 color=red);
            xaxis label="Valor del Coeficiente (Beta)" labelattrs=(size=10);
            yaxis label="Frecuencia" labelattrs=(size=10);
        run;

        title;
    %end;

%mend _boot_plot_by_var;

/* =====================================================================
%_boot_report - Orquestador de reportes

Parametros:
report_path  - Directorio para HTML + Excel
images_path  - Directorio para JPEG independientes
file_prefix  - Prefijo de nombre de archivos
===================================================================== */
%macro _boot_report(report_path=, images_path=, file_prefix=);

    %put NOTE: [bootstrap_report] Generando reportes...;
    %put NOTE: [bootstrap_report] report_path=&report_path.;
    %put NOTE: [bootstrap_report] file_prefix=&file_prefix.;

    /* ---- Abrir destinos ODS (HTML5 + Excel + listing simultaneo) ------ */
    ods graphics on;
    ods listing gpath="&images_path.";

    ods html5 file="&report_path./&file_prefix..html"
        options(bitmap_mode="inline");
    ods excel file="&report_path./&file_prefix..xlsx"
        options(sheet_name="BOOTS_ITERACIONES" sheet_interval="none"
        embedded_titles="yes");

    /* ==================================================================
    Hoja 1: BOOTS_ITERACIONES - Formato wide para analisis
    ================================================================== */
    ods graphics / imagename="&file_prefix._iter" imagefmt=jpeg;

    title "Bootstrapping - Betas por Iteracion (Formato Wide)";
    proc print data=work._boot_cubo_wide noobs label;
    run;
    title;

    /* ==================================================================
    Hoja 2: BOOTS_CUBO - Formato largo para pivot tables
    ================================================================== */
    ods excel options(sheet_name="BOOTS_CUBO" sheet_interval="now");

    title "Bootstrapping - Betas por Iteracion (Formato Largo)";
    proc print data=work._boot_tablaout noobs;
    run;
    title;

    /* ==================================================================
    Hoja 3: RESUMEN_ESTABILIDAD - Estadisticas y alertas
    ================================================================== */
    ods excel options(sheet_name="RESUMEN_ESTABILIDAD" sheet_interval="now");

    title "Resumen de Estabilidad de Coeficientes";

    proc print data=work._boot_report_final noobs label;
        var Variable
            beta_dev beta_oot
            flag_signo pct_signo_consistente alerta_signo
            n_positivos n_negativos
            beta_mean beta_std
            p5 p25 p50 p75 p95
            beta_min beta_max
            pval_dev pval_oot
            peso_dev peso_oot;
        label
            Variable = "Variable"
            beta_dev = "Beta TRAIN"
            beta_oot = "Beta OOT"
            flag_signo = "Estabilidad Signo"
            pct_signo_consistente = "% Consistencia"
            alerta_signo = "Alerta"
            n_positivos = "# Iter Positivas"
            n_negativos = "# Iter Negativas"
            beta_mean = "Media Bootstrap"
            beta_std = "Desv Est Bootstrap"
            p5 = "Percentil 5"
            p25 = "Percentil 25"
            p50 = "Mediana"
            p75 = "Percentil 75"
            p95 = "Percentil 95"
            beta_min = "Minimo"
            beta_max = "Maximo"
            pval_dev = "P-Value TRAIN"
            pval_oot = "P-Value OOT"
            peso_dev = "Peso TRAIN"
            peso_oot = "Peso OOT";
    run;
    title;

    /* ==================================================================
    Hoja 4: GRAFICOS - Distribucion por variable
    ================================================================== */
    ods excel options(sheet_name="GRAFICOS" sheet_interval="now");

    /* Grafico resumen: highlow p5-p95 con betas TRAIN/OOT */
    ods graphics / imagename="&file_prefix._resumen" imagefmt=jpeg;
    %_boot_plot_resumen;

    /* Graficos individuales por variable */
    ods graphics / imagename="&file_prefix._hist" imagefmt=jpeg;
    %_boot_plot_by_var;

    /* ---- Cerrar destinos ODS ------------------------------------------ */
    ods excel close;
    ods html5 close;
    ods graphics / reset=all;
    ods graphics off;

    %put NOTE: [bootstrap_report] HTML => &report_path./&file_prefix..html;
    %put NOTE: [bootstrap_report] Excel => &report_path./&file_prefix..xlsx;
    %put NOTE: [bootstrap_report] Images => &images_path./;

%mend _boot_report;
