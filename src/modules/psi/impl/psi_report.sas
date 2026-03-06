/* =========================================================================
   psi_report.sas - Generación de reportes PSI

   Macros:
     %_psi_report          - Excel multi-hoja + HTML con semáforo
     %_psi_plot_tendencia  - Gráfico temporal por variable
     %_psi_plot_heatmap    - Heatmap de estabilidad
     %_psi_plot_resumen    - Gráfico resumen (rangos + total)

   Lee de work: _psi_cubo, _psi_cubo_wide, _psi_resumen

   Semáforo PSI:
     < 0.10  → lightgreen (estable)
     0.10–0.25 → yellow   (alerta)
     ≥ 0.25  → red        (crítico)

   Migrado desde psi_legacy.sas v2.3 (Joseph Chombo, 02/12/2025).
   ========================================================================= */


/* ------------------------------------------------------------------
   %_psi_plot_tendencia - Serie temporal PSI por variable
   ------------------------------------------------------------------ */
%macro _psi_plot_tendencia(image_path=, file_prefix=);

    %local _var_list _n_vars _i _var_name;

    proc sql noprint;
        select distinct Variable into :_var_list separated by '|'
        from _psi_cubo
        where Tipo = "Mensual";

        select count(distinct Variable) into :_n_vars trimmed
        from _psi_cubo
        where Tipo = "Mensual";
    quit;

    %if &_n_vars. = 0 %then %do;
        %put NOTE: [psi_report] No hay datos mensuales para graficar tendencia.;
        %return;
    %end;

    ods listing gpath="&image_path.";

    %do _i = 1 %to &_n_vars.;
        %let _var_name = %scan(&_var_list., &_i., |);

        ods graphics / imagename="&file_prefix._tend_&_var_name." imagefmt=png
            width=800px height=500px;

        title "PSI Temporal: &_var_name.";
        proc sgplot data=_psi_cubo(where=(Variable="&_var_name." and Tipo="Mensual"));
            band x=Periodo lower=0 upper=0.10 /
                fillattrs=(color=lightgreen transparency=0.7)
                legendlabel="Estable (<0.10)";
            band x=Periodo lower=0.10 upper=0.25 /
                fillattrs=(color=yellow transparency=0.7)
                legendlabel="Alerta (0.10-0.25)";
            band x=Periodo lower=0.25 upper=0.3 /
                fillattrs=(color=lightcoral transparency=0.7)
                legendlabel="Crítico (>0.25)";

            series x=Periodo y=PSI /
                lineattrs=(color=navy thickness=2)
                markers markerattrs=(symbol=circlefilled color=navy size=10);

            refline 0.10 / axis=y lineattrs=(color=orange pattern=dash thickness=1);
            refline 0.25 / axis=y lineattrs=(color=red pattern=dash thickness=1);

            xaxis label="Periodo" valueattrs=(size=8) type=discrete;
            yaxis label="PSI" min=0 valueattrs=(size=8);
        run;
        title;
    %end;

    ods graphics / reset;
    ods listing close;

%mend _psi_plot_tendencia;


/* ------------------------------------------------------------------
   %_psi_plot_heatmap - Heatmap Variable × Periodo
   ------------------------------------------------------------------ */
%macro _psi_plot_heatmap(image_path=, file_prefix=);

    %local _n_mensual;

    proc sql noprint;
        select count(*) into :_n_mensual trimmed
        from _psi_cubo
        where Tipo = "Mensual";
    quit;

    %if &_n_mensual. = 0 %then %do;
        %put NOTE: [psi_report] No hay datos mensuales para heatmap.;
        %return;
    %end;

    data _psi_heatmap_tmp;
        set _psi_cubo(where=(Tipo="Mensual"));
        if PSI < 0.10 then Semaforo = 1;
        else if PSI < 0.25 then Semaforo = 2;
        else Semaforo = 3;
        PSI_Label = put(PSI, 5.3);
    run;

    proc sort data=_psi_heatmap_tmp; by Variable Periodo; run;

    ods listing gpath="&image_path.";
    ods graphics / imagename="&file_prefix._heatmap" imagefmt=png
        width=900px height=600px;

    title "Heatmap de Estabilidad PSI";
    title2 "Verde: <0.10 | Amarillo: 0.10-0.25 | Rojo: >0.25";

    proc sgplot data=_psi_heatmap_tmp;
        heatmapparm x=Periodo y=Variable colorresponse=Semaforo /
            colormodel=(lightgreen yellow lightcoral)
            outline outlineattrs=(color=gray);
        text x=Periodo y=Variable text=PSI_Label /
            textattrs=(size=8 weight=bold);
        xaxis label="Periodo" valueattrs=(size=9);
        yaxis label="Variable" valueattrs=(size=8) discreteorder=data;
    run;
    title;

    ods graphics / reset;
    ods listing close;

    proc datasets lib=work nolist nowarn; delete _psi_heatmap_tmp; quit;

%mend _psi_plot_heatmap;


/* ------------------------------------------------------------------
   %_psi_plot_resumen - Barras resumen (rango min-max + PSI Total)
   ------------------------------------------------------------------ */
%macro _psi_plot_resumen(image_path=, file_prefix=);

    ods listing gpath="&image_path.";
    ods graphics / imagename="&file_prefix._resumen" imagefmt=png
        width=800px height=500px;

    title "Resumen PSI por Variable";
    title2 "PSI Total con rangos mensuales (min-max)";

    proc sgplot data=_psi_resumen;
        highlow y=Variable low=PSI_Min high=PSI_Max /
            type=bar fillattrs=(color=lightsteelblue)
            barwidth=0.6 legendlabel="Rango Mensual";
        scatter y=Variable x=PSI_Total /
            markerattrs=(symbol=diamondfilled color=navy size=12)
            legendlabel="PSI Total";
        refline 0.10 / axis=x lineattrs=(color=orange pattern=dash);
        refline 0.25 / axis=x lineattrs=(color=red pattern=dash);
        xaxis label="PSI" min=0;
        yaxis label="Variable" discreteorder=data;
    run;
    title;

    ods graphics / reset;
    ods listing close;

%mend _psi_plot_resumen;


/* ------------------------------------------------------------------
   %_psi_report - Reporte principal: Excel multi-hoja + HTML

   Parámetros:
     report_path  - ruta de salida para .xlsx y .html
     image_path   - ruta de salida para imágenes .png
     file_prefix  - prefijo para archivos (ej. psi_troncal_1_base)
     byvar        - nombre de variable temporal (para label; vacío si N/A)
   ------------------------------------------------------------------ */
%macro _psi_report(
    report_path =,
    image_path  =,
    file_prefix =,
    byvar       =
);

    %put NOTE: [psi_report] Generando reportes en &report_path.;

    /* ---- Formato semáforo PSI ---- */
    proc format;
        value PsiSignif
            low  -< 0.10  = "lightgreen"
            0.10 -< 0.25  = "yellow"
            0.25 - high    = "red"
        ;
    run;

    /* ==================================================================
       Excel con hojas: PSI Detalle | PSI Cubo Wide | Resumen | Gráficos
       ================================================================== */
    ods excel file="&report_path./&file_prefix..xlsx"
        options(embedded_titles="yes" embedded_footnotes="yes");

    /* --- HOJA 1: PSI Detalle --- */
    ods excel options(sheet_name="PSI" sheet_interval="none");
    title "CUBO PSI: Detalle por Variable y Periodo";
    footnote "Tipo: Mensual = PSI de ese periodo vs TRAIN | Total = PSI OOT completo vs TRAIN";

    proc print data=_psi_cubo noobs label
        style(column)={backgroundcolor=PsiSignif.};
        var Variable Periodo Tipo PSI;
    run;
    title; footnote;

    /* --- HOJA 2: Cubo Wide --- */
    ods excel options(sheet_name="PSI_CUBO" sheet_interval="now");
    title "CUBO PSI: Variable x Periodo";

    proc print data=_psi_cubo_wide noobs
        style(column)={backgroundcolor=PsiSignif.};
    run;
    title;

    /* --- HOJA 3: Resumen --- */
    ods excel options(sheet_name="RESUMEN" sheet_interval="now");
    title "Resumen de Estabilidad PSI";

    proc print data=_psi_resumen noobs;
        var Variable;
        var PSI_Total / style(data)={backgroundcolor=PsiSignif.};
        var PSI_Min PSI_Max PSI_Mean PSI_Std
            Meses_Verde Meses_Amarillo Meses_Rojo Total_Meses Pct_Meses_Rojo;
        var PSI_Primer_Mes / style(data)={backgroundcolor=PsiSignif.};
        var PSI_Ultimo_Mes / style(data)={backgroundcolor=PsiSignif.};
        var Tendencia Alerta_Tendencia;
    run;
    title;

    /* --- HOJA 4: Gráficos embebidos --- */
    ods excel options(sheet_name="GRAFICOS" sheet_interval="now");

    %_psi_plot_tendencia(image_path=&image_path., file_prefix=&file_prefix.);

    ods excel close;

    /* ==================================================================
       HTML con cubo + resumen (vista rápida)
       ================================================================== */
    ods html5 file="&report_path./&file_prefix..html"
        style=Htmlblue options(bitmap_mode="inline");

    title "PSI: Detalle por Variable y Periodo";
    proc print data=_psi_cubo noobs label
        style(column)={backgroundcolor=PsiSignif.};
        var Variable Periodo Tipo PSI;
    run;
    title;

    title "PSI: Resumen de Estabilidad";
    proc print data=_psi_resumen noobs;
        var Variable;
        var PSI_Total / style(data)={backgroundcolor=PsiSignif.};
        var PSI_Min PSI_Max PSI_Mean PSI_Std Semaforo_Total Alerta_Tendencia;
    run;
    title;

    ods html5 close;

    /* ==================================================================
       Gráficos standalone (heatmap + resumen)
       ================================================================== */
    %_psi_plot_heatmap(image_path=&image_path., file_prefix=&file_prefix.);
    %_psi_plot_resumen(image_path=&image_path., file_prefix=&file_prefix.);

    %put NOTE: [psi_report] Reportes generados: &file_prefix..xlsx / .html;

%mend _psi_report;
