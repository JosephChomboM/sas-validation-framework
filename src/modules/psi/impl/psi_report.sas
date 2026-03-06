/* =========================================================================
psi_report.sas - Generación de reportes HTML + Excel + Gráficos PNG

Lee las tablas de WORK generadas por psi_compute:
work._psi_cubo
work._psi_cubo_wide
work._psi_resumen

Genera:
<report_path>/<file_prefix>.html  - cubo + resumen con semáforo
<report_path>/<file_prefix>.xlsx  - hoja PSI, Cubo Wide, Resumen, Gráficos
<images_path>/<file_prefix>_tend_*.png  - tendencia temporal por variable

Codificación de colores (semáforo PSI):
PSI < 0.10       → lightgreen  (estable)
0.10 ≤ PSI < 0.25 → yellow     (alerta)
PSI ≥ 0.25       → red         (crítico)

Migrado de psi_legacy.sas (__psi_report + __plot_psi_tendencia).
========================================================================= */

/* =====================================================================
%_psi_plot_tendencia - Gráfico de tendencia temporal del PSI
Un gráfico por variable con bandas semáforo
===================================================================== */
%macro _psi_plot_tendencia(data=work._psi_cubo, byvar=, images_path=,
    file_prefix=);

    %local var_list n_vars i var_name;

    proc sql noprint;
        select distinct Variable into :var_list separated by '|' from &data.
            where Tipo="Mensual";

        select count(distinct Variable) into :n_vars trimmed from &data. where
            Tipo="Mensual";
    quit;

    %if &n_vars.=0 %then %do;
        %put NOTE: [psi_plot_tendencia] No hay datos mensuales para graficar
            tendencia.;
        %return;
    %end;

    ods listing gpath="&images_path.";

    %do i=1 %to &n_vars.;
        %let var_name=%scan(&var_list., &i., |);

        ods graphics / imagename="&file_prefix._tend_&var_name." imagefmt=png
            width=800px height=400px;

        title "PSI Temporal: &var_name.";

        proc sgplot data=&data.(where=(Variable="&var_name." and
            Tipo="Mensual"));
            band x=&byvar. lower=0 upper=0.10 / fillattrs=(color=lightgreen
                transparency=0.7) legendlabel="Estable (<0.10)";
            band x=&byvar. lower=0.10 upper=0.25 / fillattrs=(color=yellow
                transparency=0.7) legendlabel="Alerta (0.10-0.25)";
            band x=&byvar. lower=0.25 upper=0.3 / fillattrs=(color=lightcoral
                transparency=0.7) legendlabel="Crítico (>0.25)";

            series x=&byvar. y=PSI / lineattrs=(color=navy thickness=2) markers
                markerattrs=(symbol=circlefilled color=navy size=10);

            refline 0.10 / axis=y lineattrs=(color=orange pattern=dash
                thickness=1);
            refline 0.25 / axis=y lineattrs=(color=red pattern=dash
                thickness=1);

            xaxis label="&byvar." valueattrs=(size=8) type=discrete;
            yaxis label="PSI" min=0 valueattrs=(size=8);
        run;

        title;

        %put NOTE: [psi_plot_tendencia]=>
            &images_path./&file_prefix._tend_&var_name..png;
    %end;

    ods graphics / reset;

%mend _psi_plot_tendencia;

/* =====================================================================
%_psi_report - Generador principal de reportes (HTML + Excel + PNG)
Lee tablas de WORK generadas por _psi_compute
===================================================================== */
%macro _psi_report(report_path=, images_path=, file_prefix=, byvar=);

    /* ---- Formato semáforo PSI ------------------------------------------ */
    proc format;
        value PsiSignif -0.0 -< 0.1="lightgreen" 0.1 -< 0.25="yellow" 0.25 -<
            9999="red" ;
    run;

    /* ---- HTML report ---------------------------------------------------- */
    ods graphics on / outputfmt=svg;
    ods html5 file="&report_path./&file_prefix..html";

    proc print data=work._psi_cubo noobs label
        style(column)={backgroundcolor=PsiSignif.};
        title "CUBO PSI: Detalle por Variable y Periodo - &file_prefix.";
        footnote
            "Tipo: Mensual = PSI de ese mes vs TRAIN | Total = PSI OOT completo vs TRAIN";
        %if %length(%superq(byvar)) > 0 %then %do;
            var Variable &byvar. Tipo PSI;
        %end;
        %else %do;
            var Variable Tipo PSI;
        %end;
    run;

    proc print data=work._psi_resumen noobs
        style(column)={backgroundcolor=PsiSignif.};
        title "Resumen de Estabilidad PSI - &file_prefix.";
    run;

    ods html5 close;
    ods graphics off;
    title;
    footnote;

    /* ---- Excel report (multi-hoja) -------------------------------------- */
    ods excel file="&report_path./&file_prefix..xlsx"
        options(sheet_name="PSI_Detalle" sheet_interval="none"
        embedded_titles="yes");

    proc print data=work._psi_cubo noobs label
        style(column)={backgroundcolor=PsiSignif.};
        title "CUBO PSI: Detalle por Variable y Periodo";
        footnote
            "Tipo: Mensual = PSI de ese mes vs TRAIN | Total = PSI OOT completo vs TRAIN";
        %if %length(%superq(byvar)) > 0 %then %do;
            var Variable &byvar. Tipo PSI;
        %end;
        %else %do;
            var Variable Tipo PSI;
        %end;
    run;
    title;
    footnote;

    ods excel options(sheet_name="PSI_Cubo_Wide" sheet_interval="now"
        embedded_titles="yes");

    proc print data=work._psi_cubo_wide noobs
        style(column)={backgroundcolor=PsiSignif.};
        title "CUBO PSI: Variable x Mes";
    run;
    title;

    ods excel options(sheet_name="Resumen" sheet_interval="now"
        embedded_titles="yes");

    proc print data=work._psi_resumen noobs;
        title "Resumen de Estabilidad PSI";
        var Variable;
        var PSI_Total / style(data)={backgroundcolor=PsiSignif.};
        %if %length(%superq(byvar)) > 0 %then %do;
            var PSI_Min PSI_Max PSI_Mean PSI_Std Meses_Verde Meses_Amarillo
                Meses_Rojo Total_Meses Pct_Meses_Rojo;
            var PSI_Primer_Mes / style(data)={backgroundcolor=PsiSignif.};
            var PSI_Ultimo_Mes / style(data)={backgroundcolor=PsiSignif.};
            var Tendencia Alerta_Tendencia;
        %end;
        var Semaforo_Total;
    run;
    title;

    ods excel close;

    /* ---- Gráficos PNG: solo tendencia temporal -------------------------- */
    %if %length(%superq(byvar)) > 0 %then %do;
        %_psi_plot_tendencia( data=work._psi_cubo, byvar=&byvar.,
            images_path=&images_path., file_prefix=&file_prefix. );
    %end;

    %put NOTE: [psi_report] HTML=> &report_path./&file_prefix..html;
    %put NOTE: [psi_report] Excel=> &report_path./&file_prefix..xlsx;

%mend _psi_report;
