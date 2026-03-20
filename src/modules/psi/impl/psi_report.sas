/* =========================================================================
psi_report.sas - Generación de reportes HTML + Excel + Gráficos JPEG

Lee las tablas de casuser (CAS) generadas por psi_compute:
casuser._psi_cubo
casuser._psi_cubo_wide
casuser._psi_resumen

Genera:
<report_path>/<file_prefix>.html              - cubo + resumen con semáforo
<report_path>/<file_prefix>.xlsx              - hojas: Detalle, Wide, Resumen, Graficos
<images_path>/<file_prefix>_tend_*.jpeg       - tendencia temporal por variable

Codificación de colores (semáforo PSI):
PSI < 0.10       → lightgreen  (estable)
0.10 ≤ PSI < 0.25 → yellow     (alerta)
PSI ≥ 0.25       → red         (crítico)

Convención ODS: JPEG, bitmap_mode=inline, reset=all.
Los gráficos van tanto al Excel (hoja Graficos) como a JPEG independientes.
========================================================================= */

/* =====================================================================
%_psi_plot_tendencia - Gráfico de tendencia temporal del PSI
Un gráfico por variable con bandas semáforo.
Se ejecuta dentro de un contexto ODS ya abierto (Excel + listing).
===================================================================== */
%macro _psi_plot_tendencia(data=casuser._psi_cubo, byvar=, file_prefix=);

    %local var_list n_vars i var_name;

    proc sql noprint;
        select distinct Variable into :var_list separated by '|' from &data.
            where Tipo="Mensual";

        select count(distinct Variable) into :n_vars trimmed from &data. where
            Tipo="Mensual";
    quit;

    %if &n_vars.=0 %then %do;
        %put NOTE: [psi_plot_tendencia] No hay datos mensuales para graficar.;
        %return;
    %end;

    %do i=1 %to &n_vars.;
        %let var_name=%scan(&var_list., &i., |);

        ods graphics / imagename="&file_prefix._tend_&var_name." imagefmt=jpeg
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
    %end;

%mend _psi_plot_tendencia;

%macro _psi_render_detalle(data=, byvar=, title_text=, footnote_text=);
    proc print data=&data. noobs label
        style(column)={backgroundcolor=PsiSignif.};
        title "&title_text.";
        footnote "&footnote_text.";
        %if %length(%superq(byvar)) > 0 %then %do;
            var Variable &byvar. Tipo PSI;
        %end;
        %else %do;
            var Variable Tipo PSI;
        %end;
    run;
    title;
    footnote;
%mend _psi_render_detalle;

%macro _psi_render_wide(data=);
    proc print data=&data. noobs
        style(column)={backgroundcolor=PsiSignif.};
        title "CUBO PSI: Variable x Mes";
    run;
    title;
    footnote;
%mend _psi_render_wide;

%macro _psi_render_resumen(data=, byvar=, title_text=);
    proc print data=&data. noobs;
        title "&title_text.";
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
    footnote;
%mend _psi_render_resumen;

/* =====================================================================
%_psi_report - Generador principal de reportes (HTML + Excel + JPEG)
Lee tablas de casuser (CAS) generadas por _psi_compute.
Los gráficos se renderizan en la hoja "Graficos" del Excel Y como
archivos JPEG independientes (vía ods listing gpath).
===================================================================== */
%macro _psi_report(report_path=, images_path=, file_prefix=, byvar=);

    /* ---- Crear directorios METOD4.2 si no existen ----------------------- */
    %local _dir_rc _detalle_title _detalle_footnote _resumen_title;
    %let _dir_rc=%sysfunc(dcreate(METOD4.2, &report_path./../));
    %let _dir_rc=%sysfunc(dcreate(., &report_path.));
    %let _dir_rc=%sysfunc(dcreate(METOD4.2, &images_path./../));
    %let _dir_rc=%sysfunc(dcreate(., &images_path.));
    %let _detalle_title=CUBO PSI: Detalle por Variable y Periodo;
    %let _detalle_footnote=Tipo: Mensual = PSI de ese mes vs TRAIN | Total = PSI OOT completo vs TRAIN;
    %let _resumen_title=Resumen de Estabilidad PSI;

    /* ---- Formato semáforo PSI ------------------------------------------ */
    proc format;
        value PsiSignif -0.0 -< 0.1="lightgreen" 0.1 -< 0.25="yellow" 0.25 -<
            9999="red" ;
    run;

    %if %length(%superq(byvar)) > 0 %then %do;
        proc transpose data=casuser._psi_cubo_wide out=_psi_detalle_tmp
            name=_mes_col;
            by Variable;
            var mes_:;
        run;

        data _psi_detalle_mensual_rpt;
            set _psi_detalle_tmp(rename=(COL1=PSI));
            length Tipo $15;
            &byvar.=input(scan(_mes_col, 2, '_'), best32.);
            Tipo="Mensual";
            keep Variable &byvar. Tipo PSI;
        run;

        proc sql;
            create table _psi_detalle_total_rpt as select Variable, &byvar., PSI,
                Tipo from casuser._psi_cubo where Tipo="Total";
        quit;

        data _psi_detalle_rpt;
            set _psi_detalle_mensual_rpt _psi_detalle_total_rpt;
        run;

        proc sort data=_psi_detalle_rpt;
            by Variable &byvar.;
        run;
    %end;
    %else %do;
        data _psi_detalle_rpt;
            set casuser._psi_cubo;
        run;
    %end;

    ods listing gpath="&images_path.";

    /* ==================================================================
    1) HTML report completo
    ================================================================== */
    ods graphics on;
    ods html5 file="&report_path./&file_prefix..html"
        options(bitmap_mode="inline");

    %_psi_render_detalle(data=_psi_detalle_rpt, byvar=&byvar.,
        title_text=&_detalle_title., footnote_text=&_detalle_footnote.);
    %_psi_render_wide(data=casuser._psi_cubo_wide);
    %_psi_render_resumen(data=casuser._psi_resumen, byvar=&byvar.,
        title_text=&_resumen_title.);

    %if %length(%superq(byvar)) > 0 %then %do;
        %_psi_plot_tendencia(data=casuser._psi_cubo, byvar=&byvar.,
            file_prefix=&file_prefix.);
    %end;

    ods html5 close;
    ods graphics / reset=all;
    ods graphics off;
    title;
    footnote;

    /* ==================================================================
    2) Excel report (tablas + gráficos en hoja "Graficos")
    ================================================================== */
    ods graphics on;
    ods excel file="&report_path./&file_prefix..xlsx"
        options(sheet_name="PSI_Detalle" sheet_interval="none"
        embedded_titles="yes");

    /* ---- Hoja 1: PSI Detalle ------------------------------------------ */
    %_psi_render_detalle(data=_psi_detalle_rpt, byvar=&byvar.,
        title_text=&_detalle_title., footnote_text=&_detalle_footnote.);

    /* ---- Hoja 2: PSI Cubo Wide ---------------------------------------- */
    ods excel options(sheet_name="PSI_Cubo_Wide" sheet_interval="now"
        embedded_titles="yes");

    %_psi_render_wide(data=casuser._psi_cubo_wide);

    /* ---- Hoja 3: Resumen ---------------------------------------------- */
    ods excel options(sheet_name="Resumen" sheet_interval="now"
        embedded_titles="yes");

    %_psi_render_resumen(data=casuser._psi_resumen, byvar=&byvar.,
        title_text=&_resumen_title.);

    /* ---- Hoja 4: Graficos (tendencia temporal) ------------------------ */
    %if %length(%superq(byvar)) > 0 %then %do;
        ods excel options(sheet_name="Graficos" sheet_interval="now"
            embedded_titles="yes");

        %_psi_plot_tendencia( data=casuser._psi_cubo, byvar=&byvar.,
            file_prefix=&file_prefix. );
    %end;

    ods excel close;
    ods graphics / reset=all;
    ods graphics off;

    proc datasets lib=work nolist nowarn;
        delete _psi_detalle_tmp _psi_detalle_mensual_rpt _psi_detalle_total_rpt
            _psi_detalle_rpt;
    quit;

    %put NOTE: [psi_report] HTML=> &report_path./&file_prefix..html;
    %put NOTE: [psi_report] Excel=> &report_path./&file_prefix..xlsx;
    %put NOTE: [psi_report] JPEG=> &images_path./;

%mend _psi_report;
