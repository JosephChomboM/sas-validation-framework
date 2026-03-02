/* =========================================================================
   correlacion_report.sas — Generación de reportes HTML + Excel

   Lee las tablas de WORK generadas por correlacion_compute:
     work._corr_pearson
     work._corr_spearman

   Genera:
     <report_path>/<file_prefix>.html  — ambas matrices con semáforo
     <report_path>/<file_prefix>.xlsx  — hoja Pearson + hoja Spearman

   Codificación de colores (semáforo por valor absoluto):
     |r| < 0.5   → lightgreen  (correlación débil)
     0.5 ≤ |r| < 0.6 → yellow (correlación moderada)
     |r| ≥ 0.6   → red        (correlación fuerte)
   ========================================================================= */

%macro _correlacion_report(report_path=, file_prefix=);

  /* ---- Formato semáforo de correlación -------------------------------- */
  proc format;
    value CorrSignif
      -0.5 -< 0.0  = "lightgreen"
       0.0 -< 0.5  = "lightgreen"
      -0.6 -<-0.5  = "yellow"
       0.5 -< 0.6  = "yellow"
       low -<-0.6  = "red"
       0.6 - high  = "red"
    ;
  run;

  /* ---- HTML report ---------------------------------------------------- */
  ods graphics on / outputfmt=svg;
  ods html5 file="&report_path./&file_prefix..html";

  proc print data=work._corr_pearson(drop=_type_ rename=(_name_=Variable))
             style(column)={backgroundcolor=CorrSignif.} noobs;
    title "Correlation Matrix (Pearson) — &file_prefix.";
  run;

  proc print data=work._corr_spearman(drop=_type_ rename=(_name_=Variable))
             style(column)={backgroundcolor=CorrSignif.} noobs;
    title "Correlation Matrix (Spearman) — &file_prefix.";
  run;

  ods html5 close;
  ods graphics off;

  /* ---- Excel report --------------------------------------------------- */
  ods excel file="&report_path./&file_prefix..xlsx"
            options(sheet_name="Pearson" sheet_interval="none" embedded_titles="yes");

  proc print data=work._corr_pearson(drop=_type_ rename=(_name_=Variable))
             style(column)={backgroundcolor=CorrSignif.} noobs;
    title "Correlation Matrix (Pearson)";
  run;

  ods excel options(sheet_name="Spearman" sheet_interval="now" embedded_titles="yes");

  proc print data=work._corr_spearman(drop=_type_ rename=(_name_=Variable))
             style(column)={backgroundcolor=CorrSignif.} noobs;
    title "Correlation Matrix (Spearman)";
  run;

  ods excel close;

  title;

  %put NOTE: [correlacion_report] HTML => &report_path./&file_prefix..html;
  %put NOTE: [correlacion_report] Excel => &report_path./&file_prefix..xlsx;

%mend _correlacion_report;
