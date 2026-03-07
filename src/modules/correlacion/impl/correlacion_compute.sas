/* =========================================================================
   correlacion_compute.sas - Cómputo de matrices de correlación

   Genera dos tablas en WORK:
     casuser._corr_pearson   - correlación de Pearson
     casuser._corr_spearman  - correlación de Spearman

   Ambas filtradas a _type_='CORR' (solo filas de correlación,
   sin N, MEAN, STD).

   Solo recibe variables numéricas.
   ========================================================================= */

%macro _correlacion_compute(input_lib=, input_table=, variables=);

  %put NOTE: [correlacion_compute] Calculando Pearson y Spearman...;

  /* ---- Pearson -------------------------------------------------------- */
  proc corr data=&input_lib..&input_table.
            outp=casuser._corr_pearson(where=(_type_='CORR'))
            noprint;
    var &variables.;
  run;

  /* ---- Spearman ------------------------------------------------------- */
  proc corr data=&input_lib..&input_table.
            spearman
            outs=casuser._corr_spearman(where=(_type_='CORR'))
            noprint;
    var &variables.;
  run;

  %put NOTE: [correlacion_compute] Pearson y Spearman calculados.;

%mend _correlacion_compute;
