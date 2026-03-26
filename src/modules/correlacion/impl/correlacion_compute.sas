/* =========================================================================
   correlacion_compute.sas - Cómputo de matrices de correlación

   Genera dos tablas en WORK:
     casuser._corr_pearson   - correlación de Pearson
     casuser._corr_spearman  - correlación de Spearman

   Ambas filtradas a _type_='CORR' (solo filas de correlación,
   sin N, MEAN, STD).

   Solo recibe variables numéricas.
   ========================================================================= */

%macro _corr_sort_cas(table_name=);

  %if %length(%superq(table_name))=0 %then %return;

  proc cas;
    session conn;
    table.partition /
      table={
        caslib="casuser",
        name="&table_name.",
        orderby={"_NAME_"},
        groupby={}
      },
      casout={
        caslib="casuser",
        name="&table_name.",
        replace=true
      };
  quit;

%mend _corr_sort_cas;

%macro _correlacion_compute(input_lib=, input_table=, variables=,
  pearson_table=_corr_pearson, spearman_table=_corr_spearman);

  %put NOTE: [correlacion_compute] Calculando Pearson y Spearman sobre
    &input_lib..&input_table..;

  proc corr data=&input_lib..&input_table.
            outp=casuser.&pearson_table.(where=(_type_='CORR'))
            noprint;
    var &variables.;
  run;

  proc corr data=&input_lib..&input_table.
            spearman
            outs=casuser.&spearman_table.(where=(_type_='CORR'))
            noprint;
    var &variables.;
  run;

  %_corr_sort_cas(table_name=&pearson_table.);
  %_corr_sort_cas(table_name=&spearman_table.);

  %put NOTE: [correlacion_compute] Pearson y Spearman calculados.;

%mend _correlacion_compute;
