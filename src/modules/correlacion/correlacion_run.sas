/* =========================================================================
   correlacion_run.sas — Macro pública del módulo Correlación

   API:
     %correlacion_run(
       input_caslib  = PROC,
       input_table   = _active_input,
       output_caslib = OUT,
       troncal_id    = <id>,
       split         = train | oot,
       scope         = base | segNNN,
       run_id        = <run_id>
     )

   Flujo interno:
     1) Determinar modo (AUTO / CUSTOM) y resolver variables numéricas
        - AUTO   → cfg_segmentos / cfg_troncales (num_list / num_unv)
        - CUSTOM → &corr_custom_vars (definidas en Steps 07/10)
     2) Ejecutar contract (validaciones)
     3) Calcular matrices Pearson + Spearman
     4) Generar reportes HTML + Excel
        - AUTO   → outputs/runs/<run_id>/reports/   (validación estándar)
        - CUSTOM → outputs/runs/<run_id>/experiments/ (análisis exploratorio)
     5) Persistir tablas de correlación → CASLIB OUT
        - AUTO   → tables/   (estándar)
        - CUSTOM → experiments/  (exploratorio)
     6) Cleanup tablas temporales (work + CAS)

   Variables globales leídas (definidas en Steps 07/10):
     &corr_mode         — AUTO | CUSTOM
     &corr_custom_vars  — lista vars numéricas (solo si CUSTOM)

   Dependencias (cargadas por step 08/11 vía common_public.sas):
     - %_save_into_caslib (cas_utils.sas)

   Solo recibe variables numéricas.

   Compatibilidad: segmento y universo.
   ========================================================================= */

/* ---- Incluir componentes del módulo ----------------------------------- */
%include "&fw_root./src/modules/correlacion/correlacion_contract.sas";
%include "&fw_root./src/modules/correlacion/impl/correlacion_compute.sas";
%include "&fw_root./src/modules/correlacion/impl/correlacion_report.sas";

%macro correlacion_run(
    input_caslib  = PROC,
    input_table   = _active_input,
    output_caslib = OUT,
    troncal_id    =,
    split         =,
    scope         =,
    run_id        =
);

  %local _corr_rc _corr_vars _report_path _file_prefix _seg_num
         _corr_is_custom _table_subdir;

  %put NOTE: ======================================================;
  %put NOTE: [correlacion_run] INICIO;
  %put NOTE:   troncal=&troncal_id. split=&split. scope=&scope.;
  %put NOTE:   input=&input_caslib..&input_table.  output=&output_caslib.;
  %put NOTE:   mode=&corr_mode.;
  %put NOTE: ======================================================;

  /* ==================================================================
     1) Determinar modo y resolver variables numéricas
     ================================================================== */
  %let _corr_vars     = ;
  %let _corr_is_custom = 0;

  /* ------ Modo CUSTOM: variables personalizadas ---------------------- */
  %if %upcase(&corr_mode.) = CUSTOM %then %do;
    %if %length(%superq(corr_custom_vars)) > 0 %then %do;
      %let _corr_vars     = &corr_custom_vars.;
      %let _corr_is_custom = 1;
      %put NOTE: [correlacion_run] Modo CUSTOM — vars usuario: &_corr_vars.;
    %end;
    %else %do;
      %put WARNING: [correlacion_run] corr_mode=CUSTOM pero corr_custom_vars vacía. Fallback a AUTO.;
    %end;
  %end;

  /* ------ Modo AUTO (o fallback): variables de configuración --------- */
  %if &_corr_is_custom. = 0 %then %do;
    %put NOTE: [correlacion_run] Modo AUTO — resolviendo vars desde config.;

    %if %substr(&scope., 1, 3) = seg %then %do;
      /* Extraer seg_id numérico del scope (segNNN → NNN) */
      %let _seg_num = %sysfunc(inputn(%substr(&scope., 4), best.));

      proc sql noprint;
        select strip(num_list) into :_corr_vars trimmed
        from casuser.cfg_segmentos
        where troncal_id = &troncal_id.
          and seg_id     = &_seg_num.;
      quit;
    %end;

    /* Fallback a num_unv del troncal si no hay override de segmento */
    %if %length(%superq(_corr_vars)) = 0 %then %do;
      proc sql noprint;
        select strip(num_unv) into :_corr_vars trimmed
        from casuser.cfg_troncales
        where troncal_id = &troncal_id.;
      quit;
    %end;
  %end;

  %put NOTE: [correlacion_run] Variables numéricas resueltas: &_corr_vars.;

  /* ==================================================================
     Determinar rutas de salida según modo
     AUTO   → reports/ + tables/       (validación estándar)
     CUSTOM → experiments/             (análisis exploratorio)
     ================================================================== */
  %if &_corr_is_custom. = 1 %then %do;
    %let _report_path = &fw_root./outputs/runs/&run_id./experiments;
    %let _table_subdir = experiments;
    %let _file_prefix  = custom_correlacion_troncal_&troncal_id._&split._&scope.;
    %put NOTE: [correlacion_run] Output → experiments/ (exploratorio);
  %end;
  %else %do;
    %let _report_path = &fw_root./outputs/runs/&run_id./reports;
    %let _table_subdir = tables;
    %let _file_prefix  = correlacion_troncal_&troncal_id._&split._&scope.;
    %put NOTE: [correlacion_run] Output → reports/ + tables/ (estándar);
  %end;

  /* ==================================================================
     2) Contract — validaciones pre-ejecución
     ================================================================== */
  %correlacion_contract(
    input_caslib = &input_caslib.,
    input_table  = &input_table.,
    variables    = &_corr_vars.
  );

  %if &_corr_rc. ne 0 %then %do;
    %put ERROR: [correlacion_run] Contract fallido — módulo abortado.;
    %return;
  %end;

  /* ==================================================================
     3) Compute — Pearson + Spearman → work tables
     ================================================================== */
  %_correlacion_compute(
    input_lib  = &input_caslib.,
    input_table= &input_table.,
    variables  = &_corr_vars.
  );

  /* ==================================================================
     4) Report — HTML + Excel
     ================================================================== */
  %_correlacion_report(
    report_path = &_report_path.,
    file_prefix = &_file_prefix.
  );

  /* ==================================================================
     5) Persistir tablas de correlación en CASLIB OUT
     ================================================================== */

  /* Mover Pearson de work a CAS OUT */
  data &output_caslib.._corr_prsn_tmp;
    set work._corr_pearson;
  run;

  %_save_into_caslib(
    m_cas_sess_name = conn,
    m_input_caslib  = &output_caslib.,
    m_input_data    = _corr_prsn_tmp,
    m_output_caslib = &output_caslib.,
    m_subdir_data   = &_table_subdir./&_file_prefix._pearson
  );

  proc cas;
    table.dropTable / caslib="&output_caslib." name="_corr_prsn_tmp" quiet=true;
  quit;

  /* Mover Spearman de work a CAS OUT */
  data &output_caslib.._corr_sprm_tmp;
    set work._corr_spearman;
  run;

  %_save_into_caslib(
    m_cas_sess_name = conn,
    m_input_caslib  = &output_caslib.,
    m_input_data    = _corr_sprm_tmp,
    m_output_caslib = &output_caslib.,
    m_subdir_data   = &_table_subdir./&_file_prefix._spearman
  );

  proc cas;
    table.dropTable / caslib="&output_caslib." name="_corr_sprm_tmp" quiet=true;
  quit;

  /* ==================================================================
     6) Cleanup — eliminar tablas temporales de work
     ================================================================== */
  proc datasets library=work nolist;
    delete _corr_pearson _corr_spearman;
  run;

  %put NOTE: ======================================================;
  %put NOTE: [correlacion_run] FIN — &_file_prefix. (mode=&corr_mode.);
  %put NOTE: ======================================================;

%mend correlacion_run;
