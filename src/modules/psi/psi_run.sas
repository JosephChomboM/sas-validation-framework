/* =========================================================================
   psi_run.sas — Macro pública del módulo PSI (Population Stability Index)

   API:
     %psi_run(
       input_caslib  = PROC,
       train_table   = _psi_train,
       oot_table     = _psi_oot,
       output_caslib = OUT,
       troncal_id    = <id>,
       scope         = base | segNNN,
       run_id        = <run_id>
     )

   Nota: PSI compara TRAIN vs OOT, por lo que recibe DOS tablas promovidas
   (no usa run_module.sas que promueve una sola). step_psi.sas maneja la
   promoción de ambas tablas.

   Flujo interno:
     1) Determinar modo (AUTO / CUSTOM) y resolver variables (num + cat + byvar)
        - AUTO   → cfg_segmentos / cfg_troncales (num_list, cat_list, mes_var)
        - CUSTOM → &psi_custom_vars_num, &psi_custom_vars_cat, &psi_custom_byvar
     2) Ejecutar contract (validaciones)
     3) Calcular PSI (cubo detalle, cubo wide, resumen con alertas)
     4) Generar reportes Excel + HTML + gráficos PNG
        - AUTO   → outputs/runs/<run_id>/reports/   + images/
        - CUSTOM → outputs/runs/<run_id>/experiments/
     5) Persistir tablas de resultados → .sas7bdat en disco
        - AUTO   → tables/   | CUSTOM → experiments/
     6) Cleanup tablas temporales (work)

   Variables globales leídas (definidas en step_psi.sas):
     &psi_mode            — AUTO | CUSTOM
     &psi_n_buckets       — número de bins (default 10)
     &psi_mensual         — 1 = breakdown mensual, 0 = solo total
     &psi_custom_vars_num — lista vars numéricas (solo si CUSTOM)
     &psi_custom_vars_cat — lista vars categóricas (solo si CUSTOM)
     &psi_custom_byvar    — variable temporal (solo si CUSTOM)

   Dependencias (cargadas por step_psi.sas vía common_public.sas):
     - Ninguna de cas_utils para outputs (usa libname SAS directo)

   Compatibilidad: segmento y universo.
   ========================================================================= */

/* ---- Incluir componentes del módulo ----------------------------------- */
%include "&fw_root./src/modules/psi/psi_contract.sas";
%include "&fw_root./src/modules/psi/impl/psi_compute.sas";
%include "&fw_root./src/modules/psi/impl/psi_report.sas";

%macro psi_run(
    input_caslib  = PROC,
    train_table   = _psi_train,
    oot_table     = _psi_oot,
    output_caslib = OUT,
    troncal_id    =,
    scope         =,
    run_id        =
);

  /* ---- Return code: owned here, used by contract -------------------- */
  %global _psi_rc;
  %let _psi_rc = 0;

  %local _vars_num _vars_cat _byvar
         _report_path _tables_path _image_path
         _file_prefix _tbl_prefix _seg_num _psi_is_custom
         _scope_abbr;

  %put NOTE: ======================================================;
  %put NOTE: [psi_run] INICIO;
  %put NOTE:   troncal=&troncal_id. scope=&scope.;
  %put NOTE:   train=&input_caslib..&train_table.;
  %put NOTE:   oot=&input_caslib..&oot_table.;
  %put NOTE:   output=&output_caslib.  mode=&psi_mode.;
  %put NOTE: ======================================================;

  /* ==================================================================
     1) Determinar modo y resolver variables
     ================================================================== */
  %let _vars_num     = ;
  %let _vars_cat     = ;
  %let _byvar        = ;
  %let _psi_is_custom = 0;

  /* ------ Modo CUSTOM: variables personalizadas ---------------------- */
  %if %upcase(&psi_mode.) = CUSTOM %then %do;
    %if %length(%superq(psi_custom_vars_num)) > 0 or
        %length(%superq(psi_custom_vars_cat)) > 0 %then %do;
      %let _vars_num     = &psi_custom_vars_num.;
      %let _vars_cat     = &psi_custom_vars_cat.;
      %let _byvar        = &psi_custom_byvar.;
      %let _psi_is_custom = 1;
      %put NOTE: [psi_run] Modo CUSTOM — vars_num=&_vars_num. vars_cat=&_vars_cat. byvar=&_byvar.;
    %end;
    %else %do;
      %put WARNING: [psi_run] psi_mode=CUSTOM pero variables vacías. Fallback a AUTO.;
    %end;
  %end;

  /* ------ Modo AUTO (o fallback): variables de configuración --------- */
  %if &_psi_is_custom. = 0 %then %do;
    %put NOTE: [psi_run] Modo AUTO — resolviendo vars desde config.;

    /* Intentar override de segmento si scope es segNNN */
    %if %substr(&scope., 1, 3) = seg %then %do;
      %let _seg_num = %sysfunc(inputn(%substr(&scope., 4), best.));

      proc sql noprint;
        select strip(num_list) into :_vars_num trimmed
        from casuser.cfg_segmentos
        where troncal_id = &troncal_id. and seg_id = &_seg_num.;

        select strip(cat_list) into :_vars_cat trimmed
        from casuser.cfg_segmentos
        where troncal_id = &troncal_id. and seg_id = &_seg_num.;
      quit;
    %end;

    /* Fallback a troncal si segmento no tiene override */
    %if %length(%superq(_vars_num)) = 0 %then %do;
      proc sql noprint;
        select strip(num_unv) into :_vars_num trimmed
        from casuser.cfg_troncales
        where troncal_id = &troncal_id.;
      quit;
    %end;
    %if %length(%superq(_vars_cat)) = 0 %then %do;
      proc sql noprint;
        select strip(cat_unv) into :_vars_cat trimmed
        from casuser.cfg_troncales
        where troncal_id = &troncal_id.;
      quit;
    %end;

    /* Variable temporal (mes_var) */
    proc sql noprint;
      select strip(mes_var) into :_byvar trimmed
      from casuser.cfg_troncales
      where troncal_id = &troncal_id.;
    quit;
  %end;

  %put NOTE: [psi_run] Variables numéricas: &_vars_num.;
  %put NOTE: [psi_run] Variables categóricas: &_vars_cat.;
  %put NOTE: [psi_run] Variable temporal: &_byvar.;

  /* ==================================================================
     Determinar rutas de salida según modo

     Naming de tablas .sas7bdat — máximo 32 caracteres (límite SAS):
       Formato compacto: psi_t<N>_<scope>_<tipo>
       Ej: psi_t1_base_cubo (15 chars), psi_t1_seg001_rsmn (19 chars)
       Reportes usan nombres descriptivos completos (no hay límite).
     ================================================================== */

  %if %substr(&scope., 1, 3) = seg %then %let _scope_abbr = &scope.;
  %else %let _scope_abbr = base;

  %if &_psi_is_custom. = 1 %then %do;
    %let _report_path = &fw_root./outputs/runs/&run_id./experiments;
    %let _tables_path = &fw_root./outputs/runs/&run_id./experiments;
    %let _image_path  = &fw_root./outputs/runs/&run_id./experiments;
    %let _file_prefix = custom_psi_troncal_&troncal_id._&_scope_abbr.;
    %let _tbl_prefix  = cx_psi_t&troncal_id._&_scope_abbr.;
    %put NOTE: [psi_run] Output → experiments/ (exploratorio);
  %end;
  %else %do;
    %let _report_path = &fw_root./outputs/runs/&run_id./reports;
    %let _tables_path = &fw_root./outputs/runs/&run_id./tables;
    %let _image_path  = &fw_root./outputs/runs/&run_id./images;
    %let _file_prefix = psi_troncal_&troncal_id._&_scope_abbr.;
    %let _tbl_prefix  = psi_t&troncal_id._&_scope_abbr.;
    %put NOTE: [psi_run] Output → reports/ + tables/ + images/ (estándar);
  %end;

  /* ==================================================================
     2) Contract — validaciones pre-ejecución
     ================================================================== */
  %psi_contract(
    input_caslib = &input_caslib.,
    train_table  = &train_table.,
    oot_table    = &oot_table.,
    byvar        = &_byvar.,
    var_num      = &_vars_num.,
    var_cat      = &_vars_cat.
  );

  %if &_psi_rc. ne 0 %then %do;
    %put ERROR: [psi_run] Contract fallido — módulo abortado.;
    %return;
  %end;

  /* ==================================================================
     3) Compute — PSI cubo + wide + resumen → work tables
     ================================================================== */
  %_psi_compute(
    train_data   = &input_caslib..&train_table.,
    oot_data     = &input_caslib..&oot_table.,
    byvar        = &_byvar.,
    var_num_list = &_vars_num.,
    var_cat_list = &_vars_cat.,
    n_buckets    = &psi_n_buckets.,
    mensual      = &psi_mensual.
  );

  /* ==================================================================
     4) Report — Excel + HTML + gráficos PNG
     ================================================================== */
  %_psi_report(
    report_path = &_report_path.,
    image_path  = &_image_path.,
    file_prefix = &_file_prefix.,
    byvar       = &_byvar.
  );

  /* ==================================================================
     5) Persistir tablas como .sas7bdat
        Usa _tables_path (separado de _report_path) y _tbl_prefix (≤32 ch)
     ================================================================== */
  libname _outlib "&_tables_path.";

  data _outlib.&_tbl_prefix._cubo;
    set work._psi_cubo;
  run;

  data _outlib.&_tbl_prefix._wide;
    set work._psi_cubo_wide;
  run;

  data _outlib.&_tbl_prefix._rsmn;
    set work._psi_resumen;
  run;

  libname _outlib clear;

  %put NOTE: [psi_run] Tablas .sas7bdat guardadas en &_tables_path.;
  %put NOTE: [psi_run]   &_tbl_prefix._cubo  (detalle);
  %put NOTE: [psi_run]   &_tbl_prefix._wide  (pivot);
  %put NOTE: [psi_run]   &_tbl_prefix._rsmn  (resumen);

  /* ==================================================================
     6) Cleanup — eliminar tablas temporales de work
     ================================================================== */
  proc datasets library=work nolist;
    delete _psi_cubo _psi_cubo_wide _psi_resumen;
  run;

  %put NOTE: ======================================================;
  %put NOTE: [psi_run] FIN — &_file_prefix. (mode=&psi_mode.);
  %put NOTE: ======================================================;

%mend psi_run;
