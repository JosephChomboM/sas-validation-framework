/* =========================================================================
correlacion_run.sas - Macro pública del módulo Correlación

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
- CUSTOM → &corr_custom_vars (definidas en step_correlacion.sas)
2) Ejecutar contract (validaciones)
3) Calcular matrices Pearson + Spearman
4) Generar reportes HTML + Excel
- AUTO   → outputs/runs/<run_id>/reports/   (validación estándar)
- CUSTOM → outputs/runs/<run_id>/experiments/ (análisis exploratorio)
5) Persistir tablas de correlación → .sas7bdat en disco
- AUTO   → reports/   (estándar)
- CUSTOM → experiments/  (exploratorio)
6) Cleanup tablas temporales (work)

Variables globales leídas (definidas en step_correlacion.sas):
&corr_mode         - AUTO | CUSTOM
&corr_custom_vars  - lista vars numéricas (solo si CUSTOM)

Dependencias (cargadas por step_correlacion.sas vía common_public.sas):
- Ninguna de cas_utils para outputs (usa libname SAS directo)

Solo recibe variables numéricas.

Compatibilidad: segmento y universo.
========================================================================= */
/* ---- Incluir componentes del módulo ----------------------------------- */
%include "&fw_root./src/modules/correlacion/correlacion_contract.sas";
%include "&fw_root./src/modules/correlacion/impl/correlacion_compute.sas";
%include "&fw_root./src/modules/correlacion/impl/correlacion_report.sas";

%macro correlacion_run( input_caslib=PROC, input_table=_active_input,
  output_caslib=OUT, troncal_id=, split=, scope=, run_id=);

  /* ---- Return code: owned here, used by contract -------------------- */
  %global _corr_rc;
  %let _corr_rc=0;

  %local _corr_vars _report_path _tables_path _file_prefix _tbl_prefix _seg_num
    _corr_is_custom;

  %put NOTE:======================================================;
  %put NOTE: [correlacion_run] INICIO;
  %put NOTE: troncal=&troncal_id. split=&split. scope=&scope.;
  %put NOTE: input=&input_caslib..&input_table. output=&output_caslib.;
  %put NOTE: mode=&corr_mode.;
  %put NOTE:======================================================;

  /* ==================================================================
  1) Determinar modo y resolver variables numéricas
  ================================================================== */
  %let _corr_vars= ;
  %let _corr_is_custom=0;

  /* ------ Modo CUSTOM: variables personalizadas ---------------------- */
  %if %upcase(&corr_mode.)=CUSTOM %then %do;
    %if %length(%superq(corr_custom_vars)) > 0 %then %do;
      %let _corr_vars=&corr_custom_vars.;
      %let _corr_is_custom=1;
      %put NOTE: [correlacion_run] Modo CUSTOM - vars usuario: &_corr_vars.;
    %end;
    %else %do;
      %put WARNING: [correlacion_run] corr_mode=CUSTOM pero corr_custom_vars
        vacía. Fallback a AUTO.;
    %end;
  %end;

  /* ------ Modo AUTO (o fallback): variables de configuración --------- */
  %if &_corr_is_custom.=0 %then %do;
    %put NOTE: [correlacion_run] Modo AUTO - resolviendo vars desde config.;

    %if %substr(&scope., 1, 3)=seg %then %do;
      /* Extraer seg_id numérico del scope (segNNN → NNN) */
      %let _seg_num=%sysfunc(inputn(%substr(&scope., 4), best.));

      proc sql noprint;
        select strip(num_list) into :_corr_vars trimmed from
          casuser.cfg_segmentos where troncal_id=&troncal_id. and seg_id=
          &_seg_num.;
      quit;
    %end;

    /* Fallback a num_unv del troncal si no hay override de segmento */
    %if %length(%superq(_corr_vars))=0 %then %do;
      proc sql noprint;
        select strip(num_unv) into :_corr_vars trimmed from
          casuser.cfg_troncales where troncal_id=&troncal_id.;
      quit;
    %end;
  %end;

  %put NOTE: [correlacion_run] Variables numéricas resueltas: &_corr_vars.;

  /* ==================================================================
  Determinar rutas de salida según modo
  AUTO   → reports/ (html/xlsx) + tables/ (.sas7bdat)
  CUSTOM → experiments/ (todo junto)

  Naming de tablas .sas7bdat - máximo 32 caracteres (límite SAS):
  Formato compacto: <mod>_t<N>_<spl>_<scope>_<tipo>
  Ej: corr_t1_trn_s001_prsn (21 chars)
  Reportes pueden usar nombres largos (filesystem, no SAS dataset).
  ================================================================== */
  /* -- Abreviaturas para table naming --------------------------------- */
  %local _spl_abbr _scope_abbr;
  %if %upcase(&split.)=TRAIN %then %let _spl_abbr=trn;
  %else %let _spl_abbr=oot;

  %if %substr(&scope., 1, 3)=seg %then %let _scope_abbr=&scope.;
  %else %let _scope_abbr=base;

  %if &_corr_is_custom.=1 %then %do;
    %let _report_path=&fw_root./outputs/runs/&run_id./experiments;
    %let _tables_path=&fw_root./outputs/runs/&run_id./experiments;
    %let _file_prefix=custom_correlacion_troncal_&troncal_id._&split._&scope.;
    %let _tbl_prefix=cx_corr_t&troncal_id._&_spl_abbr._&_scope_abbr.;
    %put NOTE: [correlacion_run] Output → experiments/ (exploratorio);
  %end;
  %else %do;
    %let _report_path=&fw_root./outputs/runs/&run_id./reports/METOD4.3;
    %let _tables_path=&fw_root./outputs/runs/&run_id./tables/METOD4.3;
    %let _file_prefix=correlacion_troncal_&troncal_id._&split._&scope.;
    %let _tbl_prefix=corr_t&troncal_id._&_spl_abbr._&_scope_abbr.;
    %put NOTE: [correlacion_run] Output → reports/METOD4.3/ (estándar);
  %end;

  /* ==================================================================
  2) Contract - validaciones pre-ejecución
  ================================================================== */
  %correlacion_contract( input_caslib=&input_caslib., input_table=&input_table.,
    variables=&_corr_vars. );

  %if &_corr_rc. ne 0 %then %do;
    %put ERROR: [correlacion_run] Contract fallido - módulo abortado.;
    %return;
  %end;

  /* ==================================================================
  3) Compute - Pearson + Spearman → work tables
  ================================================================== */
  %_correlacion_compute( input_lib=&input_caslib., input_table=&input_table.,
    variables=&_corr_vars. );

  /* ==================================================================
  4) Report - HTML + Excel
  ================================================================== */
  %_correlacion_report( report_path=&_report_path., file_prefix=&_file_prefix.
    );

  /* ==================================================================
  5) Persistir tablas como .sas7bdat en directorio de tables
  Usa _tables_path (separado de _report_path) y _tbl_prefix (≤32 ch)
  ================================================================== */
  /* ---- Crear directorio tables/METOD4.3 si no existe ------------------- */
  %let _dir_rc=%sysfunc(dcreate(METOD4.3, &_tables_path./../));
  %let _dir_rc=%sysfunc(dcreate(., &_tables_path.));

  libname _outlib "&_tables_path.";

  data _outlib.&_tbl_prefix._prsn;
    set casuser._corr_pearson;
  run;

  data _outlib.&_tbl_prefix._sprm;
    set casuser._corr_spearman;
  run;

  libname _outlib clear;

  %put NOTE: [correlacion_run] Tablas .sas7bdat guardadas en &_tables_path.;
  %put NOTE: [correlacion_run] &_tbl_prefix._prsn (pearson);
  %put NOTE: [correlacion_run] &_tbl_prefix._sprm (spearman);

  /* ==================================================================
  6) Cleanup - eliminar tablas temporales de work
  ================================================================== */
  proc datasets library=casuser nolist;
    delete _corr_pearson _corr_spearman;
    run;

    %put NOTE:======================================================;
    %put NOTE: [correlacion_run] FIN - &_file_prefix. (mode=&corr_mode.);
    %put NOTE:======================================================;

%mend correlacion_run;

/* -------------------------------------------------------------------------
   Refactor override: single-source scope input with internal TRAIN/OOT
   derivation. The later macro definition takes precedence over the legacy
   single-input implementation above.
   ------------------------------------------------------------------------- */
%macro _corr_csv_list(list=, outvar=, exclude_var=);

  %local _corr_csv _corr_i _corr_item;
  %let _corr_csv=;
  %let _corr_i=1;
  %let _corr_item=%scan(%superq(list), &_corr_i., %str( ));

  %do %while(%length(%superq(_corr_item)) > 0);
    %if %upcase(%superq(_corr_item)) ne %upcase(%superq(exclude_var)) %then %do;
      %if %length(%superq(_corr_csv))=0 %then %let _corr_csv=&_corr_item.;
      %else %let _corr_csv=&_corr_csv., &_corr_item.;
    %end;

    %let _corr_i=%eval(&_corr_i. + 1);
    %let _corr_item=%scan(%superq(list), &_corr_i., %str( ));
  %end;

  %let &outvar.=&_corr_csv.;

%mend _corr_csv_list;

%macro _corr_process_one(source_caslib=, source_table=, variables=, select_vars=,
  byvar=, split_name=, split_min=, split_max=, report_path=, tables_path=,
  file_prefix=, tbl_prefix=, create_metod_dir=Y);

  %global _corr_rc;
  %local _dir_rc;

  %let _corr_rc=0;

  %correlacion_contract(input_caslib=&source_caslib., input_table=&source_table.,
    variables=&variables., byvar=&byvar., split=&split_name.,
    train_min_mes=&split_min., train_max_mes=&split_max.,
    oot_min_mes=&split_min., oot_max_mes=&split_max.);

  %if &_corr_rc. ne 0 %then %do;
    %put ERROR: [correlacion_run] Contract fallido para split=&split_name..;
    %return;
  %end;

  proc fedsql sessref=conn;
    create table casuser._corr_&split_name._input {options replace=true} as
    select &byvar., &select_vars.
    from &source_caslib..&source_table.
    where &byvar. between &split_min. and &split_max.;
  quit;

  %_correlacion_compute(input_lib=casuser, input_table=_corr_&split_name._input,
    variables=&variables., pearson_table=_corr_pearson,
    spearman_table=_corr_spearman);

  %_correlacion_report(report_path=&report_path., file_prefix=&file_prefix.,
    pearson_table=_corr_pearson, spearman_table=_corr_spearman,
    create_metod_dir=&create_metod_dir.);

  %if %upcase(&create_metod_dir.)=Y %then %do;
    %let _dir_rc=%sysfunc(dcreate(METOD4.3, &tables_path./../));
  %end;

  libname _outlib "&tables_path.";

  data _outlib.&tbl_prefix._prsn;
    set casuser._corr_pearson;
  run;

  data _outlib.&tbl_prefix._sprm;
    set casuser._corr_spearman;
  run;

  libname _outlib clear;

  %put NOTE: [correlacion_run] Tablas .sas7bdat guardadas en &tables_path..;
  %put NOTE: [correlacion_run] &tbl_prefix._prsn (pearson);
  %put NOTE: [correlacion_run] &tbl_prefix._sprm (spearman);

  proc datasets library=casuser nolist nowarn;
    delete _corr_&split_name._input _corr_pearson _corr_spearman;
  quit;

%mend _corr_process_one;

%macro correlacion_run(input_caslib=PROC, input_table=_active_input,
  output_caslib=OUT, troncal_id=, split=, scope=, run_id=);

  %global _corr_rc;
  %let _corr_rc=0;

  %local _corr_vars _report_path _tables_path _seg_num _corr_is_custom
    _scope_abbr _corr_byvar _corr_train_min _corr_train_max _corr_oot_min
    _corr_oot_max _corr_requested_split _corr_select_vars _corr_run_train
    _corr_run_oot _corr_success_count _corr_create_metod_dir _file_prefix
    _tbl_prefix;

  %put NOTE:======================================================;
  %put NOTE: [correlacion_run] INICIO;
  %put NOTE: troncal=&troncal_id. split=&split. scope=&scope.;
  %put NOTE: input=&input_caslib..&input_table. output=&output_caslib.;
  %put NOTE: mode=&corr_mode.;
  %put NOTE:======================================================;

  %let _corr_vars=;
  %let _corr_is_custom=0;

  %if %upcase(&corr_mode.)=CUSTOM %then %do;
    %if %length(%superq(corr_custom_vars)) > 0 %then %do;
      %let _corr_vars=&corr_custom_vars.;
      %let _corr_is_custom=1;
      %put NOTE: [correlacion_run] Modo CUSTOM - vars usuario: &_corr_vars.;
    %end;
    %else %do;
      %put WARNING: [correlacion_run] corr_mode=CUSTOM pero corr_custom_vars vacia. Fallback a AUTO.;
    %end;
  %end;

  %if &_corr_is_custom.=0 %then %do;
    %put NOTE: [correlacion_run] Modo AUTO - resolviendo vars desde config.;

    %if %substr(&scope., 1, 3)=seg %then %do;
      %let _seg_num=%sysfunc(inputn(%substr(&scope., 4), best.));

      proc sql noprint;
        select strip(num_list)
          into :_corr_vars trimmed
        from casuser.cfg_segmentos
        where troncal_id=&troncal_id.
          and seg_id=&_seg_num.;
      quit;
    %end;

    %if %length(%superq(_corr_vars))=0 %then %do;
      proc sql noprint;
        select strip(num_unv)
          into :_corr_vars trimmed
        from casuser.cfg_troncales
        where troncal_id=&troncal_id.;
      quit;
    %end;
  %end;

  %put NOTE: [correlacion_run] Variables numericas resueltas: &_corr_vars.;

  proc sql noprint;
    select strip(byvar),
           strip(put(train_min_mes, best.)),
           strip(put(train_max_mes, best.)),
           strip(put(oot_min_mes, best.)),
           strip(put(oot_max_mes, best.))
      into :_corr_byvar trimmed,
           :_corr_train_min trimmed,
           :_corr_train_max trimmed,
           :_corr_oot_min trimmed,
           :_corr_oot_max trimmed
    from casuser.cfg_troncales
    where troncal_id=&troncal_id.;
  quit;

  %if %length(%superq(_corr_byvar))=0 or
      %length(%superq(_corr_train_min))=0 or
      %length(%superq(_corr_train_max))=0 or
      %length(%superq(_corr_oot_min))=0 or
      %length(%superq(_corr_oot_max))=0 %then %do;
    %put ERROR: [correlacion_run] No se pudo resolver byvar/ventanas para troncal=&troncal_id..;
    %return;
  %end;

  %_corr_csv_list(list=&_corr_vars., outvar=_corr_select_vars,
    exclude_var=&_corr_byvar.);

  %if %substr(&scope., 1, 3)=seg %then %let _scope_abbr=&scope.;
  %else %let _scope_abbr=base;

  %if &_corr_is_custom.=1 %then %do;
    %let _report_path=&fw_root./outputs/runs/&run_id./experiments;
    %let _tables_path=&fw_root./outputs/runs/&run_id./experiments;
    %let _corr_create_metod_dir=N;
    %put NOTE: [correlacion_run] Output => experiments/ (exploratorio);
  %end;
  %else %do;
    %let _report_path=&fw_root./outputs/runs/&run_id./reports/METOD4.3;
    %let _tables_path=&fw_root./outputs/runs/&run_id./tables/METOD4.3;
    %let _corr_create_metod_dir=Y;
    %put NOTE: [correlacion_run] Output => reports/tables METOD4.3/ (estandar);
  %end;

  %let _corr_requested_split=&split.;
  %if %length(%superq(_corr_requested_split))=0 and %symexist(ctx_split) %then
    %let _corr_requested_split=&ctx_split.;

  %if %upcase(&_corr_requested_split.)=TRAIN %then %do;
    %let _corr_run_train=1;
    %let _corr_run_oot=0;
  %end;
  %else %if %upcase(&_corr_requested_split.)=OOT %then %do;
    %let _corr_run_train=0;
    %let _corr_run_oot=1;
  %end;
  %else %do;
    %let _corr_run_train=1;
    %let _corr_run_oot=1;
  %end;

  %put NOTE: [correlacion_run] Single source=&input_caslib..&input_table.
    byvar=&_corr_byvar. TRAIN=&_corr_train_min.-&_corr_train_max.
    OOT=&_corr_oot_min.-&_corr_oot_max..;
  %put NOTE: [correlacion_run] Split solicitado=&_corr_requested_split.
    => run_train=&_corr_run_train. run_oot=&_corr_run_oot..;

  %let _corr_success_count=0;

  %if &_corr_run_train.=1 %then %do;
    %if &_corr_is_custom.=1 %then %do;
      %let _file_prefix=custom_correlacion_troncal_&troncal_id._TRAIN_&scope.;
      %let _tbl_prefix=cx_corr_t&troncal_id._trn._&_scope_abbr.;
    %end;
    %else %do;
      %let _file_prefix=correlacion_troncal_&troncal_id._TRAIN_&scope.;
      %let _tbl_prefix=corr_t&troncal_id._trn._&_scope_abbr.;
    %end;

    %_corr_process_one(source_caslib=&input_caslib., source_table=&input_table.,
      variables=&_corr_vars., select_vars=&_corr_select_vars., byvar=&_corr_byvar.,
      split_name=TRAIN, split_min=&_corr_train_min., split_max=&_corr_train_max.,
      report_path=&_report_path., tables_path=&_tables_path.,
      file_prefix=&_file_prefix., tbl_prefix=&_tbl_prefix.,
      create_metod_dir=&_corr_create_metod_dir.);

    %if &_corr_rc.=0 %then
      %let _corr_success_count=%eval(&_corr_success_count. + 1);
  %end;

  %if &_corr_run_oot.=1 %then %do;
    %if &_corr_is_custom.=1 %then %do;
      %let _file_prefix=custom_correlacion_troncal_&troncal_id._OOT_&scope.;
      %let _tbl_prefix=cx_corr_t&troncal_id._oot._&_scope_abbr.;
    %end;
    %else %do;
      %let _file_prefix=correlacion_troncal_&troncal_id._OOT_&scope.;
      %let _tbl_prefix=corr_t&troncal_id._oot._&_scope_abbr.;
    %end;

    %_corr_process_one(source_caslib=&input_caslib., source_table=&input_table.,
      variables=&_corr_vars., select_vars=&_corr_select_vars., byvar=&_corr_byvar.,
      split_name=OOT, split_min=&_corr_oot_min., split_max=&_corr_oot_max.,
      report_path=&_report_path., tables_path=&_tables_path.,
      file_prefix=&_file_prefix., tbl_prefix=&_tbl_prefix.,
      create_metod_dir=&_corr_create_metod_dir.);

    %if &_corr_rc.=0 %then
      %let _corr_success_count=%eval(&_corr_success_count. + 1);
  %end;

  proc datasets library=casuser nolist nowarn;
    delete _corr_:;
  quit;

  %if &_corr_success_count.=0 %then %do;
    %put ERROR: [correlacion_run] Ningun split pudo ejecutarse para scope=&scope..;
    %return;
  %end;

  %put NOTE:======================================================;
  %put NOTE: [correlacion_run] FIN - scope=&scope. splits_ok=&_corr_success_count.
    (mode=&corr_mode.);
  %put NOTE:======================================================;

%mend correlacion_run;
