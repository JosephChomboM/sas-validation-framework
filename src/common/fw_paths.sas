/* =========================================================================
  fw_paths.sas - Resolver único de rutas processed y derivados por split
   Evita hardcode de paths en módulos y runner.

   Macros públicas:
     %fw_path_processed(outvar=, troncal_id=, split=, seg_id=)
     %_fw_load_scope_input(...)
     %_fw_build_split_table(...)

   Convención (design.md §3.2):
     - Universo  : troncal_<id>/base
     - Segmento  : troncal_<id>/seg<NNN>  (z3. padding)

   Las rutas devueltas son RELATIVAS al CASLIB PROC (PATH-based,
   subdirs=1, mapeado a data/processed/). NO incluyen extensión
   (.sashdat lo agrega el consumidor: _promote_castable, _load_cas_data, etc.).

   Nota de diseño:
   - `processed` materializa una sola base persistente por troncal/segmento.
   - TRAIN/OOT se derivan dinámicamente en ejecución usando las ventanas
     temporales de `casuser.cfg_troncales`.
   ========================================================================= */

%macro fw_path_processed(outvar=, troncal_id=, split=, seg_id=);

  /* --- Validación mínima ------------------------------------------------ */
  %if %superq(outvar) = %then %do;
    %put ERROR: [fw_path_processed] outvar= es obligatorio.;
    %abort cancel;
  %end;
  %if %superq(troncal_id) = %then %do;
    %put ERROR: [fw_path_processed] troncal_id= es obligatorio.;
    %abort cancel;
  %end;

  /* `split` se conserva solo por compatibilidad de llamadas existentes. */
  %if %length(%superq(split)) > 0 %then %do;
    %if %upcase(&split.) ne TRAIN and %upcase(&split.) ne OOT %then %do;
      %put WARNING: [fw_path_processed] split=&split. no es TRAIN/OOT. Se ignorara para resolver la ruta física.;
    %end;
  %end;

  /* --- Construir ruta --------------------------------------------------- */
  %global &outvar.;

  %if %superq(seg_id) = %then %do;
    /* Universo (base completa) - sin extensión; el consumidor agrega .sashdat */
    %let &outvar. = troncal_&troncal_id./base;
  %end;
  %else %do;
    /* Segmento con padding z3. - sin extensión */
    %let _seg_pad = %sysfunc(putn(&seg_id., z3.));
    %let &outvar. = troncal_&troncal_id./seg&_seg_pad.;
    %symdel _seg_pad / nowarn;
  %end;

  %put NOTE: [fw_path_processed] &outvar. = &&&outvar.;

%mend fw_path_processed;

/* -------------------------------------------------------------------------
   %_fw_load_scope_input - Carga la base persistente de un troncal/segmento
   desde data/processed hacia CAS.
   ------------------------------------------------------------------------- */
%macro _fw_load_scope_input(
  troncal_id=,
  seg_id=,
  input_caslib=PROC,
  output_caslib=PROC,
  output_table=_scope_input,
  sess=conn
);

  %local _fw_scope_path;

  %if %length(%superq(troncal_id))=0 %then %do;
    %put ERROR: [_fw_load_scope_input] troncal_id= es obligatorio.;
    %return;
  %end;

  %fw_path_processed(outvar=_fw_scope_path, troncal_id=&troncal_id.,
    seg_id=&seg_id.);

  %_promote_castable(m_cas_sess_name=&sess., m_input_caslib=&input_caslib.,
    m_subdir_data=&_fw_scope_path., m_output_caslib=&output_caslib.,
    m_output_data=&output_table.);

%mend _fw_load_scope_input;

/* -------------------------------------------------------------------------
   %_fw_build_split_table - Deriva TRAIN/OOT desde la base persistente
   usando las ventanas configuradas en casuser.cfg_troncales.
   ------------------------------------------------------------------------- */
%macro _fw_build_split_table(
  troncal_id=,
  split=,
  source_caslib=PROC,
  source_table=,
  target_caslib=PROC,
  target_table=,
  sess=conn
);

  %local _fw_split_uc _fw_min_col _fw_max_col _fw_byvar _fw_min _fw_max;

  %if %length(%superq(troncal_id))=0 %then %do;
    %put ERROR: [_fw_build_split_table] troncal_id= es obligatorio.;
    %return;
  %end;
  %if %length(%superq(source_table))=0 %then %do;
    %put ERROR: [_fw_build_split_table] source_table= es obligatorio.;
    %return;
  %end;
  %if %length(%superq(target_table))=0 %then %do;
    %put ERROR: [_fw_build_split_table] target_table= es obligatorio.;
    %return;
  %end;

  %let _fw_split_uc=%upcase(%superq(split));
  %if &_fw_split_uc. ne TRAIN and &_fw_split_uc. ne OOT %then %do;
    %put ERROR: [_fw_build_split_table] split= debe ser TRAIN u OOT (recibido: &split.).;
    %return;
  %end;

  %if &_fw_split_uc.=TRAIN %then %do;
    %let _fw_min_col=train_min_mes;
    %let _fw_max_col=train_max_mes;
  %end;
  %else %do;
    %let _fw_min_col=oot_min_mes;
    %let _fw_max_col=oot_max_mes;
  %end;

  proc sql noprint;
    select strip(byvar),
           strip(put(&_fw_min_col., best.)),
           strip(put(&_fw_max_col., best.))
      into :_fw_byvar trimmed,
           :_fw_min trimmed,
           :_fw_max trimmed
    from casuser.cfg_troncales
    where troncal_id=&troncal_id.;
  quit;

  %if %length(%superq(_fw_byvar))=0 or %length(%superq(_fw_min))=0 or
      %length(%superq(_fw_max))=0 %then %do;
    %put ERROR: [_fw_build_split_table] No se pudo resolver byvar o ventana temporal para troncal=&troncal_id. split=&_fw_split_uc..;
    %return;
  %end;

  %put NOTE: [_fw_build_split_table] troncal=&troncal_id. split=&_fw_split_uc. byvar=&_fw_byvar. ventana=&_fw_min.-&_fw_max..;

  proc fedsql sessref=&sess.;
    create table &target_caslib..&target_table. {options replace=true} as
    select *
    from &source_caslib..&source_table.
    where &_fw_byvar. >= &_fw_min.
      and &_fw_byvar. <= &_fw_max.;
  quit;

%mend _fw_build_split_table;
