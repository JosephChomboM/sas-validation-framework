/* =========================================================================
   steps/universo/context.sas — Contexto de ejecución: UNIVERSO (troncal)
   =========================================================================
   Define qué troncal analizar en este swimlane.
   Se corre solo la base/universo del troncal indicado (sin segmentos).

   Variables UI (seteadas desde .flw o editadas manualmente):
     _id_universe_troncal_id : número de troncal a analizar
     _id_universe_split      : TRAIN | OOT | BOTH
   ========================================================================= */

/* ---- Scope del swimlane (NO EDITAR) ----------------------------------- */
%let ctx_scope = UNIVERSO;

/* ---- Configuración del contexto (editar aquí) ------------------------- */

/* Troncal a analizar — se corre solo su base (universo)                   */
%let ctx_universe_troncal_id = &_id_universe_troncal_id.;

/* ctx_universe_split:
     TRAIN → solo train
     OOT   → solo oot
     BOTH  → train y oot                                                   */
%let ctx_universe_split = &_id_universe_split.;

%put NOTE: [universo/context] scope=&ctx_scope. troncal=&ctx_universe_troncal_id. split=&ctx_universe_split.;

/* ---- Validación: troncal existe + split válido ------------------------ */
%macro _ctx_unv_validate;
   %local _ctx_unv_tr_exists;

   /* 1) Troncal debe existir en config */
   proc sql noprint;
      select count(*) into :_ctx_unv_tr_exists trimmed
      from casuser.cfg_troncales
      where troncal_id = &ctx_universe_troncal_id.;
   quit;

   %if &_ctx_unv_tr_exists. = 0 %then %do;
      %put ERROR: [universo/context] troncal_id=&ctx_universe_troncal_id. no existe en cfg_troncales.;
      %return;
   %end;

   /* 2) Split debe ser TRAIN, OOT o BOTH */
   %if %upcase(&ctx_universe_split.) ne TRAIN and
       %upcase(&ctx_universe_split.) ne OOT and
       %upcase(&ctx_universe_split.) ne BOTH %then %do;
      %put ERROR: [universo/context] split=&ctx_universe_split. no válido. Debe ser TRAIN, OOT o BOTH.;
      %return;
   %end;

   %put NOTE: [universo/context] Validación OK — troncal=&ctx_universe_troncal_id. split=&ctx_universe_split.;
%mend _ctx_unv_validate;
%_ctx_unv_validate;
