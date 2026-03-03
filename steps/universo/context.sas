/* =========================================================================
   steps/universo/context.sas — Contexto de ejecución: UNIVERSO (troncal)
   =========================================================================
   Define qué troncal analizar en este swimlane.
   Se corre solo la base/universo del troncal indicado (sin segmentos).

   Variables UI:
     ctx_universe_troncal_id : número de troncal a analizar
     ctx_universe_split      : TRAIN | OOT | BOTH
   ========================================================================= */

/* ---- Scope del swimlane (NO EDITAR) ----------------------------------- */
%let ctx_scope = UNIVERSO;

/* ---- Configuración del contexto (editar aquí) ------------------------- */

/* Troncal a analizar — se corre solo su base (universo)                   */
%let ctx_universe_troncal_id = 1;

/* ctx_universe_split:
     TRAIN → solo train
     OOT   → solo oot
     BOTH  → train y oot                                                   */
%let ctx_universe_split      = BOTH;

%put NOTE: [universo/context] scope=&ctx_scope. troncal=&ctx_universe_troncal_id. split=&ctx_universe_split.;

/* ---- Validación: troncal existe en config ----------------------------- */
%macro _ctx_unv_validate;
   proc sql noprint;
      select count(*) into :_ctx_unv_tr_exists trimmed
      from casuser.cfg_troncales
      where troncal_id = &ctx_universe_troncal_id.;
   quit;

   %if &_ctx_unv_tr_exists. = 0 %then %do;
      %put ERROR: [universo/context] troncal_id=&ctx_universe_troncal_id. no existe en cfg_troncales.;
   %end;
%mend _ctx_unv_validate;
%_ctx_unv_validate;
