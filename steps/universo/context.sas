/* =========================================================================
   steps/universo/context.sas — Contexto de ejecución: UNIVERSO (troncal)
   =========================================================================
   Define qué troncal(es) analizar en este swimlane.
   No incluye segmentos — solo base/universo por troncal.

   Variables UI (_id_*):
     _id_ctx_unv_mode       : ONE | ALL
     _id_ctx_unv_troncal_id : número de troncal (solo si mode=ONE)
     _id_ctx_unv_split      : TRAIN | OOT | BOTH
   ========================================================================= */

/* ---- Scope del swimlane (NO EDITAR) ----------------------------------- */
%let ctx_scope = UNIVERSO;

/* ---- Configuración del contexto (editar aquí) ------------------------- */

/* ctx_universe_mode:
     ALL → itera todos los troncales
     ONE → solo el troncal indicado abajo                                  */
%let ctx_universe_mode       = ALL;
%let ctx_universe_troncal_id = 1;

/* ctx_universe_split:
     TRAIN → solo train
     OOT   → solo oot
     BOTH  → train y oot                                                   */
%let ctx_universe_split      = BOTH;

%put NOTE: [universo/context] scope=&ctx_scope. mode=&ctx_universe_mode. troncal=&ctx_universe_troncal_id. split=&ctx_universe_split.;

/* ---- Validación cuando mode=ONE --------------------------------------- */
%macro _ctx_unv_validate;
   %if %upcase(&ctx_universe_mode.) = ONE %then %do;
      proc sql noprint;
         select count(*) into :_ctx_unv_tr_exists trimmed
         from casuser.cfg_troncales
         where troncal_id = &ctx_universe_troncal_id.;
      quit;

      %if &_ctx_unv_tr_exists. = 0 %then %do;
         %put ERROR: [universo/context] troncal_id=&ctx_universe_troncal_id. no existe en cfg_troncales.;
      %end;
   %end;
%mend _ctx_unv_validate;
%_ctx_unv_validate;
