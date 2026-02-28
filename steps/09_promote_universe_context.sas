/* =========================================================================
   steps/09_promote_universe_context.sas — Step 9: Contexto universo/base
   ========================================================================= */

/* ctx_universe_mode: ONE | ALL */
%let ctx_universe_mode       = ALL;
%let ctx_universe_troncal_id = 1;
/* ctx_universe_split: TRAIN | OOT | BOTH */
%let ctx_universe_split      = BOTH;

%put NOTE: [step-09] mode=&ctx_universe_mode. troncal=&ctx_universe_troncal_id. split=&ctx_universe_split.;

/* Validación de contexto cuando mode=ONE */
%if %upcase(&ctx_universe_mode.) = ONE %then %do;
   proc sql noprint;
      select count(*) into :_ctx_unv_tr_exists trimmed
      from casuser.cfg_troncales
      where troncal_id = &ctx_universe_troncal_id.;
   quit;

   %if &_ctx_unv_tr_exists. = 0 %then %do;
      %put ERROR: [step-09] troncal_id=&ctx_universe_troncal_id. no existe en cfg_troncales.;
   %end;
%end;
