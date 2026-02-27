/* =========================================================================
   steps/09_promote_universe_context.sas — Step 9: Contexto universo/base
   ========================================================================= */

/* ctx_universe_mode: ONE | ALL */
%let ctx_universe_mode       = ALL;
%let ctx_universe_troncal_id = 1;
/* ctx_universe_split: TRAIN | OOT | BOTH */
%let ctx_universe_split      = BOTH;

%put NOTE: [step-09] mode=&ctx_universe_mode. troncal=&ctx_universe_troncal_id. split=&ctx_universe_split.;
