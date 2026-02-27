/* =========================================================================
   steps/06_promote_segment_context.sas — Step 6: Contexto segmento
   ========================================================================= */

/* ctx_segment_mode: ONE | ALL */
%let ctx_segment_mode       = ALL;
%let ctx_segment_troncal_id = 1;
/* ctx_segment_split: TRAIN | OOT | BOTH */
%let ctx_segment_split      = BOTH;
/* seg_id numérico o ALL (solo aplica en mode=ONE) */
%let ctx_segment_seg_id     = ALL;

%put NOTE: [step-06] mode=&ctx_segment_mode. troncal=&ctx_segment_troncal_id. split=&ctx_segment_split. seg=&ctx_segment_seg_id.;
