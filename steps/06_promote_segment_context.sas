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

/* Validación de contexto cuando mode=ONE */
%if %upcase(&ctx_segment_mode.) = ONE %then %do;
   proc sql noprint;
      select count(*) into :_ctx_tr_exists trimmed
      from casuser.cfg_troncales
      where troncal_id = &ctx_segment_troncal_id.;
   quit;

   %if &_ctx_tr_exists. = 0 %then %do;
      %put ERROR: [step-06] troncal_id=&ctx_segment_troncal_id. no existe en cfg_troncales.;
   %end;

   %if %upcase(&ctx_segment_seg_id.) ne ALL %then %do;
      proc sql noprint;
         select count(*) into :_ctx_seg_exists trimmed
         from casuser.cfg_segmentos
         where troncal_id = &ctx_segment_troncal_id.
            and seg_id     = &ctx_segment_seg_id.;
      quit;

      %if &_ctx_seg_exists. = 0 %then %do;
         %put ERROR: [step-06] seg_id=&ctx_segment_seg_id. no existe para troncal=&ctx_segment_troncal_id.;
      %end;
   %end;
%end;
