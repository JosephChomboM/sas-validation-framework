/* =========================================================================
   steps/segmento/context.sas — Contexto de ejecución: SEGMENTO
   =========================================================================
   Define qué troncal(es) y segmento(s) analizar en este swimlane.

   Variables UI (_id_*):
     _id_ctx_seg_mode       : ONE | ALL
     _id_ctx_seg_troncal_id : número de troncal (solo si mode=ONE)
     _id_ctx_seg_split      : TRAIN | OOT | BOTH
     _id_ctx_seg_seg_id     : seg_id numérico o ALL (solo si mode=ONE)
   ========================================================================= */

/* ---- Scope del swimlane (NO EDITAR) ----------------------------------- */
%let ctx_scope = SEGMENTO;

/* ---- Configuración del contexto (editar aquí) ------------------------- */

/* ctx_segment_mode:
     ALL → itera todos los troncales y sus segmentos
     ONE → solo el troncal y segmento indicado abajo                       */
%let ctx_segment_mode       = ALL;
%let ctx_segment_troncal_id = 1;

/* ctx_segment_split:
     TRAIN → solo train
     OOT   → solo oot
     BOTH  → train y oot                                                   */
%let ctx_segment_split      = BOTH;

/* seg_id: número de segmento específico, o ALL para todos los segmentos
   (solo aplica cuando mode=ONE)                                           */
%let ctx_segment_seg_id     = ALL;

%put NOTE: [segmento/context] scope=&ctx_scope. mode=&ctx_segment_mode. troncal=&ctx_segment_troncal_id. split=&ctx_segment_split. seg=&ctx_segment_seg_id.;

/* ---- Validación cuando mode=ONE --------------------------------------- */
%macro _ctx_seg_validate;
   %if %upcase(&ctx_segment_mode.) = ONE %then %do;
      proc sql noprint;
         select count(*) into :_ctx_tr_exists trimmed
         from casuser.cfg_troncales
         where troncal_id = &ctx_segment_troncal_id.;
      quit;

      %if &_ctx_tr_exists. = 0 %then %do;
         %put ERROR: [segmento/context] troncal_id=&ctx_segment_troncal_id. no existe en cfg_troncales.;
      %end;

      %if %upcase(&ctx_segment_seg_id.) ne ALL %then %do;
         proc sql noprint;
            select count(*) into :_ctx_seg_exists trimmed
            from casuser.cfg_segmentos
            where troncal_id = &ctx_segment_troncal_id.
               and seg_id     = &ctx_segment_seg_id.;
         quit;

         %if &_ctx_seg_exists. = 0 %then %do;
            %put ERROR: [segmento/context] seg_id=&ctx_segment_seg_id. no existe para troncal=&ctx_segment_troncal_id.;
         %end;
      %end;
   %end;
%mend _ctx_seg_validate;
%_ctx_seg_validate;
