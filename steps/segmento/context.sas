/* =========================================================================
steps/segmento/context.sas — Contexto de ejecución: SEGMENTO
=========================================================================
Define qué troncal y segmento(s) analizar en este swimlane.

Variables UI:
ctx_segment_troncal_id : número de troncal a analizar
ctx_segment_split      : TRAIN | OOT | BOTH
ctx_segment_seg_id     : ALL (todos los segmentos) | número específico
========================================================================= */
/* ---- Scope del swimlane (NO EDITAR) ----------------------------------- */
%let ctx_scope=SEGMENTO;

/* ---- Configuración del contexto (editar aquí) ------------------------- */
/* Troncal a analizar                                                      */
%let ctx_segment_troncal_id=&_id_segment_troncal_id.;

/* ctx_segment_split:
TRAIN → solo train
OOT   → solo oot
BOTH  → train y oot                                                   */
%let ctx_segment_split=BOTH;

/* ctx_segment_seg_id (radio button):
ALL → correr TODOS los segmentos del troncal
<N> → correr solo el segmento N (ej. 1, 2, 3...)                      */
%let ctx_segment_seg_id=&_id_segment_seg_id.;

%if &ctx_segment_seg_id. eq CUSTOM %then %do;
    %let ctx_segment_seg_id=&_id_segmento_seg_num.;
%end;

%put NOTE: [segmento/context] scope=&ctx_scope. troncal=&ctx_segment_troncal_id.split=&ctx_segment_split. seg=&ctx_segment_seg_id.;

/* ---- Validación: troncal existe en config ----------------------------- */
%macro _ctx_seg_validate;
    %global ctx_segment_n_segments;

    proc sql noprint;
        select count(*) into :_ctx_tr_exists trimmed from casuser.cfg_troncales
            where troncal_id=&ctx_segment_troncal_id.;
    quit;

    %if &_ctx_tr_exists.=0 %then %do;
        %put ERROR: [segmento/context] troncal_id=&ctx_segment_troncal_id. no
            existe en cfg_troncales.;
    %end;

    /* Guardar n_segments para que los módulos lo usen */
    proc sql noprint;
        select n_segments into :ctx_segment_n_segments trimmed from
            casuser.cfg_troncales where troncal_id=&ctx_segment_troncal_id.;
    quit;
    %put NOTE: [segmento/context] troncal &ctx_segment_troncal_id. tiene
        &ctx_segment_n_segments. segmentos.;

    /* Validar seg_id específico si no es ALL */
    %if %upcase(&ctx_segment_seg_id.) ne ALL %then %do;
        %if &ctx_segment_seg_id. < 1 or &ctx_segment_seg_id. >
            &ctx_segment_n_segments. %then %do;
            %put ERROR: [segmento/context] seg_id=&ctx_segment_seg_id. fuera de
                rango (1..&ctx_segment_n_segments.) para
                troncal=&ctx_segment_troncal_id..;
        %end;
    %end;
%mend _ctx_seg_validate;
%_ctx_seg_validate;
