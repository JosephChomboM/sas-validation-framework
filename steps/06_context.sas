/* ==== CONTEXTO DE EJECUCIÓN ============================================ */

/* ctx_scope:
     UNIVERSO → corre solo la base/universo del troncal (sin segmentos)
     SEGMENTO → corre segmento(s) del troncal                             */
%let ctx_scope = &_id_scope_flg.;

/* Troncal a analizar                                                      */
%let ctx_troncal_id = &_id_segment_troncal_id.;

/* ctx_split:
     TRAIN → solo train
     OOT   → solo oot
     BOTH  → train y oot                                                   */
%let ctx_split = BOTH;

/* ctx_seg_id (solo aplica si ctx_scope=SEGMENTO):
     ALL → correr TODOS los segmentos del troncal
     <N> → correr solo el segmento N (ej. 1, 2, 3...)
     Se ignora si ctx_scope=UNIVERSO.                                      */
%let ctx_seg_id = &_id_segment_seg_id.;

%if &ctx_seg_id. eq CUSTOM %then %do;
    %let ctx_seg_id = &_id_segment_seg_num.;
%end;
%put NOTE: [context_and_modules] scope=&ctx_scope. troncal=&ctx_troncal_id. split=&ctx_split. seg=&ctx_seg_id.;

/* ==== VALIDACIÓN ======================================================= */
%macro _ctx_validate;
    %global ctx_n_segments;
    %local _ctx_tr_exists;

    /* ---- 1) scope válido ---------------------------------------------- */
    %if %upcase(&ctx_scope.) ne UNIVERSO and
        %upcase(&ctx_scope.) ne SEGMENTO %then %do;
        %put ERROR: [context_and_modules] ctx_scope=&ctx_scope. no válido. Debe ser UNIVERSO o SEGMENTO.;
        %return;
    %end;

    /* ---- 2) split válido ---------------------------------------------- */
    %if %upcase(&ctx_split.) ne TRAIN and
        %upcase(&ctx_split.) ne OOT and
        %upcase(&ctx_split.) ne BOTH %then %do;
        %put ERROR: [context_and_modules] split=&ctx_split. no válido. Debe ser TRAIN, OOT o BOTH.;
        %return;
    %end;

    /* ---- 3) troncal existe en config ---------------------------------- */
    proc sql noprint;
        select count(*) into :_ctx_tr_exists trimmed
        from casuser.cfg_troncales
        where troncal_id = &ctx_troncal_id.;
    quit;

    %if &_ctx_tr_exists. = 0 %then %do;
        %put ERROR: [context_and_modules] troncal_id=&ctx_troncal_id. no existe en cfg_troncales.;
        %return;
    %end;

    /* ---- 4) Resolver n_segments y validar seg_id (solo SEGMENTO) ------ */
    %if %upcase(&ctx_scope.) = SEGMENTO %then %do;
        proc sql noprint;
            select n_segments into :ctx_n_segments trimmed
            from casuser.cfg_troncales
            where troncal_id = &ctx_troncal_id.;
        quit;
        %put NOTE: [context_and_modules] troncal &ctx_troncal_id. tiene &ctx_n_segments. segmentos.;

        /* Validar seg_id específico si no es ALL */
        %if %upcase(&ctx_seg_id.) ne ALL %then %do;
            %if &ctx_seg_id. < 1 or &ctx_seg_id. > &ctx_n_segments. %then %do;
                %put ERROR: [context_and_modules] seg_id=&ctx_seg_id. fuera de rango (1..&ctx_n_segments.) para troncal=&ctx_troncal_id..;
                %return;
            %end;
        %end;
    %end;
    %else %do;
        /* UNIVERSO: n_segments no aplica */
        %let ctx_n_segments = 0;
    %end;

    %put NOTE: [context_and_modules] Validación OK - scope=&ctx_scope. troncal=&ctx_troncal_id. split=&ctx_split.;
%mend _ctx_validate;
%_ctx_validate;
data &_id_sas_output; dummy=1;run;