/* =========================================================================
steps/methods/metod_7/step_monotonicidad.sas
Step de modulo: Monotonicidad (METOD7)

Analiza monotonicidad de variables numericas y categoricas vs target usando:
- cortes definidos en TRAIN para variables numericas
- reuso de cortes en OOT
- agrupacion directa para categoricas
- filtro por default cerrado (def_cld de config.sas)

Usa run_module con dual_input=1 (TRAIN + OOT en una ejecucion).

Dependencias:
- &ctx_scope (SEGMENTO | UNIVERSO)
- &run_monotonicidad (0|1)
- &ctx_troncal_id
- SEGMENTO: &ctx_n_segments, &ctx_seg_id (ALL|N)
- casuser.cfg_troncales (promovida en Step 02)
- &fw_root., &run_id
========================================================================= */
/* ---- Dependencias ----------------------------------------------------- */
%include "&fw_root./src/common/common_public.sas";
%include "&fw_root./src/dispatch/run_module.sas";

/* ---- CONFIGURACION DEL MODULO (editar aqui) --------------------------- */
/* mono_mode:
AUTO   -> usa variables de cfg_segmentos/cfg_troncales
          (num_list, cat_list, target, byvar).
          Outputs van a reports/ + images/ (validacion estandar).
CUSTOM -> usa mono_custom_vars_num/cat y permite override target/byvar.
          Outputs van a experiments/ (analisis exploratorio).              */
%let mono_mode=AUTO;

/* Numero de grupos/bins para discretizacion de variables continuas        */
%let mono_n_groups=5;

/* Variables personalizadas (solo si mono_mode=CUSTOM)                    */
%let mono_custom_vars_num= ;
%let mono_custom_vars_cat= ;
%let mono_custom_target= ;
%let mono_custom_byvar= ;

/* ---- EJECUCION -------------------------------------------------------- */
%macro _step_monotonicidad;

    %local _run_mono;
    %if %symexist(run_monotonicidad)=1 %then %let _run_mono=&run_monotonicidad.;
    %else %let _run_mono=0;

    /* ---- 0) Check flag de habilitacion -------------------------------- */
    %if &_run_mono. ne 1 %then %do;
        %put NOTE: [step_monotonicidad] Modulo deshabilitado
            (run_monotonicidad=&_run_mono.). Saltando.;
        %return;
    %end;

    %put NOTE: [step_monotonicidad] Iniciando - scope=&ctx_scope.
        mono_mode=&mono_mode. n_groups=&mono_n_groups.;
    %put NOTE: [step_monotonicidad] Usa dual_input=1 y def_cld (default cerrado).;

    /* ---- 1) Crear CASLIBs PROC + OUT --------------------------------- */
    %_create_caslib(cas_path=&fw_root./data/processed, caslib_name=PROC,
        lib_caslib=PROC, global=Y, cas_sess_name=conn, term_global_sess=0,
        subdirs_flg=1);
    %_create_caslib(cas_path=&fw_root./outputs/runs/&run_id., caslib_name=OUT,
        lib_caslib=OUT, global=Y, cas_sess_name=conn, term_global_sess=0,
        subdirs_flg=1);

    /* ---- 2) Iterar segun ctx_scope ----------------------------------- */
    %if %upcase(&ctx_scope.)=SEGMENTO %then %do;

        %put NOTE: [step_monotonicidad] SEGMENTO: troncal=&ctx_troncal_id.
            n_segments=&ctx_n_segments. seg_id=&ctx_seg_id.;

        %if &ctx_n_segments.=0 %then %do;
            %put WARNING: [step_monotonicidad] Troncal &ctx_troncal_id.
                tiene 0 segmentos. Nada que ejecutar.;
        %end;
        %else %if %upcase(&ctx_seg_id.) ne ALL %then %do;
            %run_module(module=monotonicidad, troncal_id=&ctx_troncal_id.,
                split=, seg_id=&ctx_seg_id., run_id=&run_id., dual_input=1);
        %end;
        %else %do;
            %do _sg=1 %to &ctx_n_segments.;
                %run_module(module=monotonicidad, troncal_id=&ctx_troncal_id.,
                    split=, seg_id=&_sg., run_id=&run_id., dual_input=1);
            %end;
        %end;

    %end;
    %else %if %upcase(&ctx_scope.)=UNIVERSO %then %do;

        %put NOTE: [step_monotonicidad] UNIVERSO: troncal=&ctx_troncal_id.;

        %run_module(module=monotonicidad, troncal_id=&ctx_troncal_id., split=,
            seg_id=, run_id=&run_id., dual_input=1);

    %end;
    %else %do;
        %put ERROR: [step_monotonicidad] ctx_scope=&ctx_scope. no reconocido.
            Debe ser SEGMENTO o UNIVERSO.;
    %end;

    /* ---- 3) Cleanup CASLIBs ------------------------------------------ */
    %_drop_caslib(caslib_name=OUT, cas_sess_name=conn, del_prom_tables=1);
    %_drop_caslib(caslib_name=PROC, cas_sess_name=conn, del_prom_tables=1);

    %put NOTE:======================================================;
    %put NOTE: [step_monotonicidad] Completado (scope=&ctx_scope.);
    %put NOTE:======================================================;

%mend _step_monotonicidad;
%_step_monotonicidad;
