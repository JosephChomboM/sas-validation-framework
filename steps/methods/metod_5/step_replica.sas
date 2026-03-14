/* =========================================================================
steps/methods/metod_5/step_replica.sas
Step de modulo: Replica de Modelo (Metodo 5.2.1)

Replica corre TRAIN + OOT en una sola ejecucion usando dual_input=1 y:
- replica logistica del set de variables
- pesos por variable
- chequeos de supuestos (VIF, normalidad, Levene, Durbin-Watson)
- contraste opcional de y_est vs PD/XB/TARGET
- filtro por default cerrado (def_cld)

Dependencias:
- &ctx_scope (SEGMENTO | UNIVERSO)
- &run_replica (0|1)
- &ctx_troncal_id
- SEGMENTO: &ctx_n_segments, &ctx_seg_id (ALL|N)
- casuser.cfg_troncales / cfg_segmentos
- &fw_root., &run_id
========================================================================= */
/* ---- Dependencias ----------------------------------------------------- */
%include "&fw_root./src/common/common_public.sas";
%include "&fw_root./src/dispatch/run_module.sas";

/* ---- CONFIGURACION DEL MODULO (editar aqui) --------------------------- */
/* replica_mode:
AUTO   -> usa num_list/cfg_segmentos con fallback num_unv + target/byvar/
          pd/xb/def_cld desde config.sas.
          Outputs van a reports/ + images/ (validacion estandar).
CUSTOM -> usa replica_custom_vars y permite override de target, time_var,
          control_var y def_cld. byvar sigue viniendo de config.sas.
          Outputs van a experiments/ (analisis exploratorio).             */
%let replica_mode=AUTO;

/* 1 = logistica ponderada; 0 = estandar                                 */
%let replica_ponderada=1;

/* Numero de grupos para el test de Levene sobre residuales              */
%let replica_n_groups=10;

/* Control opcional para contrastar y_est vs variable del framework:
   AUTO = usa PD si existe, si no XB
   NONE = no genera tabla/grafico de control
   PD   = usa cfg_troncales.pd
   XB   = usa cfg_troncales.xb
   TARGET = usa target como referencia                                   */
%let replica_control_source=AUTO;

/* Variables personalizadas (solo si replica_mode=CUSTOM)                */
%let replica_custom_vars= ;
%let replica_custom_target= ;
/* Dejar vacio para usar byvar de config. Usar NONE para omitir DW.       */
%let replica_custom_time_var= ;
/* Puede apuntar a PD/XB/otra variable si no quieres usar control_source. */
%let replica_custom_control_var= ;
/* Fecha de default cerrado en formato YYYYMM                            */
%let replica_custom_def_cld= ;

/* ---- EJECUCION -------------------------------------------------------- */
%macro _step_replica;

    %local _run_replica _step_rc;
    %let _step_rc=0;
    %if %symexist(run_replica)=1 %then %let _run_replica=&run_replica.;
    %else %let _run_replica=0;

    %fw_log_start(step_name=step_replica, run_id=&run_id.,
        fw_root=&fw_root., log_stem=metod_5_step_replica);

    /* ---- 0) Check flag de habilitacion -------------------------------- */
    %if &_run_replica. ne 1 %then %do;
        %put NOTE: [step_replica] Modulo deshabilitado
            (run_replica=&_run_replica.). Saltando.;
        %goto _step_replica_end;
    %end;

    %put NOTE: [step_replica] Iniciando - scope=&ctx_scope.
        replica_mode=&replica_mode. ponderada=&replica_ponderada.
        control_source=&replica_control_source.;

    /* ---- 1) Crear CASLIBs PROC + OUT --------------------------------- */
    %_create_caslib(cas_path=&fw_root./data/processed, caslib_name=PROC,
        lib_caslib=PROC, global=Y, cas_sess_name=conn, term_global_sess=0,
        subdirs_flg=1);
    %_create_caslib(cas_path=&fw_root./outputs/runs/&run_id., caslib_name=OUT,
        lib_caslib=OUT, global=Y, cas_sess_name=conn, term_global_sess=0,
        subdirs_flg=1);

    /* ---- 2) Iterar segun ctx_scope ----------------------------------- */
    %if %upcase(&ctx_scope.)=SEGMENTO %then %do;

        %put NOTE: [step_replica] SEGMENTO: troncal=&ctx_troncal_id.
            n_segments=&ctx_n_segments. seg_id=&ctx_seg_id.;

        %if &ctx_n_segments.=0 %then %do;
            %put WARNING: [step_replica] Troncal &ctx_troncal_id. tiene 0
                segmentos. Nada que ejecutar.;
        %end;
        %else %if %upcase(&ctx_seg_id.) ne ALL %then %do;
            %run_module(module=replica, troncal_id=&ctx_troncal_id., split=,
                seg_id=&ctx_seg_id., run_id=&run_id., dual_input=1);
        %end;
        %else %do;
            %do _sg=1 %to &ctx_n_segments.;
                %run_module(module=replica, troncal_id=&ctx_troncal_id.,
                    split=, seg_id=&_sg., run_id=&run_id., dual_input=1);
            %end;
        %end;

    %end;
    %else %if %upcase(&ctx_scope.)=UNIVERSO %then %do;

        %put NOTE: [step_replica] UNIVERSO: troncal=&ctx_troncal_id.;

        %run_module(module=replica, troncal_id=&ctx_troncal_id., split=,
            seg_id=, run_id=&run_id., dual_input=1);

    %end;
    %else %do;
        %put ERROR: [step_replica] ctx_scope=&ctx_scope. no reconocido.
            Debe ser SEGMENTO o UNIVERSO.;
    %end;

    /* ---- 3) Cleanup CASLIBs ------------------------------------------ */
    %_drop_caslib(caslib_name=OUT, cas_sess_name=conn, del_prom_tables=1);
    %_drop_caslib(caslib_name=PROC, cas_sess_name=conn, del_prom_tables=1);

    %put NOTE:======================================================;
    %put NOTE: [step_replica] Completado (scope=&ctx_scope.
        mode=&replica_mode.);
    %put NOTE:======================================================;

%_step_replica_end:
    %fw_log_stop(step_name=step_replica, step_rc=&_step_rc.);

%mend _step_replica;
%_step_replica;
