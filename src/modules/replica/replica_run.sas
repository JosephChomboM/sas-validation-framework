/* =========================================================================
replica_run.sas - Macro publica del modulo Replica (Metodo 5.2.1)

API:
%replica_run(
    input_caslib  = PROC,
    train_table   = _train_input,
    oot_table     = _oot_input,
    output_caslib = OUT,
    troncal_id    = <id>,
    scope         = base | segNNN,
    run_id        = <run_id>
)

Flujo interno:
1) Resolver variables desde cfg_troncales/cfg_segmentos
   (target, pd, xb, byvar, def_cld, lista num)
2) Resolver modo AUTO/CUSTOM y control opcional (PD/XB/TARGET)
3) Ejecutar contract (validaciones)
4) Generar reportes HTML + Excel + JPEG (TRAIN + OOT)

Regla de negocio:
- Replica usa target para la regresion logistica y filtra por default
  cerrado (def_cld).
- byvar siempre se resuelve desde config.sas.
- En CUSTOM se puede overridear vars, target, time_var, control_var
  y def_cld.
- control_var es opcional; puede apuntar a PD o XB para contrastar
  la probabilidad replicada vs el score/config original.

Dual-input: recibe train + oot promovidas por run_module(dual_input=1).
Compatibilidad: segmento y universo.
========================================================================= */
/* ---- Incluir componentes del modulo ----------------------------------- */
%include "&fw_root./src/modules/replica/replica_contract.sas";
%include "&fw_root./src/modules/replica/impl/replica_compute.sas";
%include "&fw_root./src/modules/replica/impl/replica_report.sas";

%macro replica_run(input_caslib=PROC, train_table=_train_input,
    oot_table=_oot_input, output_caslib=OUT, troncal_id=, scope=, run_id=);

    %global _rep_rc;
    %let _rep_rc=0;

    %local _rep_vars _rep_target _rep_byvar _rep_def_cld _rep_time_var
        _rep_pd _rep_xb _rep_control_var _rep_control_mode _scope_abbr
        _report_path _images_path _file_prefix _rep_is_custom _seg_num;

    %put NOTE:======================================================;
    %put NOTE: [replica_run] INICIO;
    %put NOTE: troncal=&troncal_id. scope=&scope.;
    %put NOTE: train=&input_caslib..&train_table.;
    %put NOTE: oot=&input_caslib..&oot_table.;
    %put NOTE:======================================================;

    /* ==================================================================
       1) Resolver variables base desde cfg_troncales
       ================================================================== */
    %let _rep_vars= ;
    %let _rep_target= ;
    %let _rep_byvar= ;
    %let _rep_def_cld= ;
    %let _rep_time_var= ;
    %let _rep_pd= ;
    %let _rep_xb= ;
    %let _rep_control_var= ;
    %let _rep_is_custom=0;

    proc sql noprint;
        select strip(target) into :_rep_target trimmed
        from casuser.cfg_troncales
        where troncal_id=&troncal_id.;

        select strip(byvar) into :_rep_byvar trimmed
        from casuser.cfg_troncales
        where troncal_id=&troncal_id.;

        select strip(put(def_cld, best.)) into :_rep_def_cld trimmed
        from casuser.cfg_troncales
        where troncal_id=&troncal_id.;

        select strip(pd) into :_rep_pd trimmed
        from casuser.cfg_troncales
        where troncal_id=&troncal_id.;

        select strip(xb) into :_rep_xb trimmed
        from casuser.cfg_troncales
        where troncal_id=&troncal_id.;
    quit;

    %let _rep_time_var=&_rep_byvar.;
    %let _rep_control_mode=%upcase(%superq(replica_control_source));
    %if %length(%superq(_rep_control_mode))=0 %then %let _rep_control_mode=AUTO;

    /* ==================================================================
       2) Modo CUSTOM
       ================================================================== */
    %if %upcase(&replica_mode.)=CUSTOM %then %do;
        %if %length(%superq(replica_custom_vars)) > 0 %then %do;
            %let _rep_vars=&replica_custom_vars.;
            %let _rep_is_custom=1;

            %if %length(%superq(replica_custom_target)) > 0 %then
                %let _rep_target=&replica_custom_target.;
            %if %length(%superq(replica_custom_def_cld)) > 0 %then
                %let _rep_def_cld=&replica_custom_def_cld.;

            %if %length(%superq(replica_custom_time_var)) > 0 %then %do;
                %if %upcase(%superq(replica_custom_time_var))=NONE %then
                    %let _rep_time_var=;
                %else
                    %let _rep_time_var=&replica_custom_time_var.;
            %end;

            %if %length(%superq(replica_custom_control_var)) > 0 %then
                %let _rep_control_var=&replica_custom_control_var.;

            %put NOTE: [replica_run] Modo CUSTOM activado.;
        %end;
        %else %do;
            %put WARNING: [replica_run] replica_mode=CUSTOM pero sin vars
                custom. Fallback a AUTO.;
        %end;
    %end;

    /* ==================================================================
       3) Modo AUTO (o fallback)
       ================================================================== */
    %if &_rep_is_custom.=0 %then %do;
        %put NOTE: [replica_run] Modo AUTO - resolviendo vars desde config.;

        %if %substr(&scope., 1, 3)=seg %then %do;
            %let _seg_num=%sysfunc(inputn(%substr(&scope., 4), best.));

            proc sql noprint;
                select strip(num_list) into :_rep_vars trimmed
                from casuser.cfg_segmentos
                where troncal_id=&troncal_id.
                    and seg_id=&_seg_num.;
            quit;
        %end;

        %if %length(%superq(_rep_vars))=0 %then %do;
            proc sql noprint;
                select strip(num_unv) into :_rep_vars trimmed
                from casuser.cfg_troncales
                where troncal_id=&troncal_id.;
            quit;
        %end;
    %end;

    /* ==================================================================
       4) Resolver control opcional (PD/XB/TARGET/NONE/AUTO)
       ================================================================== */
    %if %length(%superq(_rep_control_var))=0 %then %do;
        %if &_rep_control_mode.=PD %then %let _rep_control_var=&_rep_pd.;
        %else %if &_rep_control_mode.=XB %then %let _rep_control_var=&_rep_xb.;
        %else %if &_rep_control_mode.=TARGET %then
            %let _rep_control_var=&_rep_target.;
        %else %if &_rep_control_mode.=NONE %then %let _rep_control_var=;
        %else %do;
            %if %length(%superq(_rep_pd)) > 0 %then
                %let _rep_control_var=&_rep_pd.;
            %else %if %length(%superq(_rep_xb)) > 0 %then
                %let _rep_control_var=&_rep_xb.;
        %end;
    %end;

    %put NOTE: [replica_run] Variables resueltas:;
    %put NOTE: [replica_run] target=&_rep_target. byvar=&_rep_byvar.;
    %put NOTE: [replica_run] vars=&_rep_vars.;
    %put NOTE: [replica_run] time_var=&_rep_time_var.;
    %put NOTE: [replica_run] pd=&_rep_pd. xb=&_rep_xb.;
    %put NOTE: [replica_run] control_var=&_rep_control_var.;
    %put NOTE: [replica_run] def_cld=&_rep_def_cld.;

    /* ==================================================================
       5) Determinar rutas de salida
       ================================================================== */
    %if %substr(&scope., 1, 3)=seg %then %let _scope_abbr=&scope.;
    %else %let _scope_abbr=base;

    %if &_rep_is_custom.=1 %then %do;
        %let _report_path=&fw_root./outputs/runs/&run_id./experiments;
        %let _images_path=&fw_root./outputs/runs/&run_id./experiments;
        %let _file_prefix=custom_replica_troncal_&troncal_id._&_scope_abbr.;
        %put NOTE: [replica_run] Output -> experiments/ (exploratorio).;
    %end;
    %else %do;
        %let _report_path=&fw_root./outputs/runs/&run_id./reports/METOD5.2.1;
        %let _images_path=&fw_root./outputs/runs/&run_id./images/METOD5.2.1;
        %let _file_prefix=replica_troncal_&troncal_id._&_scope_abbr.;
        %put NOTE: [replica_run] Output -> reports/METOD5.2.1 + images/METOD5.2.1.;
    %end;

    /* ==================================================================
       6) Contract - validaciones
       ================================================================== */
    %replica_contract(input_caslib=&input_caslib., train_table=&train_table.,
        oot_table=&oot_table., lista_variables=&_rep_vars.,
        target=&_rep_target., byvar=&_rep_byvar., def_cld=&_rep_def_cld.,
        time_var=&_rep_time_var., control_var=&_rep_control_var.);

    %if &_rep_rc. ne 0 %then %do;
        %put ERROR: [replica_run] Contract fallido - modulo abortado.;
        %return;
    %end;

    /* ==================================================================
       7) Report - HTML + Excel + JPEG
       ================================================================== */
    %_replica_report(input_caslib=&input_caslib., train_table=&train_table.,
        oot_table=&oot_table., byvar=&_rep_byvar., target=&_rep_target.,
        vars_num=&_rep_vars., time_var=&_rep_time_var.,
        control_var=&_rep_control_var., def_cld=&_rep_def_cld.,
        ponderada=&replica_ponderada., groups=&replica_n_groups.,
        run_id=&run_id., report_path=&_report_path.,
        images_path=&_images_path.,
        file_prefix=&_file_prefix.);

    %put NOTE:======================================================;
    %put NOTE: [replica_run] FIN - &_file_prefix. (mode=&replica_mode.);
    %put NOTE:======================================================;

%mend replica_run;
