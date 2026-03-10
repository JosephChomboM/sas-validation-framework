/* =========================================================================
monotonicidad_run.sas - Macro publica del modulo Monotonicidad (METOD7)

API:
%monotonicidad_run(
    input_caslib  = PROC,
    train_table   = _train_input,
    oot_table     = _oot_input,
    output_caslib = OUT,
    troncal_id    = <id>,
    scope         = base | segNNN,
    run_id        = <run_id>
)

Flujo interno:
1) Determinar modo (AUTO / CUSTOM) y resolver variables num/cat
2) Resolver target, byvar y def_cld
3) Ejecutar contract (validaciones)
4) Generar reportes TRAIN/OOT con cortes TRAIN -> OOT

Regla de negocio:
- Siempre usa default cerrado (def_cld) para filtrar el analisis.
- AUTO usa cfg_segmentos/cfg_troncales.
- CUSTOM usa listas definidas en el step y permite override de target/byvar.

Dual-input: recibe train + oot promovidas por run_module(dual_input=1).
Compatibilidad: segmento y universo.
========================================================================= */
/* ---- Incluir componentes del modulo ----------------------------------- */
%include "&fw_root./src/modules/monotonicidad/monotonicidad_contract.sas";
%include "&fw_root./src/modules/monotonicidad/impl/monotonicidad_compute.sas";
%include "&fw_root./src/modules/monotonicidad/impl/monotonicidad_report.sas";

%macro monotonicidad_run(input_caslib=PROC, train_table=_train_input,
    oot_table=_oot_input, output_caslib=OUT, troncal_id=, scope=, run_id=);

    %global _mono_rc;
    %let _mono_rc=0;

    %local _mono_vars_num _mono_vars_cat _mono_target _mono_byvar
        _mono_def_cld _mono_groups _scope_abbr _report_path _images_path
        _file_prefix _mono_is_custom _seg_num;

    %put NOTE:======================================================;
    %put NOTE: [monotonicidad_run] INICIO;
    %put NOTE: troncal=&troncal_id. scope=&scope.;
    %put NOTE: train=&input_caslib..&train_table.;
    %put NOTE: oot=&input_caslib..&oot_table.;
    %put NOTE:======================================================;

    /* ==================================================================
       1) Resolver variables
       ================================================================== */
    %let _mono_vars_num= ;
    %let _mono_vars_cat= ;
    %let _mono_target= ;
    %let _mono_byvar= ;
    %let _mono_is_custom=0;

    proc sql noprint;
        select strip(target) into :_mono_target trimmed from
            casuser.cfg_troncales where troncal_id=&troncal_id.;
        select strip(byvar) into :_mono_byvar trimmed from
            casuser.cfg_troncales where troncal_id=&troncal_id.;
        select strip(put(def_cld, best.)) into :_mono_def_cld trimmed from
            casuser.cfg_troncales where troncal_id=&troncal_id.;
    quit;

    /* ------ Modo CUSTOM: variables del step --------------------------- */
    %if %upcase(&mono_mode.)=CUSTOM %then %do;
        %if %length(%superq(mono_custom_vars_num)) > 0 or
            %length(%superq(mono_custom_vars_cat)) > 0 %then %do;
            %let _mono_vars_num=&mono_custom_vars_num.;
            %let _mono_vars_cat=&mono_custom_vars_cat.;
            %let _mono_is_custom=1;

            %if %length(%superq(mono_custom_target)) > 0 %then
                %let _mono_target=&mono_custom_target.;
            %if %length(%superq(mono_custom_byvar)) > 0 %then
                %let _mono_byvar=&mono_custom_byvar.;

            %put NOTE: [monotonicidad_run] Modo CUSTOM activado.;
        %end;
        %else %do;
            %put WARNING: [monotonicidad_run] mono_mode=CUSTOM pero sin
                variables custom. Fallback a AUTO.;
        %end;
    %end;

    /* ------ Modo AUTO (o fallback) ----------------------------------- */
    %if &_mono_is_custom.=0 %then %do;
        %put NOTE: [monotonicidad_run] Modo AUTO - resolviendo vars desde config.;

        %if %substr(&scope., 1, 3)=seg %then %do;
            %let _seg_num=%sysfunc(inputn(%substr(&scope., 4), best.));

            proc sql noprint;
                select strip(num_list) into :_mono_vars_num trimmed from
                    casuser.cfg_segmentos where troncal_id=&troncal_id.
                    and seg_id=&_seg_num.;
                select strip(cat_list) into :_mono_vars_cat trimmed from
                    casuser.cfg_segmentos where troncal_id=&troncal_id.
                    and seg_id=&_seg_num.;
            quit;
        %end;

        %if %length(%superq(_mono_vars_num))=0 %then %do;
            proc sql noprint;
                select strip(num_unv) into :_mono_vars_num trimmed from
                    casuser.cfg_troncales where troncal_id=&troncal_id.;
            quit;
        %end;

        %if %length(%superq(_mono_vars_cat))=0 %then %do;
            proc sql noprint;
                select strip(cat_unv) into :_mono_vars_cat trimmed from
                    casuser.cfg_troncales where troncal_id=&troncal_id.;
            quit;
        %end;
    %end;

    %if %symexist(mono_n_groups)=1 %then %let _mono_groups=&mono_n_groups.;
    %else %if %symexist(mono_groups)=1 %then %let _mono_groups=&mono_groups.;
    %else %let _mono_groups=5;
    %if %length(%superq(_mono_groups))=0 %then %let _mono_groups=5;

    %put NOTE: [monotonicidad_run] Variables resueltas:;
    %put NOTE: [monotonicidad_run] byvar=&_mono_byvar.;
    %put NOTE: [monotonicidad_run] target=&_mono_target.;
    %put NOTE: [monotonicidad_run] num=&_mono_vars_num.;
    %put NOTE: [monotonicidad_run] cat=&_mono_vars_cat.;
    %put NOTE: [monotonicidad_run] def_cld=&_mono_def_cld. groups=&_mono_groups.;

    /* ==================================================================
       2) Definir rutas de salida (METOD7)
       ================================================================== */
    %if %substr(&scope., 1, 3)=seg %then %let _scope_abbr=&scope.;
    %else %let _scope_abbr=base;

    %if &_mono_is_custom.=1 %then %do;
        %let _report_path=&fw_root./outputs/runs/&run_id./experiments;
        %let _images_path=&fw_root./outputs/runs/&run_id./experiments;
        %let _file_prefix=custom_monotonicidad_troncal_&troncal_id._&_scope_abbr.;
        %put NOTE: [monotonicidad_run] Output -> experiments/ (exploratorio).;
    %end;
    %else %do;
        %let _report_path=&fw_root./outputs/runs/&run_id./reports/METOD7;
        %let _images_path=&fw_root./outputs/runs/&run_id./images/METOD7;
        %let _file_prefix=monotonicidad_troncal_&troncal_id._&_scope_abbr.;
        %put NOTE: [monotonicidad_run] Output -> reports/METOD7 + images/METOD7.;
    %end;

    /* ==================================================================
       3) Contract - validaciones
       ================================================================== */
    %monotonicidad_contract(input_caslib=&input_caslib.,
        train_table=&train_table., oot_table=&oot_table.,
        vars_num=&_mono_vars_num., vars_cat=&_mono_vars_cat.,
        target=&_mono_target., byvar=&_mono_byvar., def_cld=&_mono_def_cld.);

    %if &_mono_rc. ne 0 %then %do;
        %put ERROR: [monotonicidad_run] Contract fallido - modulo abortado.;
        %return;
    %end;

    /* ==================================================================
       4) Report - HTML + Excel + JPEG
       ================================================================== */
    %_monotonicidad_report(input_caslib=&input_caslib.,
        train_table=&train_table., oot_table=&oot_table., byvar=&_mono_byvar.,
        target=&_mono_target., vars_num=&_mono_vars_num.,
        vars_cat=&_mono_vars_cat.,
        def_cld=&_mono_def_cld., groups=&_mono_groups.,
        report_path=&_report_path., images_path=&_images_path.,
        file_prefix=&_file_prefix.);

    %put NOTE:======================================================;
    %put NOTE: [monotonicidad_run] FIN - &_file_prefix. (mode=&mono_mode.);
    %put NOTE:======================================================;

%mend monotonicidad_run;
