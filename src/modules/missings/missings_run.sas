/* =========================================================================
missings_run.sas - Macro publica del modulo Missings (Metodo 4.2)

API:
%missings_run(
input_caslib  = PROC,
input_table   = _scope_input,
output_caslib = OUT,
troncal_id    = <id>,
scope         = base | segNNN,
run_id        = <run_id>
)

Flujo interno:
1) Resolver variables desde cfg_troncales/cfg_segmentos (vars num/cat)
2) Resolver byvar + ventanas TRAIN/OOT desde cfg_troncales
3) Ejecutar contract (validaciones)
4) Generar reporte consolidado HTML + Excel

NOTA: No persiste tablas .sas7bdat (analisis visual solamente).
Compatibilidad: segmento y universo.

Modos de ejecucion (configurados en step_missings.sas):
AUTO   - resuelve vars desde config (cfg_segmentos.num_list/cat_list,
fallback cfg_troncales.num_unv/cat_unv)
CUSTOM - usa miss_custom_vars_num / miss_custom_vars_cat
========================================================================= */
/* ---- Incluir componentes del modulo ----------------------------------- */
%include "&fw_root./src/modules/missings/missings_contract.sas";
%include "&fw_root./src/modules/missings/impl/missings_compute.sas";
%include "&fw_root./src/modules/missings/impl/missings_report.sas";

%macro missings_run(input_caslib=PROC, input_table=_scope_input,
    output_caslib=OUT, troncal_id=, scope=, run_id=);

    /* ---- Return code ---------------------------------------------------- */
    %global _miss_rc;
    %let _miss_rc=0;

    %local _miss_vars_num _miss_vars_cat _miss_threshold _report_path
        _file_prefix _scope_abbr _miss_is_custom _seg_num _miss_byvar
        _miss_train_min _miss_train_max _miss_oot_min _miss_oot_max;

    %put NOTE:======================================================;
    %put NOTE: [missings_run] INICIO;
    %put NOTE: troncal=&troncal_id. scope=&scope.;
    %put NOTE: input=&input_caslib..&input_table.;
    %put NOTE:======================================================;

    /* ==================================================================
    1) Resolver variables
    ================================================================== */
    %let _miss_vars_num= ;
    %let _miss_vars_cat= ;
    %let _miss_is_custom=0;

    /* Resolver threshold */
    %if %length(%superq(miss_threshold)) > 0 %then
        %let _miss_threshold=&miss_threshold.;
    %else %let _miss_threshold=0.1;

    /* ------ Modo CUSTOM: variables personalizadas ---------------------- */
    %if %upcase(&miss_mode.)=CUSTOM %then %do;
        %if %length(%superq(miss_custom_vars_num)) > 0 or
            %length(%superq(miss_custom_vars_cat)) > 0 %then %do;
            %let _miss_vars_num=&miss_custom_vars_num.;
            %let _miss_vars_cat=&miss_custom_vars_cat.;
            %let _miss_is_custom=1;
            %put NOTE: [missings_run] Modo CUSTOM activado.;
        %end;
        %else %do;
            %put WARNING: [missings_run] miss_mode=CUSTOM pero sin variables
                custom. Fallback a AUTO.;
        %end;
    %end;

    /* ------ Modo AUTO (o fallback): variables de configuracion ---------- */
    %if &_miss_is_custom.=0 %then %do;
        %put NOTE: [missings_run] Modo AUTO - resolviendo vars desde config.;

        /* Si es segmento, intentar override desde cfg_segmentos */
        %if %substr(&scope., 1, 3)=seg %then %do;
            %let _seg_num=%sysfunc(inputn(%substr(&scope., 4), best.));

            proc sql noprint;
                select strip(num_list)
                    into :_miss_vars_num trimmed
                from casuser.cfg_segmentos
                where troncal_id=&troncal_id.
                  and seg_id=&_seg_num.;

                select strip(cat_list)
                    into :_miss_vars_cat trimmed
                from casuser.cfg_segmentos
                where troncal_id=&troncal_id.
                  and seg_id=&_seg_num.;
            quit;
        %end;

        /* Fallback a vars del troncal si no hay override */
        %if %length(%superq(_miss_vars_num))=0 %then %do;
            proc sql noprint;
                select strip(num_unv)
                    into :_miss_vars_num trimmed
                from casuser.cfg_troncales
                where troncal_id=&troncal_id.;
            quit;
        %end;

        %if %length(%superq(_miss_vars_cat))=0 %then %do;
            proc sql noprint;
                select strip(cat_unv)
                    into :_miss_vars_cat trimmed
                from casuser.cfg_troncales
                where troncal_id=&troncal_id.;
            quit;
        %end;
    %end;

    %put NOTE: [missings_run] Variables resueltas:;
    %put NOTE: [missings_run] num=&_miss_vars_num.;
    %put NOTE: [missings_run] cat=&_miss_vars_cat.;
    %put NOTE: [missings_run] threshold=&_miss_threshold.;

    /* ==================================================================
    2) Resolver byvar y ventanas TRAIN/OOT
    ================================================================== */
    proc sql noprint;
        select strip(byvar),
               strip(put(train_min_mes, best.)),
               strip(put(train_max_mes, best.)),
               strip(put(oot_min_mes, best.)),
               strip(put(oot_max_mes, best.))
          into :_miss_byvar trimmed,
               :_miss_train_min trimmed,
               :_miss_train_max trimmed,
               :_miss_oot_min trimmed,
               :_miss_oot_max trimmed
        from casuser.cfg_troncales
        where troncal_id=&troncal_id.;
    quit;

    %put NOTE: [missings_run] byvar=&_miss_byvar.;
    %put NOTE: [missings_run] Ventanas TRAIN=&_miss_train_min.-&_miss_train_max.
        OOT=&_miss_oot_min.-&_miss_oot_max..;

    /* ==================================================================
    3) Determinar rutas de salida
    ================================================================== */
    %if %substr(&scope., 1, 3)=seg %then %let _scope_abbr=&scope.;
    %else %let _scope_abbr=base;

    %if &_miss_is_custom.=1 %then %do;
        %let _report_path=&fw_root./outputs/runs/&run_id./experiments;
        %let _file_prefix=custom_miss_troncal_&troncal_id._&_scope_abbr.;
        %put NOTE: [missings_run] Output -> experiments/ (exploratorio);
    %end;
    %else %do;
        %let _report_path=&fw_root./outputs/runs/&run_id./reports/METOD4.2;
        %let _file_prefix=miss_troncal_&troncal_id._&_scope_abbr.;
        %put NOTE: [missings_run] Output -> reports/METOD4.2/.;
    %end;

    /* ==================================================================
    4) Contract - validaciones
    ================================================================== */
    %missings_contract(input_caslib=&input_caslib., input_table=&input_table.,
        byvar=&_miss_byvar., train_min_mes=&_miss_train_min.,
        train_max_mes=&_miss_train_max., oot_min_mes=&_miss_oot_min.,
        oot_max_mes=&_miss_oot_max., vars_num=&_miss_vars_num.,
        vars_cat=&_miss_vars_cat.);

    %if &_miss_rc. ne 0 %then %do;
        %put ERROR: [missings_run] Contract fallido - modulo abortado.;
        %return;
    %end;

    /* ==================================================================
    5) Report - HTML + Excel consolidado TRAIN/OOT
    ================================================================== */
    %_missings_report(input_caslib=&input_caslib., input_table=&input_table.,
        byvar=&_miss_byvar., train_min_mes=&_miss_train_min.,
        train_max_mes=&_miss_train_max., oot_min_mes=&_miss_oot_min.,
        oot_max_mes=&_miss_oot_max., vars_num=&_miss_vars_num.,
        vars_cat=&_miss_vars_cat., threshold=&_miss_threshold.,
        report_path=&_report_path., file_prefix=&_file_prefix.);

    /* No se persisten tablas (analisis visual solamente) */
    %put NOTE:======================================================;
    %put NOTE: [missings_run] FIN - &_file_prefix. (mode=&miss_mode.);
    %put NOTE:======================================================;

%mend missings_run;
