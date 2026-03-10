/* =========================================================================
similitud_run.sas - Macro publica del modulo Similitud (Metodo 4.2)

API:
%similitud_run(
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
(target, byvar, vars num/cat)
2) Ejecutar contract (validaciones)
3) Generar reportes HTML + Excel:
- Seccion 1: Distribucion por buckets (evolutivo TRAIN+OOT)
- Seccion 2: Similitud estadistica (mediana/moda TRAIN vs OOT)
4) Cleanup

NOTA: No persiste tablas .sas7bdat (analisis visual solamente).

Dual-input: recibe train + oot promovidas por run_module(dual_input=1).

Compatibilidad: segmento y universo.

Modos de ejecucion (configurados en step_similitud.sas):
AUTO   - resuelve vars desde config (cfg_segmentos.num_list/cat_list,
fallback cfg_troncales.num_unv/cat_unv + byvar + target)
CUSTOM - usa simil_custom_vars_num / simil_custom_vars_cat
========================================================================= */
/* ---- Incluir componentes del modulo ----------------------------------- */
%include "&fw_root./src/modules/similitud/similitud_contract.sas";
%include "&fw_root./src/modules/similitud/impl/similitud_compute.sas";
%include "&fw_root./src/modules/similitud/impl/similitud_report.sas";

%macro similitud_run( input_caslib=PROC, train_table=_train_input,
    oot_table=_oot_input, output_caslib=OUT, troncal_id=, scope=, run_id=);

    /* ---- Return code ---------------------------------------------------- */
    %global _simil_rc;
    %let _simil_rc=0;

    %local _simil_vars_num _simil_vars_cat _simil_target _simil_byvar
        _report_path _images_path _file_prefix _scope_abbr _simil_is_custom
        _seg_num;

    %put NOTE:======================================================;
    %put NOTE: [similitud_run] INICIO;
    %put NOTE: troncal=&troncal_id. scope=&scope.;
    %put NOTE: train=&input_caslib..&train_table.;
    %put NOTE: oot=&input_caslib..&oot_table.;
    %put NOTE:======================================================;

    /* ==================================================================
    1) Resolver variables
    ================================================================== */
    %let _simil_vars_num= ;
    %let _simil_vars_cat= ;
    %let _simil_target= ;
    %let _simil_byvar= ;
    %let _simil_is_custom=0;

    /* ------ Resolver target y byvar desde config del troncal (siempre) - */
    proc sql noprint;
        select strip(target) into :_simil_target trimmed from
            casuser.cfg_troncales where troncal_id=&troncal_id.;

        select strip(byvar) into :_simil_byvar trimmed from
            casuser.cfg_troncales where troncal_id=&troncal_id.;
    quit;

    /* ------ Modo CUSTOM: variables personalizadas ---------------------- */
    %if %upcase(&simil_mode.)=CUSTOM %then %do;
        %if %length(%superq(simil_custom_vars_num)) > 0 or
            %length(%superq(simil_custom_vars_cat)) > 0 %then %do;
            %let _simil_vars_num=&simil_custom_vars_num.;
            %let _simil_vars_cat=&simil_custom_vars_cat.;
            %let _simil_is_custom=1;
            %put NOTE: [similitud_run] Modo CUSTOM activado.;
        %end;
        %else %do;
            %put WARNING: [similitud_run] simil_mode=CUSTOM pero sin variables
                custom. Fallback a AUTO.;
        %end;
    %end;

    /* ------ Modo AUTO (o fallback): variables de configuracion ---------- */
    %if &_simil_is_custom.=0 %then %do;
        %put NOTE: [similitud_run] Modo AUTO - resolviendo vars desde config.;

        /* Si es segmento, intentar override desde cfg_segmentos */
        %if %substr(&scope., 1, 3)=seg %then %do;
            %let _seg_num=%sysfunc(inputn(%substr(&scope., 4), best.));

            proc sql noprint;
                select strip(num_list) into :_simil_vars_num trimmed from
                    casuser.cfg_segmentos where troncal_id=&troncal_id. and
                    seg_id=&_seg_num.;

                select strip(cat_list) into :_simil_vars_cat trimmed from
                    casuser.cfg_segmentos where troncal_id=&troncal_id. and
                    seg_id=&_seg_num.;
            quit;
        %end;

        /* Fallback a vars del troncal si no hay override */
        %if %length(%superq(_simil_vars_num))=0 %then %do;
            proc sql noprint;
                select strip(num_unv) into :_simil_vars_num trimmed from
                    casuser.cfg_troncales where troncal_id=&troncal_id.;
            quit;
        %end;

        %if %length(%superq(_simil_vars_cat))=0 %then %do;
            proc sql noprint;
                select strip(cat_unv) into :_simil_vars_cat trimmed from
                    casuser.cfg_troncales where troncal_id=&troncal_id.;
            quit;
        %end;
    %end;

    %put NOTE: [similitud_run] Variables resueltas:;
    %put NOTE: [similitud_run] target=&_simil_target.;
    %put NOTE: [similitud_run] byvar=&_simil_byvar.;
    %put NOTE: [similitud_run] num=&_simil_vars_num.;
    %put NOTE: [similitud_run] cat=&_simil_vars_cat.;

    /* ==================================================================
    Determinar rutas de salida
    ================================================================== */
    %if %substr(&scope., 1, 3)=seg %then %let _scope_abbr=&scope.;
    %else %let _scope_abbr=base;

    %if &_simil_is_custom.=1 %then %do;
        %let _report_path=&fw_root./outputs/runs/&run_id./experiments;
        %let _images_path=&fw_root./outputs/runs/&run_id./experiments;
        %let _file_prefix=custom_simil_troncal_&troncal_id._&_scope_abbr.;
        %put NOTE: [similitud_run] Output -> experiments/ (exploratorio);
    %end;
    %else %do;
        %let _report_path=&fw_root./outputs/runs/&run_id./reports/METOD6;
        %let _images_path=&fw_root./outputs/runs/&run_id./images/METOD6;
        %let _file_prefix=simil_troncal_&troncal_id._&_scope_abbr.;
        %put NOTE: [similitud_run] Output -> reports/METOD6/ + images/METOD6/;
    %end;

    /* ==================================================================
    2) Contract - validaciones
    ================================================================== */
    %similitud_contract( input_caslib=&input_caslib., train_table=&train_table.,
        oot_table=&oot_table., vars_num=&_simil_vars_num.,
        vars_cat=&_simil_vars_cat., target=&_simil_target., byvar=&_simil_byvar.
        );

    %if &_simil_rc. ne 0 %then %do;
        %put ERROR: [similitud_run] Contract fallido - modulo abortado.;
        %return;
    %end;

    /* ==================================================================
    3) Report - HTML + Excel (incluye computo inline)
    ================================================================== */
    %_similitud_report( input_caslib=&input_caslib., train_table=&train_table.,
        oot_table=&oot_table., target=&_simil_target., byvar=&_simil_byvar.,
        vars_num=&_simil_vars_num., vars_cat=&_simil_vars_cat.,
        groups=&simil_n_groups., report_path=&_report_path.,
        images_path=&_images_path., file_prefix=&_file_prefix. );

    /* No se persisten tablas (analisis visual solamente) */
    %put NOTE:======================================================;
    %put NOTE: [similitud_run] FIN - &_file_prefix. (mode=&simil_mode.);
    %put NOTE:======================================================;

%mend similitud_run;
