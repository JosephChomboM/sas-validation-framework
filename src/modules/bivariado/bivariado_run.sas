/* =========================================================================
bivariado_run.sas - Macro publica del modulo Bivariado (Metodo 4.3)

API:
%bivariado_run(
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
(target, vars num/cat, drivers dri_num/dri_cat)
2) Ejecutar contract (validaciones)
3) Generar reportes HTML + Excel (TRAIN+OOT + DRIVERS en un solo Excel)
4) Cleanup

NOTA: No persiste tablas .sas7bdat (analisis visual solamente).

Dual-input: recibe train + oot promovidas por run_module(dual_input=1).

Compatibilidad: segmento y universo.

Modos de ejecucion (configurados en step_bivariado.sas):
AUTO   - resuelve vars desde config (cfg_segmentos.num_list/cat_list,
fallback cfg_troncales.num_unv/cat_unv + dri_num_*/
dri_cat_*) CUSTOM - usa biv_custom_vars_num / biv_custom_vars_cat
    =========================================================================*/
    /* ---- Incluir componentes del modulo ----------------------------------- */
    %include "&fw_root./src/modules/bivariado/bivariado_contract.sas";
%include "&fw_root./src/modules/bivariado/impl/bivariado_compute.sas";
%include "&fw_root./src/modules/bivariado/impl/bivariado_report.sas";

%macro bivariado_run( input_caslib=PROC, train_table=_train_input,
    oot_table=_oot_input, output_caslib=OUT, troncal_id=, scope=, run_id=);

    /* ---- Return code ---------------------------------------------------- */
    %global _biv_rc;
    %let _biv_rc=0;

    %local _biv_vars_num _biv_vars_cat _biv_target _biv_dri_num _biv_dri_cat
        _report_path _images_path _file_prefix _scope_abbr _biv_is_custom
        _seg_num;

    %put NOTE:======================================================;
    %put NOTE: [bivariado_run] INICIO;
    %put NOTE: troncal=&troncal_id. scope=&scope.;
    %put NOTE: train=&input_caslib..&train_table.;
    %put NOTE: oot=&input_caslib..&oot_table.;
    %put NOTE:======================================================;

    /* ==================================================================
    1) Resolver variables
    ================================================================== */
    %let _biv_vars_num= ;
    %let _biv_vars_cat= ;
    %let _biv_target= ;
    %let _biv_dri_num= ;
    %let _biv_dri_cat= ;
    %let _biv_is_custom=0;

    /* ------ Resolver target desde config del troncal (siempre) --------- */
    proc sql noprint;
        select strip(target) into :_biv_target trimmed from
            casuser.cfg_troncales where troncal_id=&troncal_id.;
    quit;

    /* ------ Modo CUSTOM: variables personalizadas ---------------------- */
    %if %upcase(&biv_mode.)=CUSTOM %then %do;
        %if %length(%superq(biv_custom_vars_num)) > 0 or
            %length(%superq(biv_custom_vars_cat)) > 0 %then %do;
            %let _biv_vars_num=&biv_custom_vars_num.;
            %let _biv_vars_cat=&biv_custom_vars_cat.;
            %let _biv_is_custom=1;
            %put NOTE: [bivariado_run] Modo CUSTOM activado.;
        %end;
        %else %do;
            %put WARNING: [bivariado_run] biv_mode=CUSTOM pero sin variables
                custom. Fallback a AUTO.;
        %end;
    %end;

    /* ------ Modo AUTO (o fallback): variables de configuracion ---------- */
    %if &_biv_is_custom.=0 %then %do;
        %put NOTE: [bivariado_run] Modo AUTO - resolviendo vars desde config.;

        /* Si es segmento, intentar override desde cfg_segmentos */
        %if %substr(&scope., 1, 3)=seg %then %do;
            %let _seg_num=%sysfunc(inputn(%substr(&scope., 4), best.));

            proc sql noprint;
                select strip(num_list) into :_biv_vars_num trimmed from
                    casuser.cfg_segmentos where troncal_id=&troncal_id. and
                    seg_id=&_seg_num.;

                select strip(cat_list) into :_biv_vars_cat trimmed from
                    casuser.cfg_segmentos where troncal_id=&troncal_id. and
                    seg_id=&_seg_num.;

                select strip(dri_num_list) into :_biv_dri_num trimmed from
                    casuser.cfg_segmentos where troncal_id=&troncal_id. and
                    seg_id=&_seg_num.;

                select strip(dri_cat_list) into :_biv_dri_cat trimmed from
                    casuser.cfg_segmentos where troncal_id=&troncal_id. and
                    seg_id=&_seg_num.;
            quit;
        %end;

        /* Fallback a vars del troncal si no hay override */
        %if %length(%superq(_biv_vars_num))=0 %then %do;
            proc sql noprint;
                select strip(num_unv) into :_biv_vars_num trimmed from
                    casuser.cfg_troncales where troncal_id=&troncal_id.;
            quit;
        %end;

        %if %length(%superq(_biv_vars_cat))=0 %then %do;
            proc sql noprint;
                select strip(cat_unv) into :_biv_vars_cat trimmed from
                    casuser.cfg_troncales where troncal_id=&troncal_id.;
            quit;
        %end;

        /* Drivers: fallback a troncal si no hay override */
        %if %length(%superq(_biv_dri_num))=0 %then %do;
            proc sql noprint;
                select strip(dri_num_unv) into :_biv_dri_num trimmed from
                    casuser.cfg_troncales where troncal_id=&troncal_id.;
            quit;
        %end;

        %if %length(%superq(_biv_dri_cat))=0 %then %do;
            proc sql noprint;
                select strip(dri_cat_unv) into :_biv_dri_cat trimmed from
                    casuser.cfg_troncales where troncal_id=&troncal_id.;
            quit;
        %end;
    %end;

    %put NOTE: [bivariado_run] Variables resueltas:;
    %put NOTE: [bivariado_run] target=&_biv_target.;
    %put NOTE: [bivariado_run] num=&_biv_vars_num.;
    %put NOTE: [bivariado_run] cat=&_biv_vars_cat.;
    %put NOTE: [bivariado_run] dri_num=&_biv_dri_num.;
    %put NOTE: [bivariado_run] dri_cat=&_biv_dri_cat.;

    /* ==================================================================
    Determinar rutas de salida
    ================================================================== */
    %if %substr(&scope., 1, 3)=seg %then %let _scope_abbr=&scope.;
    %else %let _scope_abbr=base;

    %if &_biv_is_custom.=1 %then %do;
        %let _report_path=&fw_root./outputs/runs/&run_id./experiments;
        %let _images_path=&fw_root./outputs/runs/&run_id./experiments;
        %let _file_prefix=custom_biv_troncal_&troncal_id._&_scope_abbr.;
        %put NOTE: [bivariado_run] Output -> experiments/ (exploratorio);
    %end;
    %else %do;
        %let _report_path=&fw_root./outputs/runs/&run_id./reports/METOD4.3;
        %let _images_path=&fw_root./outputs/runs/&run_id./images/METOD4.3;
        %let _file_prefix=biv_troncal_&troncal_id._&_scope_abbr.;
        %put NOTE: [bivariado_run] Output -> reports/METOD4.3/ +
            images/METOD4.3/;
    %end;

    /* ==================================================================
    2) Contract - validaciones
    ================================================================== */
    %bivariado_contract( input_caslib=&input_caslib., train_table=&train_table.,
        oot_table=&oot_table., vars_num=&_biv_vars_num.,
        vars_cat=&_biv_vars_cat., target=&_biv_target. );

    %if &_biv_rc. ne 0 %then %do;
        %put ERROR: [bivariado_run] Contract fallido - modulo abortado.;
        %return;
    %end;

    /* ==================================================================
    3) Report - HTML + Excel (incluye computo inline)
    ================================================================== */
    %_bivariado_report( input_caslib=&input_caslib., train_table=&train_table.,
        oot_table=&oot_table., target=&_biv_target., vars_num=&_biv_vars_num.,
        vars_cat=&_biv_vars_cat., dri_num=&_biv_dri_num.,
        dri_cat=&_biv_dri_cat., groups=&biv_n_groups.,
        report_path=&_report_path., images_path=&_images_path.,
        file_prefix=&_file_prefix. );

    /* No se persisten tablas (analisis visual solamente) */
    %put NOTE:======================================================;
    %put NOTE: [bivariado_run] FIN - &_file_prefix. (mode=&biv_mode.);
    %put NOTE:======================================================;

%mend bivariado_run;
