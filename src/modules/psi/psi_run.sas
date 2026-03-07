/* =========================================================================
psi_run.sas - Macro pública del módulo PSI (Population Stability Index)

API:
%psi_run(
input_caslib  = PROC,
train_table   = _train_input,
oot_table     = _oot_input,
output_caslib = OUT,
troncal_id    = <id>,
scope         = base | segNNN,
run_id        = <run_id>
)

Flujo interno:
1) Determinar modo (AUTO / CUSTOM) y resolver variables (num + cat + byvar)
- AUTO   → cfg_segmentos / cfg_troncales
- CUSTOM → &psi_custom_vars_num/cat/byvar (definidas en step_psi.sas)
2) Ejecutar contract (validaciones)
3) Calcular PSI: cubo + cubo_wide + resumen
4) Generar reportes HTML + Excel + PNG (tendencia temporal)
- AUTO   → outputs/runs/<run_id>/reports/ + images/
- CUSTOM → outputs/runs/<run_id>/experiments/
5) Persistir tablas como .sas7bdat vía libname
6) Cleanup tablas temporales (work)

Variables globales leídas (definidas en step_psi.sas):
&psi_mode              - AUTO | CUSTOM
&psi_n_buckets         - número de bins (default 10)
&psi_mensual           - 1=breakdown mensual, 0=solo total
&psi_custom_vars_num   - lista vars numéricas (solo CUSTOM)
&psi_custom_vars_cat   - lista vars categóricas (solo CUSTOM)
&psi_custom_byvar      - variable temporal (solo CUSTOM)

NOTA: PSI es dual-input (train+oot). Recibe dos tablas promovidas.
No usa split como parámetro (siempre train vs oot).

Compatibilidad: segmento y universo.
========================================================================= */
/* ---- Incluir componentes del módulo ----------------------------------- */
%include "&fw_root./src/modules/psi/psi_contract.sas";
%include "&fw_root./src/modules/psi/impl/psi_compute.sas";
%include "&fw_root./src/modules/psi/impl/psi_report.sas";

%macro psi_run( input_caslib=PROC, train_table=_train_input, oot_table=
    _oot_input, output_caslib=OUT, troncal_id=, scope=, run_id=);

    /* ---- Return code: owned here, used by contract ---------------------- */
    %global _psi_rc;
    %let _psi_rc=0;

    %local _psi_vars_num _psi_vars_cat _psi_byvar _report_path _tables_path
        _images_path _file_prefix _tbl_prefix _psi_is_custom _seg_num
        _scope_abbr;

    %put NOTE:======================================================;
    %put NOTE: [psi_run] INICIO;
    %put NOTE: troncal=&troncal_id. scope=&scope.;
    %put NOTE: train=&input_caslib..&train_table.;
    %put NOTE: oot=&input_caslib..&oot_table.;
    %put NOTE: mode=&psi_mode.;
    %put NOTE:======================================================;

    /* ==================================================================
    1) Determinar modo y resolver variables
    ================================================================== */
    %let _psi_vars_num= ;
    %let _psi_vars_cat= ;
    %let _psi_byvar= ;
    %let _psi_is_custom=0;

    /* ------ Modo CUSTOM: variables personalizadas ---------------------- */
    %if %upcase(&psi_mode.)=CUSTOM %then %do;
        %if %length(%superq(psi_custom_vars_num)) > 0 or
            %length(%superq(psi_custom_vars_cat)) > 0 %then %do;
            %let _psi_vars_num=&psi_custom_vars_num.;
            %let _psi_vars_cat=&psi_custom_vars_cat.;
            %let _psi_byvar=&psi_custom_byvar.;
            %let _psi_is_custom=1;
            %put NOTE: [psi_run] Modo CUSTOM - vars_num=&_psi_vars_num.
                vars_cat=&_psi_vars_cat. byvar=&_psi_byvar.;
        %end;
        %else %do;
            %put WARNING: [psi_run] psi_mode=CUSTOM pero sin variables custom.
                Fallback a AUTO.;
        %end;
    %end;

    /* ------ Modo AUTO (o fallback): variables de configuración --------- */
    %if &_psi_is_custom.=0 %then %do;
        %put NOTE: [psi_run] Modo AUTO - resolviendo vars desde config.;

        /* Resolver byvar desde config del troncal */
        proc sql noprint;
            select strip(byvar) into :_psi_byvar trimmed from
                casuser.cfg_troncales where troncal_id=&troncal_id.;
        quit;

        /* Si es segmento, intentar override desde cfg_segmentos */
        %if %substr(&scope., 1, 3)=seg %then %do;
            %let _seg_num=%sysfunc(inputn(%substr(&scope., 4), best.));

            proc sql noprint;
                select strip(num_list) into :_psi_vars_num trimmed from
                    casuser.cfg_segmentos where troncal_id=&troncal_id. and
                    seg_id=&_seg_num.;

                select strip(cat_list) into :_psi_vars_cat trimmed from
                    casuser.cfg_segmentos where troncal_id=&troncal_id. and
                    seg_id=&_seg_num.;
            quit;
        %end;

        /* Fallback a vars del troncal si no hay override */
        %if %length(%superq(_psi_vars_num))=0 %then %do;
            proc sql noprint;
                select strip(num_unv) into :_psi_vars_num trimmed from
                    casuser.cfg_troncales where troncal_id=&troncal_id.;
            quit;
        %end;

        %if %length(%superq(_psi_vars_cat))=0 %then %do;
            proc sql noprint;
                select strip(cat_unv) into :_psi_vars_cat trimmed from
                    casuser.cfg_troncales where troncal_id=&troncal_id.;
            quit;
        %end;
    %end;

    %put NOTE: [psi_run] Variables resueltas:;
    %put NOTE: [psi_run] num=&_psi_vars_num.;
    %put NOTE: [psi_run] cat=&_psi_vars_cat.;
    %put NOTE: [psi_run] byvar=&_psi_byvar.;

    /* ==================================================================
    Determinar rutas de salida según modo
    AUTO   → reports/ + tables/ + images/
    CUSTOM → experiments/

    Naming de tablas .sas7bdat - máximo 32 caracteres (límite SAS):
    Formato: <mod>_t<N>_<scope>_<tipo>
    Ej: psi_t1_base_cubo (16 chars), psi_t1_seg001_cubo (19 chars)
    ================================================================== */
    %if %substr(&scope., 1, 3)=seg %then %let _scope_abbr=&scope.;
    %else %let _scope_abbr=base;

    %if &_psi_is_custom.=1 %then %do;
        %let _report_path=&fw_root./outputs/runs/&run_id./experiments;
        %let _tables_path=&fw_root./outputs/runs/&run_id./experiments;
        %let _images_path=&fw_root./outputs/runs/&run_id./experiments;
        %let _file_prefix=custom_psi_troncal_&troncal_id._&_scope_abbr.;
        %let _tbl_prefix=cx_psi_t&troncal_id._&_scope_abbr.;
        %put NOTE: [psi_run] Output → experiments/ (exploratorio);
    %end;
    %else %do;
        %let _report_path=&fw_root./outputs/runs/&run_id./reports/METOD4.2;
        %let _tables_path=&fw_root./outputs/runs/&run_id./tables/METOD4.2;
        %let _images_path=&fw_root./outputs/runs/&run_id./images/METOD4.2;
        %let _file_prefix=psi_troncal_&troncal_id._&_scope_abbr.;
        %let _tbl_prefix=psi_t&troncal_id._&_scope_abbr.;
        %put NOTE: [psi_run] Output → reports/METOD4.2/ (estándar);
    %end;

    /* ==================================================================
    2) Contract - validaciones pre-ejecución
    ================================================================== */
    %psi_contract( input_caslib=&input_caslib., train_table=&train_table.,
        oot_table=&oot_table., vars_num=&_psi_vars_num., vars_cat=
        &_psi_vars_cat., byvar=&_psi_byvar. );

    %if &_psi_rc. ne 0 %then %do;
        %put ERROR: [psi_run] Contract fallido - módulo abortado.;
        %return;
    %end;

    /* ==================================================================
    3) Compute - PSI cubo + cubo_wide + resumen → work tables
    ================================================================== */
    %_psi_compute( input_caslib=&input_caslib., train_table=&train_table.,
        oot_table=&oot_table., vars_num=&_psi_vars_num., vars_cat=
        &_psi_vars_cat., byvar=&_psi_byvar., n_buckets=&psi_n_buckets., mensual=
        &psi_mensual. );

    /* ==================================================================
    4) Report - HTML + Excel + JPEG
    ================================================================== */
    %_psi_report( report_path=&_report_path., images_path=&_images_path.,
        file_prefix=&_file_prefix., byvar=&_psi_byvar. );

    /* ==================================================================
    5) Persistir tablas como .sas7bdat en directorio de tables
    Usa _tables_path y _tbl_prefix (≤32 chars)
    ================================================================== */
    /* ---- Crear directorio tables/METOD4.2 si no existe --------------- */
    %let _dir_rc=%sysfunc(dcreate(METOD4.2, &_tables_path./../));
    %let _dir_rc=%sysfunc(dcreate(., &_tables_path.));

    libname _outlib "&_tables_path.";

    data _outlib.&_tbl_prefix._cubo;
        set casuser._psi_cubo;
    run;

    data _outlib.&_tbl_prefix._wide;
        set casuser._psi_cubo_wide;
    run;

    data _outlib.&_tbl_prefix._rsmn;
        set casuser._psi_resumen;
    run;

    libname _outlib clear;

    %put NOTE: [psi_run] Tablas .sas7bdat guardadas en &_tables_path.;
    %put NOTE: [psi_run] &_tbl_prefix._cubo (detalle);
    %put NOTE: [psi_run] &_tbl_prefix._wide (pivot);
    %put NOTE: [psi_run] &_tbl_prefix._rsmn (resumen);

    /* ==================================================================
    6) Cleanup - eliminar tablas temporales de casuser (CAS)
    ================================================================== */
    proc datasets library=casuser nolist nowarn;
        delete _psi_cubo _psi_cubo_wide _psi_resumen;
        run;

        %put NOTE:======================================================;
        %put NOTE: [psi_run] FIN - &_file_prefix. (mode=&psi_mode.);
        %put NOTE:======================================================;

%mend psi_run;
