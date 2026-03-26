/* =========================================================================
estabilidad_run.sas - Macro publica del modulo Estabilidad (Metodo 4.2)

API:
%estabilidad_run(
input_caslib  = PROC,
input_table   = _scope_input,
output_caslib = OUT,
troncal_id    = <id>,
scope         = base | segNNN,
run_id        = <run_id>
)

Flujo interno:
1) Resolver variables desde cfg_troncales/cfg_segmentos (byvar, vars num/cat)
2) Derivar TRAIN/OOT internamente desde input consolidado (tabla canonica)
3) Ejecutar contract (validaciones)
4) Generar reportes HTML + Excel (TRAIN + OOT en un solo reporte)
5) Cleanup

NOTA: No persiste tablas .sas7bdat (analisis visual solamente).
Usa ventanas TRAIN/OOT desde cfg_troncales (sin def_cld).

Scope-input: recibe _scope_input promovida por run_module(scope_input=1).

Compatibilidad: segmento y universo.

Modos de ejecucion (configurados en step_estabilidad.sas):
AUTO   - resuelve vars desde config (cfg_segmentos.num_list/cat_list,
fallback cfg_troncales.num_unv/cat_unv)
CUSTOM - usa estab_custom_vars_num / estab_custom_vars_cat
========================================================================= */
/* ---- Incluir componentes del modulo ----------------------------------- */
%include "&fw_root./src/modules/estabilidad/estabilidad_contract.sas";
%include "&fw_root./src/modules/estabilidad/impl/estabilidad_compute.sas";
%include "&fw_root./src/modules/estabilidad/impl/estabilidad_report.sas";

%macro estabilidad_run(input_caslib=PROC, input_table=_scope_input,
    output_caslib=OUT, troncal_id=, scope=, run_id=);

    /* ---- Return code ---------------------------------------------------- */
    %global _estab_rc;
    %let _estab_rc=0;

    %local _estab_byvar _estab_vars_num _estab_vars_cat _report_path
        _images_path _file_prefix _scope_abbr _estab_is_custom _seg_num
        _estab_train_min _estab_train_max _estab_oot_min _estab_oot_max;

    %put NOTE:======================================================;
    %put NOTE: [estabilidad_run] INICIO;
    %put NOTE: troncal=&troncal_id. scope=&scope.;
    %put NOTE: input=&input_caslib..&input_table.;
    %put NOTE:======================================================;

    /* ==================================================================
    1) Resolver variables
    ================================================================== */
    %let _estab_vars_num= ;
    %let _estab_vars_cat= ;
    %let _estab_byvar= ;
    %let _estab_is_custom=0;
    %let _estab_train_min= ;
    %let _estab_train_max= ;
    %let _estab_oot_min= ;
    %let _estab_oot_max= ;

    /* Resolver byvar y ventanas desde configuracion del troncal */
    proc sql noprint;
        select strip(byvar),
               strip(put(train_min_mes, best.)),
               strip(put(train_max_mes, best.)),
               strip(put(oot_min_mes, best.)),
               strip(put(oot_max_mes, best.))
          into :_estab_byvar trimmed,
               :_estab_train_min trimmed,
               :_estab_train_max trimmed,
               :_estab_oot_min trimmed,
               :_estab_oot_max trimmed
        from casuser.cfg_troncales
        where troncal_id=&troncal_id.;
    quit;

    /* ------ Modo CUSTOM: variables personalizadas ---------------------- */
    %if %upcase(&estab_mode.)=CUSTOM %then %do;
        %if %length(%superq(estab_custom_vars_num)) > 0 or
            %length(%superq(estab_custom_vars_cat)) > 0 %then %do;
            %let _estab_vars_num=&estab_custom_vars_num.;
            %let _estab_vars_cat=&estab_custom_vars_cat.;
            %let _estab_is_custom=1;
            %put NOTE: [estabilidad_run] Modo CUSTOM activado.;
        %end;
        %else %do;
            %put WARNING: [estabilidad_run] estab_mode=CUSTOM pero sin variables
                custom. Fallback a AUTO.;
        %end;
    %end;

    /* ------ Modo AUTO (o fallback): variables de configuracion ---------- */
    %if &_estab_is_custom.=0 %then %do;
        %put NOTE: [estabilidad_run] Modo AUTO - resolviendo vars desde config.;

        /* Si es segmento, intentar override desde cfg_segmentos */
        %if %substr(&scope., 1, 3)=seg %then %do;
            %let _seg_num=%sysfunc(inputn(%substr(&scope., 4), best.));

            proc sql noprint;
                select strip(num_list) into :_estab_vars_num trimmed from
                    casuser.cfg_segmentos where troncal_id=&troncal_id. and
                    seg_id=&_seg_num.;

                select strip(cat_list) into :_estab_vars_cat trimmed from
                    casuser.cfg_segmentos where troncal_id=&troncal_id. and
                    seg_id=&_seg_num.;
            quit;
        %end;

        /* Fallback a vars del troncal si no hay override */
        %if %length(%superq(_estab_vars_num))=0 %then %do;
            proc sql noprint;
                select strip(num_unv) into :_estab_vars_num trimmed from
                    casuser.cfg_troncales where troncal_id=&troncal_id.;
            quit;
        %end;

        %if %length(%superq(_estab_vars_cat))=0 %then %do;
            proc sql noprint;
                select strip(cat_unv) into :_estab_vars_cat trimmed from
                    casuser.cfg_troncales where troncal_id=&troncal_id.;
            quit;
        %end;
    %end;

    %put NOTE: [estabilidad_run] Variables resueltas:;
    %put NOTE: [estabilidad_run] num=&_estab_vars_num.;
    %put NOTE: [estabilidad_run] cat=&_estab_vars_cat.;
    %put NOTE: [estabilidad_run] byvar=&_estab_byvar.;
    %put NOTE: [estabilidad_run] TRAIN ventana=&_estab_train_min.-&_estab_train_max.;
    %put NOTE: [estabilidad_run] OOT ventana=&_estab_oot_min.-&_estab_oot_max.;

    /* ==================================================================
    Determinar rutas de salida
    ================================================================== */
    %if %substr(&scope., 1, 3)=seg %then %let _scope_abbr=&scope.;
    %else %let _scope_abbr=base;

    %if &_estab_is_custom.=1 %then %do;
        %let _report_path=&fw_root./outputs/runs/&run_id./experiments;
        %let _images_path=&fw_root./outputs/runs/&run_id./experiments;
        %let _file_prefix=custom_estab_troncal_&troncal_id._&_scope_abbr.;
        %put NOTE: [estabilidad_run] Output -> experiments/ (exploratorio);
    %end;
    %else %do;
        %let _report_path=&fw_root./outputs/runs/&run_id./reports/METOD4.2;
        %let _images_path=&fw_root./outputs/runs/&run_id./images/METOD4.2;
        %let _file_prefix=estab_troncal_&troncal_id._&_scope_abbr.;
        %put NOTE: [estabilidad_run] Output -> reports/METOD4.2/ +
            images/METOD4.2/;
    %end;

    /* ==================================================================
    2) Contract - validaciones sobre input consolidado
    ================================================================== */
    %estabilidad_contract(input_caslib=&input_caslib.,
        input_table=&input_table.,
        vars_num=&_estab_vars_num., vars_cat=&_estab_vars_cat.,
        byvar=&_estab_byvar., train_min_mes=&_estab_train_min.,
        train_max_mes=&_estab_train_max., oot_min_mes=&_estab_oot_min.,
        oot_max_mes=&_estab_oot_max.);

    %if &_estab_rc. ne 0 %then %do;
        %put ERROR: [estabilidad_run] Contract fallido - modulo abortado.;
        %return;
    %end;

    /* ==================================================================
    3) Construir tabla canonica con Muestra=TRAIN/OOT
    ================================================================== */
    proc cas;
        session conn;
        table.dropTable / caslib='casuser' name='_estab_input' quiet=true;
    quit;

    proc fedsql sessref=conn;
        create table casuser._estab_input {options replace=true} as
        select 'TRAIN' as Muestra, *
        from &input_caslib..&input_table.
        where &_estab_byvar. >= &_estab_train_min.
          and &_estab_byvar. <= &_estab_train_max.
        union all
        select 'OOT' as Muestra, *
        from &input_caslib..&input_table.
        where &_estab_byvar. >= &_estab_oot_min.
          and &_estab_byvar. <= &_estab_oot_max.;
    quit;

    /* ==================================================================
    4) Report - HTML + Excel (incluye computo inline)
    ================================================================== */
    %_estabilidad_report(input_caslib=casuser, input_table=_estab_input,
        byvar=&_estab_byvar.,
        vars_num=&_estab_vars_num., vars_cat=&_estab_vars_cat.,
        report_path=&_report_path., images_path=&_images_path.,
        file_prefix=&_file_prefix.);

    /* ==================================================================
    5) Cleanup
    ================================================================== */
    proc datasets library=casuser nolist nowarn;
        delete _estab_:;
    quit;

    proc datasets library=work nolist nowarn;
        delete _estab_:;
    quit;

    /* No se persisten tablas (analisis visual solamente) */
    %put NOTE:======================================================;
    %put NOTE: [estabilidad_run] FIN - &_file_prefix. (mode=&estab_mode.);
    %put NOTE:======================================================;

%mend estabilidad_run;
