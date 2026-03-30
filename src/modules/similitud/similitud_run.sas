/* =========================================================================
similitud_run.sas - Macro publica del modulo Similitud (Metodo 6)

API:
%similitud_run(
input_caslib  = PROC,
input_table   = _scope_input,
output_caslib = OUT,
troncal_id    = <id>,
scope         = base | segNNN,
run_id        = <run_id>
)

Flujo CAS-first:
1) Resolver variables y ventanas desde cfg_troncales/cfg_segmentos
2) Derivar tabla unificada casuser._simil_input con Split=TRAIN/OOT
3) Ejecutar contract sobre tabla unificada
4) Generar reportes HTML + Excel (bucket evolution + similitud)
5) Cleanup de temporales CAS
========================================================================= */
%include "&fw_root./src/modules/similitud/similitud_contract.sas";
%include "&fw_root./src/modules/similitud/impl/similitud_compute.sas";
%include "&fw_root./src/modules/similitud/impl/similitud_report.sas";

%macro _simil_prepare_input_scope(input_caslib=, input_table=, byvar=,
    train_min_mes=, train_max_mes=, oot_min_mes=, oot_max_mes=,
    out_table=_simil_input, split_var=Split);

    proc cas;
        session conn;
        table.dropTable / caslib='casuser' name='&out_table.' quiet=true;
        table.dropTable / caslib='casuser' name='_simil_input_stage' quiet=true;
    quit;

    proc fedsql sessref=conn;
        create table casuser.&out_table. {options replace=true} as
        select 'TRAIN' as &split_var. length 5,
               a.*
        from &input_caslib..&input_table. a
        where a.&byvar. >= &train_min_mes.
          and a.&byvar. <= &train_max_mes.;
    quit;

    proc fedsql sessref=conn;
        create table casuser._simil_input_stage {options replace=true} as
        select 'OOT' as &split_var. length 5,
               a.*
        from &input_caslib..&input_table. a
        where a.&byvar. >= &oot_min_mes.
          and a.&byvar. <= &oot_max_mes.;
    quit;

    proc cas;
        session conn;
        table.append /
            source={caslib='casuser', name='_simil_input_stage'},
            target={caslib='casuser', name='&out_table.'};
        table.dropTable / caslib='casuser' name='_simil_input_stage' quiet=true;
    quit;

%mend _simil_prepare_input_scope;

%macro similitud_run(input_caslib=PROC, input_table=_scope_input,
    output_caslib=OUT, troncal_id=, scope=, run_id=);

    %global _simil_rc;
    %let _simil_rc=0;

    %local _simil_vars_num _simil_vars_cat _simil_target _simil_byvar
        _report_path _images_path _file_prefix _scope_abbr _simil_is_custom
        _seg_num _simil_train_min _simil_train_max _simil_oot_min
        _simil_oot_max;

    %put NOTE:======================================================;
    %put NOTE: [similitud_run] INICIO;
    %put NOTE: troncal=&troncal_id. scope=&scope.;
    %put NOTE: input=&input_caslib..&input_table.;
    %put NOTE:======================================================;

    %let _simil_vars_num=;
    %let _simil_vars_cat=;
    %let _simil_target=;
    %let _simil_byvar=;
    %let _simil_train_min=;
    %let _simil_train_max=;
    %let _simil_oot_min=;
    %let _simil_oot_max=;
    %let _simil_is_custom=0;

    proc sql noprint;
        select strip(target),
               strip(byvar),
               strip(put(train_min_mes, best.)),
               strip(put(train_max_mes, best.)),
               strip(put(oot_min_mes, best.)),
               strip(put(oot_max_mes, best.))
          into :_simil_target trimmed,
               :_simil_byvar trimmed,
               :_simil_train_min trimmed,
               :_simil_train_max trimmed,
               :_simil_oot_min trimmed,
               :_simil_oot_max trimmed
        from casuser.cfg_troncales
        where troncal_id=&troncal_id.;
    quit;

    %if %upcase(&simil_mode.)=CUSTOM %then %do;
        %if %length(%superq(simil_custom_vars_num)) > 0 or
            %length(%superq(simil_custom_vars_cat)) > 0 %then %do;
            %let _simil_vars_num=&simil_custom_vars_num.;
            %let _simil_vars_cat=&simil_custom_vars_cat.;
            %let _simil_is_custom=1;
            %put NOTE: [similitud_run] Modo CUSTOM activado.;
        %end;
        %else %do;
            %put WARNING: [similitud_run] simil_mode=CUSTOM pero sin variables custom. Fallback a AUTO.;
        %end;
    %end;

    %if &_simil_is_custom.=0 %then %do;
        %put NOTE: [similitud_run] Modo AUTO - resolviendo vars desde config.;

        %if %substr(&scope., 1, 3)=seg %then %do;
            %let _seg_num=%sysfunc(inputn(%substr(&scope., 4), best.));

            proc sql noprint;
                select strip(num_list) into :_simil_vars_num trimmed
                from casuser.cfg_segmentos
                where troncal_id=&troncal_id.
                  and seg_id=&_seg_num.;

                select strip(cat_list) into :_simil_vars_cat trimmed
                from casuser.cfg_segmentos
                where troncal_id=&troncal_id.
                  and seg_id=&_seg_num.;
            quit;
        %end;

        %if %length(%superq(_simil_vars_num))=0 %then %do;
            proc sql noprint;
                select strip(num_unv) into :_simil_vars_num trimmed
                from casuser.cfg_troncales
                where troncal_id=&troncal_id.;
            quit;
        %end;

        %if %length(%superq(_simil_vars_cat))=0 %then %do;
            proc sql noprint;
                select strip(cat_unv) into :_simil_vars_cat trimmed
                from casuser.cfg_troncales
                where troncal_id=&troncal_id.;
            quit;
        %end;
    %end;

    %if %length(%superq(_simil_target))=0 or %length(%superq(_simil_byvar))=0 %then
        %do;
        %put ERROR: [similitud_run] Config incompleta en cfg_troncales para troncal &troncal_id. (target/byvar).;
        %let _simil_rc=1;
        %return;
    %end;

    %if %length(%superq(_simil_train_min))=0 or
        %length(%superq(_simil_train_max))=0 or
        %length(%superq(_simil_oot_min))=0 or
        %length(%superq(_simil_oot_max))=0 %then %do;
        %put ERROR: [similitud_run] Ventanas TRAIN/OOT incompletas en cfg_troncales para troncal &troncal_id..;
        %let _simil_rc=1;
        %return;
    %end;

    %put NOTE: [similitud_run] target=&_simil_target.;
    %put NOTE: [similitud_run] byvar=&_simil_byvar.;
    %put NOTE: [similitud_run] ventanas TRAIN=&_simil_train_min.-&_simil_train_max. OOT=&_simil_oot_min.-&_simil_oot_max..;
    %put NOTE: [similitud_run] num=&_simil_vars_num.;
    %put NOTE: [similitud_run] cat=&_simil_vars_cat.;

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

    %_simil_prepare_input_scope(input_caslib=&input_caslib.,
        input_table=&input_table., byvar=&_simil_byvar.,
        train_min_mes=&_simil_train_min., train_max_mes=&_simil_train_max.,
        oot_min_mes=&_simil_oot_min., oot_max_mes=&_simil_oot_max.,
        out_table=_simil_input, split_var=Split);

    %similitud_contract(input_caslib=casuser, input_table=_simil_input,
        vars_num=&_simil_vars_num., vars_cat=&_simil_vars_cat.,
        target=&_simil_target., byvar=&_simil_byvar., split_var=Split);

    %if &_simil_rc. ne 0 %then %do;
        %put ERROR: [similitud_run] Contract fallido - modulo abortado.;
        %return;
    %end;

    %_similitud_report(input_caslib=casuser, input_table=_simil_input,
        split_var=Split, target=&_simil_target., byvar=&_simil_byvar.,
        vars_num=&_simil_vars_num., vars_cat=&_simil_vars_cat.,
        groups=&simil_n_groups., report_path=&_report_path.,
        images_path=&_images_path., file_prefix=&_file_prefix.);

    proc datasets library=casuser nolist nowarn;
        delete _simil_input;
    quit;

    %put NOTE:======================================================;
    %put NOTE: [similitud_run] FIN - &_file_prefix. (mode=&simil_mode.);
    %put NOTE:======================================================;

%mend similitud_run;
