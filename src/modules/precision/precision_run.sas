/* =========================================================================
precision_run.sas - Macro publica del modulo Precision (METOD6)

API:
%precision_run(
    input_caslib  = PROC,
    train_table   = _train_input,
    oot_table     = _oot_input,
    output_caslib = OUT,
    troncal_id    = <id>,
    scope         = base | segNNN,
    run_id        = <run_id>
)

Precision compara el promedio observado (target) vs el score del modelo
(PD o XB), con y sin ponderacion por monto, y opcionalmente por segmento.
Usa default cerrado (def_cld) para filtrar TRAIN y OOT.
========================================================================= */
/* ---- Incluir componentes del modulo ----------------------------------- */
%include "&fw_root./src/modules/precision/precision_contract.sas";
%include "&fw_root./src/modules/precision/impl/precision_compute.sas";
%include "&fw_root./src/modules/precision/impl/precision_report.sas";

%macro precision_run(input_caslib=PROC, train_table=_train_input,
    oot_table=_oot_input, output_caslib=OUT, troncal_id=, scope=, run_id=);

    %global _prec_rc;
    %let _prec_rc=0;

    %local _prec_target _prec_score _prec_pd _prec_xb _prec_monto _prec_byvar
        _prec_def_cld _prec_segvar _prec_score_mode _prec_scope_abbr
        _prec_report_path _prec_images_path _prec_file_prefix
        _prec_is_custom _prec_seg_num _prec_dir_rc _prec_has_trn
        _prec_has_oot;

    %put NOTE:======================================================;
    %put NOTE: [precision_run] INICIO;
    %put NOTE: troncal=&troncal_id. scope=&scope.;
    %put NOTE: train=&input_caslib..&train_table.;
    %put NOTE: oot=&input_caslib..&oot_table.;
    %put NOTE:======================================================;

    %let _prec_target= ;
    %let _prec_score= ;
    %let _prec_pd= ;
    %let _prec_xb= ;
    %let _prec_monto= ;
    %let _prec_byvar= ;
    %let _prec_def_cld= ;
    %let _prec_segvar= ;
    %let _prec_is_custom=0;

    proc sql noprint;
        select strip(target) into :_prec_target trimmed
        from casuser.cfg_troncales where troncal_id=&troncal_id.;
        select strip(pd) into :_prec_pd trimmed
        from casuser.cfg_troncales where troncal_id=&troncal_id.;
        select strip(xb) into :_prec_xb trimmed
        from casuser.cfg_troncales where troncal_id=&troncal_id.;
        select strip(monto) into :_prec_monto trimmed
        from casuser.cfg_troncales where troncal_id=&troncal_id.;
        select strip(byvar) into :_prec_byvar trimmed
        from casuser.cfg_troncales where troncal_id=&troncal_id.;
        select strip(put(def_cld, best.)) into :_prec_def_cld trimmed
        from casuser.cfg_troncales where troncal_id=&troncal_id.;
        select strip(var_seg) into :_prec_segvar trimmed
        from casuser.cfg_troncales where troncal_id=&troncal_id.;
    quit;

    %let _prec_score_mode=%upcase(%superq(prec_score_source));
    %if %length(%superq(_prec_score_mode))=0 %then %let _prec_score_mode=AUTO;

    %if %upcase(&prec_mode.)=CUSTOM %then %do;
        %let _prec_is_custom=1;
        %put NOTE: [precision_run] Modo CUSTOM activado.;

        %if %length(%superq(prec_custom_target)) > 0 %then
            %let _prec_target=&prec_custom_target.;
        %if %length(%superq(prec_custom_monto)) > 0 %then
            %let _prec_monto=&prec_custom_monto.;
        %if %length(%superq(prec_custom_def_cld)) > 0 %then
            %let _prec_def_cld=&prec_custom_def_cld.;
        %if %length(%superq(prec_custom_segvar)) > 0 %then
            %let _prec_segvar=&prec_custom_segvar.;
        %if %length(%superq(prec_custom_score_var)) > 0 %then
            %let _prec_score=&prec_custom_score_var.;
    %end;

    %if %length(%superq(_prec_score))=0 %then %do;
        %if &_prec_score_mode.=PD %then %let _prec_score=&_prec_pd.;
        %else %if &_prec_score_mode.=XB %then %let _prec_score=&_prec_xb.;
        %else %do;
            %if %length(%superq(_prec_pd)) > 0 %then %let _prec_score=&_prec_pd.;
            %else %if %length(%superq(_prec_xb)) > 0 %then %let _prec_score=&_prec_xb.;
        %end;
    %end;

    %if %upcase(&prec_use_segmentation.)=0 or
        %upcase(&prec_use_segmentation.)=NO %then %let _prec_segvar=;
    %else %if (%upcase(&prec_use_segmentation.)=1 or
        %upcase(&prec_use_segmentation.)=YES) and
        %length(%superq(_prec_segvar))=0 %then %do;
        %put WARNING: [precision_run] prec_use_segmentation=1 pero no hay
            segvar resuelta. Se ejecutara sin segmentacion.;
    %end;

    %put NOTE: [precision_run] Variables resueltas:;
    %put NOTE: [precision_run] target=&_prec_target. score=&_prec_score.;
    %put NOTE: [precision_run] pd=&_prec_pd. xb=&_prec_xb.;
    %put NOTE: [precision_run] monto=&_prec_monto. segvar=&_prec_segvar.;
    %put NOTE: [precision_run] byvar=&_prec_byvar. def_cld=&_prec_def_cld.;

    %if %substr(&scope., 1, 3)=seg %then %let _prec_scope_abbr=&scope.;
    %else %let _prec_scope_abbr=base;

    %if &_prec_is_custom.=1 %then %do;
        %let _prec_report_path=&fw_root./outputs/runs/&run_id./experiments;
        %let _prec_images_path=&fw_root./outputs/runs/&run_id./experiments;
        %let _prec_file_prefix=custom_precision_troncal_&troncal_id._&_prec_scope_abbr.;
        %put NOTE: [precision_run] Output -> experiments/ (exploratorio).;
    %end;
    %else %do;
        %let _prec_report_path=&fw_root./outputs/runs/&run_id./reports/METOD6;
        %let _prec_images_path=&fw_root./outputs/runs/&run_id./images/METOD6;
        %let _prec_file_prefix=precision_troncal_&troncal_id._&_prec_scope_abbr.;
        %let _prec_dir_rc=%sysfunc(dcreate(METOD6, &fw_root./outputs/runs/&run_id./reports));
        %let _prec_dir_rc=%sysfunc(dcreate(METOD6, &fw_root./outputs/runs/&run_id./images));
        %put NOTE: [precision_run] Output -> reports/METOD6 + images/METOD6.;
    %end;

    %precision_contract(input_caslib=&input_caslib., train_table=&train_table.,
        oot_table=&oot_table., target=&_prec_target., score_var=&_prec_score.,
        byvar=&_prec_byvar., def_cld=&_prec_def_cld., monto_var=&_prec_monto.,
        segvar=&_prec_segvar.);

    %if &_prec_rc. ne 0 %then %do;
        %put ERROR: [precision_run] Contract fallido - modulo abortado.;
        %return;
    %end;

    /* ---- Normalizar opcionales: si no estan en ambos splits, se omiten */
    %if %length(%superq(_prec_monto)) > 0 %then %do;
        %let _prec_has_trn=0;
        %let _prec_has_oot=0;
        proc sql noprint;
            select count(*) into :_prec_has_trn trimmed
            from dictionary.columns
            where upcase(libname)=upcase("&input_caslib.")
              and upcase(memname)=upcase("&train_table.")
              and upcase(name)=upcase("&_prec_monto.");
            select count(*) into :_prec_has_oot trimmed
            from dictionary.columns
            where upcase(libname)=upcase("&input_caslib.")
              and upcase(memname)=upcase("&oot_table.")
              and upcase(name)=upcase("&_prec_monto.");
        quit;
        %if &_prec_has_trn.=0 or &_prec_has_oot.=0 %then %do;
            %put WARNING: [precision_run] monto=&_prec_monto. no disponible en
                ambos splits. Se omite precision ponderada.;
            %let _prec_monto=;
        %end;
    %end;

    %if %length(%superq(_prec_segvar)) > 0 %then %do;
        %let _prec_has_trn=0;
        %let _prec_has_oot=0;
        proc sql noprint;
            select count(*) into :_prec_has_trn trimmed
            from dictionary.columns
            where upcase(libname)=upcase("&input_caslib.")
              and upcase(memname)=upcase("&train_table.")
              and upcase(name)=upcase("&_prec_segvar.");
            select count(*) into :_prec_has_oot trimmed
            from dictionary.columns
            where upcase(libname)=upcase("&input_caslib.")
              and upcase(memname)=upcase("&oot_table.")
              and upcase(name)=upcase("&_prec_segvar.");
        quit;
        %if &_prec_has_trn.=0 or &_prec_has_oot.=0 %then %do;
            %put WARNING: [precision_run] segvar=&_prec_segvar. no disponible
                en ambos splits. Se omite precision segmentada.;
            %let _prec_segvar=;
        %end;
    %end;

    %_precision_report(input_caslib=&input_caslib., train_table=&train_table.,
        oot_table=&oot_table., target=&_prec_target., score_var=&_prec_score.,
        monto_var=&_prec_monto., segvar=&_prec_segvar., byvar=&_prec_byvar.,
        def_cld=&_prec_def_cld., ponderado=&prec_use_weighted.,
        report_path=&_prec_report_path., images_path=&_prec_images_path.,
        file_prefix=&_prec_file_prefix.);

    %put NOTE:======================================================;
    %put NOTE: [precision_run] FIN - &_prec_file_prefix. (mode=&prec_mode.);
    %put NOTE:======================================================;

%mend precision_run;
